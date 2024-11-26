###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

"""
Manages formatting for displaying information to the console.
"""
import logging
import os
import pprint
import shutil
import time
import traceback
from datetime import datetime
from multiprocessing import Pipe, Process
from typing import Dict

from kombu import Connection
from tabulate import tabulate

from merlin.ascii_art import banner_small
from merlin.study.status_renderers import status_renderer_factory
from merlin.utils import get_package_versions


LOG = logging.getLogger("merlin")
DEFAULT_LOG_LEVEL = "INFO"

# Colors here are chosen based on the Bang Wong color palette (https://www.nature.com/articles/nmeth.1618)
# Another useful link for comparing colors:
# https://davidmathlogic.com/colorblind/#%2356B4E9-%230072B2-%23009E73-%23D55E00-%23F0E442-%23E69F00-%23666666
ANSI_COLORS = {
    "RESET": "\033[0m",
    "GREY": "\033[38;2;102;102;102m",
    "LIGHT_BLUE": "\033[38;2;86;180;233m",
    "BLUE": "\033[38;2;0;114;178m",
    "GREEN": "\033[38;2;0;158;115m",
    "YELLOW": "\033[38;2;240;228;66m",
    "ORANGE": "\033[38;2;230;159;0m",
    "RED": "\033[38;2;213;94;0m",
}

# Inverse of ANSI_COLORS (useful for debugging)
COLOR_TRANSLATOR = {v: k for k, v in ANSI_COLORS.items()}


class ConnProcess(Process):
    """
    An extension of Multiprocessing's Process class in order
    to overwrite the run and exception defintions.
    """

    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as e:  # pylint: disable=C0103,W0703
            trace_back = traceback.format_exc()
            self._cconn.send((e, trace_back))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        """Create custom exception"""
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


def check_server_access(sconf):
    """
    Check if there are any issues connecting to the servers.
    If there are, output the errors.
    """
    servers = ["broker server", "results server"]

    if sconf.keys():
        print("\nChecking server connections:")
        print("-" * 28)

    excpts = {}
    for server in servers:
        if server in sconf:
            _examine_connection(server, sconf, excpts)

    if excpts:
        print("\nExceptions:")
        for key, val in excpts.items():
            print(f"{key}: {val}")


def _examine_connection(server, sconf, excpts):
    from merlin.config import broker, results_backend  # pylint: disable=C0415

    connect_timeout = 60
    try:
        ssl_conf = None
        if "broker" in server:
            ssl_conf = broker.get_ssl_config()
        if "results" in server:
            ssl_conf = results_backend.get_ssl_config()
        conn = Connection(sconf[server], ssl=ssl_conf)
        conn_check = ConnProcess(target=conn.connect)
        conn_check.start()
        counter = 0
        while conn_check.is_alive():
            time.sleep(1)
            counter += 1
            if counter > connect_timeout:
                conn_check.kill()
                raise TimeoutError(f"Connection was killed due to timeout ({connect_timeout}s)")
        conn.release()
        if conn_check.exception:
            error, _ = conn_check.exception
            raise error
    except Exception as e:  # pylint: disable=C0103,W0703
        print(f"{server} connection: Error")
        excpts[server] = e
    else:
        print(f"{server} connection: OK")


def display_config_info():
    """
    Prints useful configuration information to the console.
    """
    from merlin.config import broker, results_backend  # pylint: disable=C0415
    from merlin.config.configfile import default_config_info  # pylint: disable=C0415

    print("Merlin Configuration")
    print("-" * 25)
    print("")

    conf = default_config_info()
    sconf = {}
    excpts = {}
    try:
        conf["broker server"] = broker.get_connection_string(include_password=False)
        sconf["broker server"] = broker.get_connection_string()
        conf["broker ssl"] = broker.get_ssl_config()
    except Exception as e:  # pylint: disable=C0103,W0703
        conf["broker server"] = "Broker server error."
        excpts["broker server"] = e

    try:
        conf["results server"] = results_backend.get_connection_string(include_password=False)
        sconf["results server"] = results_backend.get_connection_string()
        conf["results ssl"] = results_backend.get_ssl_config()
    except Exception as e:  # pylint: disable=C0103,W0703
        conf["results server"] = "No results server configured or error."
        excpts["results server"] = e

    print(tabulate(conf.items(), tablefmt="presto"))

    if excpts:
        print("\nExceptions:")
        for key, val in excpts.items():
            print(f"{key}: {val}")

    check_server_access(sconf)


def display_multiple_configs(files, configs):
    """
    Logic for displaying multiple Merlin config files.

    :param `files`: List of merlin config files
    :param `configs`: List of merlin configurations
    """
    print("=" * 50)
    print(" MERLIN CONFIG ")
    print("=" * 50)

    for _file, config in zip(files, configs):
        print("")
        print(f"Display config info for path: {_file}")
        print("-" * 25)
        print("")
        pprint.pprint(config)


# Might use args here in the future so we'll disable the pylint warning for now
def print_info(args):  # pylint: disable=W0613
    """
    Provide version and location information about python and packages to
    facilitate user troubleshooting. Also provides info about server connections
    and configurations.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    display_config_info()

    print("")
    print("Python Configuration")
    print("-" * 25)
    print("")
    package_list = ["pip", "merlin", "maestrowf", "celery", "kombu", "amqp", "redis"]
    package_versions = get_package_versions(package_list)
    print(package_versions)
    pythonpath = os.environ.get("PYTHONPATH")
    print(f"$PYTHONPATH: {pythonpath}")


def display_status_task_by_task(status_obj: "DetailedStatus", test_mode: bool = False):  # noqa: F821
    """
    Displays a low level overview of the status of a study. This is a task-by-task
    status display where each task will show:
    step name, worker name, task queue, cmd & restart parameters,
    step workspace, step status, return code, elapsed time, run time, and num restarts.
    If too many tasks are found and the pager is disabled, prompts will appear for the user to decide
    what to do that way we don't overload the terminal (unless the no-prompts flag is provided).

    :param `status_obj`: A DetailedStatus object
    :param `test_mode`: If true, run this in testing mode and don't print any output. This will also
                        decrease the limit on the number of tasks allowed before a prompt is displayed.
    """
    args = status_obj.args
    try:
        status_renderer = status_renderer_factory.get_renderer(args.layout, args.disable_theme, args.disable_pager)
    except ValueError:
        LOG.error(f"Layout '{args.layout}' not implemented.")
        raise

    cancel_display = False

    # If the pager is disabled then we need to be careful not to overload the terminal with a bazillion tasks
    if args.disable_pager and not args.no_prompts:
        # Setting the limit by default to be 250 tasks before asking for additional filters
        no_prompt_limit = 250 if not test_mode else 15
        while status_obj.num_requested_statuses > no_prompt_limit:
            # See if the user wants to apply additional filters
            apply_additional_filters = input(
                f"About to display {status_obj.num_requested_statuses} tasks without a pager. "
                "Would you like to apply additional filters? (y/n/c) "
            ).lower()
            while apply_additional_filters not in ("y", "n", "c"):
                apply_additional_filters = input(
                    "Invalid input. You must enter either 'y' for yes, 'n' for no, or 'c' for cancel: "
                ).lower()

            # Apply filters if necessary or break the loop
            if apply_additional_filters == "y":
                status_obj.filter_via_prompts()
            elif apply_additional_filters == "n":
                print(f"Not filtering further. Displaying {status_obj.num_requested_statuses} tasks...")
                break
            else:
                print("Cancelling status display.")
                cancel_display = True
                break

    # Display the statuses
    if not cancel_display and not test_mode:
        if status_obj.num_requested_statuses > 0:
            # Table layout requires csv format (since it uses Maestro's renderer)
            if args.layout == "table":
                status_data = status_obj.format_status_for_csv()
            else:
                status_data = status_obj.requested_statuses
            status_renderer.layout(status_data=status_data, study_title=status_obj.workspace)
            status_renderer.render()

        for ustep in status_obj.step_tracker["unstarted_steps"]:
            print(f"\n{ustep} has not started yet.")
            print()


def _display_summary(state_info: Dict[str, str], cb_help: bool):
    """
    Given a dict of state info for a step, print a summary of the task states.

    :param `state_info`: A dictionary of information related to task states for a step
    :param `cb_help`: True if colorblind assistance (using symbols) is needed. False otherwise.
    """
    # Build a summary list of task info
    print("\nSUMMARY:")
    summary = []
    for key, val in state_info.items():
        label = key
        # Add colorblind symbols if needed
        if cb_help and "fill" in val:
            label = f"{key} {val['fill']}"
        # Color the label
        if "color" in val:
            label = f"{val['color']}{label}{ANSI_COLORS['RESET']}"

        # Grab the value associated with the label
        value = None
        if "count" in val:
            if val["count"] > 0:
                value = val["count"]
        elif "total" in val:
            value = val["total"]
        elif "name" in val:
            value = val["name"]
        else:
            value = val

        # Add the label and value as an entry to the summary
        if value:
            summary.append([label, value])

    # Display the summary
    print(tabulate(summary))
    print()


def display_status_summary(  # pylint: disable=R0912
    status_obj: "Status", non_workspace_keys: set, test_mode=False  # noqa: F821
) -> Dict:
    """
    Displays a high level overview of the status of a study. This includes
    progress bars for each step and a summary of the number of initialized,
    running, finished, cancelled, dry ran, failed, and unknown tasks.

    :param `status_obj`: A Status object
    :param `non_workspace_keys`: A set of keys in requested_statuses that are not workspace keys.
                                 This will be set("parameters", "task_queue", "workers")
    :param `test_mode`: If True, don't print anything and just return a dict of all the state info for each step
    :returns: A dict that's empty usually. If ran in test_mode it will be a dict of state_info for every step.
    """
    all_state_info = {}
    if not test_mode:
        print(f"{ANSI_COLORS['YELLOW']}Status for {status_obj.workspace} as of {datetime.now()}:{ANSI_COLORS['RESET']}")
        terminal_size = shutil.get_terminal_size()
        progress_bar_width = terminal_size.columns // 4

    LOG.debug(f"step_tracker in display: {status_obj.step_tracker}")
    for sstep in status_obj.step_tracker["started_steps"]:
        # This dict will keep track of the number of tasks at each status
        state_info = {
            "FINISHED": {"count": 0, "color": ANSI_COLORS["GREEN"], "fill": "█"},
            "CANCELLED": {"count": 0, "color": ANSI_COLORS["YELLOW"], "fill": "/"},
            "FAILED": {"count": 0, "color": ANSI_COLORS["RED"], "fill": "⣿"},
            "UNKNOWN": {"count": 0, "color": ANSI_COLORS["GREY"], "fill": "?"},
            "INITIALIZED": {"count": 0, "color": ANSI_COLORS["LIGHT_BLUE"]},
            "RUNNING": {"count": 0, "color": ANSI_COLORS["BLUE"]},
            "DRY_RUN": {"count": 0, "color": ANSI_COLORS["ORANGE"], "fill": "\\"},
            "TOTAL TASKS": {"total": status_obj.tasks_per_step[sstep]},
            "AVG RUN TIME": status_obj.run_time_info[sstep]["avg_run_time"],
            "RUN TIME STD DEV": status_obj.run_time_info[sstep]["run_time_std_dev"],
        }

        # Initialize a var to track # of completed tasks and grab the statuses for this step
        num_completed_tasks = 0

        # Loop through each entry for the step (if there's no parameters there will just be one entry)
        for full_step_name in status_obj.full_step_name_map[sstep]:
            overall_step_info = status_obj.requested_statuses[full_step_name]

            # If this was a non-local run we should have a task queue and worker name to add to state_info
            if "task_queue" in overall_step_info:
                state_info["TASK QUEUE"] = {"name": overall_step_info["task_queue"]}
            if "workers" in overall_step_info:
                worker_str = ", ".join(overall_step_info["workers"])
                state_info["WORKER(S)"] = {"name": worker_str}

            # Loop through all workspaces for this step (if there's no samples for this step it'll just be one path)
            for sub_step_workspace, task_status_info in overall_step_info.items():
                # We've already handled the non-workspace keys that we need so ignore them here
                if sub_step_workspace in non_workspace_keys:
                    continue

                state_info[task_status_info["status"]]["count"] += 1
                # Increment the number of completed tasks (not running or initialized)
                if task_status_info["status"] not in ("INITIALIZED", "RUNNING"):
                    num_completed_tasks += 1

        if test_mode:
            all_state_info[sstep] = state_info
        else:
            # Display the progress bar and summary for the step
            print(f"\n{sstep}\n")
            display_progress_bar(
                num_completed_tasks,
                status_obj.tasks_per_step[sstep],
                state_info=state_info,
                suffix="Complete",
                length=progress_bar_width,
                cb_help=status_obj.args.cb_help,
            )
            _display_summary(state_info, status_obj.args.cb_help)
            print("-" * (terminal_size.columns // 2))

    # For each unstarted step, print an empty progress bar
    for ustep in status_obj.step_tracker["unstarted_steps"]:
        if test_mode:
            all_state_info[ustep] = "UNSTARTED"
        else:
            print(f"\n{ustep}\n")
            display_progress_bar(0, 100, suffix="Complete", length=progress_bar_width)
            print(f"\n{ustep} has not started yet.\n")
            print("-" * (terminal_size.columns // 2))

    return all_state_info


# Credit to this stack overflow post: https://stackoverflow.com/a/34325723
def display_progress_bar(  # pylint: disable=R0913,R0914
    current,
    total,
    state_info=None,
    prefix="",
    suffix="",
    decimals=1,
    length=80,
    fill="█",
    print_end="\n",
    color=None,
    cb_help=False,
):
    """
    Prints a progress bar based on current and total.

    :param `current`:     current number (Int)
    :param `total`:       total number (Int)
    :param `state_info`:  information about the state of tasks (Dict) (overrides color)
    :param `prefix`:      prefix string (Str)
    :param `suffix`:      suffix string (Str)
    :param `decimals`:    positive number of decimals in percent complete (Int)
    :param `length`:      character length of bar (Int)
    :param `fill`:        bar fill character (Str)
    :param `print_end`:    end character (e.g. "\r", "\r\n") (Str)
    :param `color`:       color of the progress bar (ANSI Str) (overridden by state_info)
    :param `cb_help`:     true if color blind help is needed; false otherwise (Bool)
    """
    # Set the color of the bar
    if color and color in ANSI_COLORS:
        fill = f"{color}{fill}{ANSI_COLORS['RESET']}"

    # Get the percentage done and the total fill length of the bar
    percent = ("{0:." + str(decimals) + "f}").format(100 * (current / float(total)))
    total_filled_length = int(length * current // total)

    # Print a progress bar based on state of the study
    if state_info:
        print(f"\r{prefix} |", end="")
        for key, val in state_info.items():
            # Only fill bar with completed tasks
            if key in (
                "INITIALIZED",
                "RUNNING",
                "TASK QUEUE",
                "WORKER(S)",
                "TOTAL TASKS",
                "AVG RUN TIME",
                "RUN TIME STD DEV",
            ):
                continue

            # Get the length to fill for this specific state
            partial_filled_length = int(length * val["count"] // total)

            if partial_filled_length > 0:
                if cb_help:
                    fill = val["fill"]
                progress_bar = fill * partial_filled_length
                print(f"{val['color']}{progress_bar}", end="")

        # The remaining bar represents the number of tasks still incomplete
        remaining_bar = "-" * (length - total_filled_length)
        print(f'{ANSI_COLORS["RESET"]}{remaining_bar}| {percent}% {suffix}', end=print_end)
    # Print a normal progress bar
    else:
        progress_bar = fill * total_filled_length + "-" * (length - total_filled_length)
        print(f"\r{prefix} |{progress_bar}| {percent}% {suffix}", end=print_end)
