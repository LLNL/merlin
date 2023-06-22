###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.0.
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
import os
import pprint
import shutil
import subprocess
import time
import traceback
from collections import deque
from datetime import datetime
from multiprocessing import Pipe, Process
from typing import Dict, List, Tuple, Union

from kombu import Connection
from tabulate import tabulate

from merlin.ascii_art import banner_small
from merlin.config import broker, results_backend
from merlin.config.configfile import default_config_info
from merlin.study.status_renderers import status_renderer_factory
from merlin.celery import app

import logging
LOG = logging.getLogger("merlin")
DEFAULT_LOG_LEVEL = "INFO"

# Colors here are chosen based on the Bang Wong color palette (https://www.nature.com/articles/nmeth.1618)
# Another useful link for comparing colors: https://davidmathlogic.com/colorblind/#%2356B4E9-%230072B2-%23009E73-%23D55E00-%23F0E442-%23E69F00-%23666666
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
        except Exception as e:  # pylint: disable=W0718,C0103
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
    except Exception as e:  # pylint: disable=W0718,C0103
        print(f"{server} connection: Error")
        excpts[server] = e
    else:
        print(f"{server} connection: OK")


def display_config_info():
    """
    Prints useful configuration information to the console.
    """
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
    except Exception as e:  # pylint: disable=W0718,C0103
        conf["broker server"] = "Broker server error."
        excpts["broker server"] = e

    try:
        conf["results server"] = results_backend.get_connection_string(include_password=False)
        sconf["results server"] = results_backend.get_connection_string()
        conf["results ssl"] = results_backend.get_ssl_config()
    except Exception as e:  # pylint: disable=W0718,C0103
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
    Provide version and location information about python and pip to
    facilitate user troubleshooting. 'merlin info' is a CLI tool only for
    developer versions of Merlin.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    display_config_info()

    print("")
    print("Python Configuration")
    print("-" * 25)
    print("")
    info_calls = ["which python3", "python3 --version", "which pip3", "pip3 --version"]
    info_str = ""
    for cmd in info_calls:
        info_str += 'echo " $ ' + cmd + '" && ' + cmd + "\n"
        info_str += "echo \n"
    info_str += r"echo \"echo \$PYTHONPATH\" && echo $PYTHONPATH"
    _ = subprocess.run(info_str, shell=True)
    print("")


def get_user_filter() -> Union[int, List[str]]:
    """
    Get a filter on the statuses to display from the user. Possible options
    for filtering:
        - An int representing the max number of tasks to display -> equivalent to the --max-tasks flag
        - A list of statuses -> equivalent to the --task-status flag
        - A list of return codes -> equivalent to the --return-code flag
        - An exit keyword to leave the filter prompt without filtering

    :returns: An int or a list of strings to filter by
    """
    from merlin.study.status import VALID_STATUS_FILTERS, VALID_RETURN_CODES, VALID_EXIT_FILTERS, ALL_VALID_FILTERS  # pylint: disable=C0415
    filter_info = {
        "Filter Type": [
            "Display a specific number of tasks",
            "Filter by status",
            "Filter by return code",
            "Exit without filtering"
        ], 
        "Description": [
            "Enter an integer greater than 0",
            f"Enter a comma separated list of the following statuses you'd like to see: {VALID_STATUS_FILTERS}",
            f"Enter a comma separated list of the following return codes you'd like to see: {VALID_RETURN_CODES}",
            f"Enter one of the following: {VALID_EXIT_FILTERS}"
        ],
        "Example": [
            "30",
            "status FAILED, CANCELLED",
            "return code SOFT_FAIL, RETRY",
            "EXIT"
        ]
    }
    filter_option_renderer = status_renderer_factory.get_renderer("table", disable_theme=True, disable_pager=True)
    filter_option_renderer.layout(status_data=filter_info)
    filter_option_renderer.render()

    # Obtain and validate the filter provided by the user
    invalid_filter = True
    while invalid_filter:
        user_filter = input("How would you like to filter the tasks? ")
        try:
            user_filter = int(user_filter)
            if user_filter > 0:
                invalid_filter = False
            else:
                print(f"Integer must be greater than 0.")
        except ValueError:
            # Remove spaces
            user_filter = user_filter.replace(" ", "")
            # Split user filters by commas
            user_filter = user_filter.split(",")
            # Ensure all filters are valid
            for i, entry in enumerate(user_filter):
                entry = entry.upper()
                user_filter[i] = entry
                if entry not in ALL_VALID_FILTERS:
                    invalid_filter = True
                    print(f"Invalid input: {entry}. Input must be one of the following {ALL_VALID_FILTERS}")
                    break
                else:
                    invalid_filter = False

    return user_filter

# TODO double check the current changes
# TODO organize the code in status.py and here
# TODO add this new stuff to the docs
# TODO create a PR for the status command :)

def filter_via_prompts(status_info: Dict[str, List]):
    """
    Interact with the user to manage how many/which tasks are displayed. This helps to
    prevent us from overloading the terminal by displaying a bazillion tasks at once.

    :param `status_info`: A dict of task statuses read from MERLIN_STATUS files
    """
    from merlin.study.status import apply_filters, _filter_by_max_tasks, VALID_STATUS_FILTERS, VALID_RETURN_CODES  # pylint: disable=C0415
    # Get the filter from the user
    user_filter = get_user_filter()
    # Display a certain amount of tasks provided by the user
    if isinstance(user_filter, int):
        _filter_by_max_tasks(status_info, user_filter)
    # Apply the non-numerical filter given by the user
    else:
        # Exit without displaying anything
        if "E" in user_filter or "EXIT" in user_filter:
            pass
        # Filter by other filter types
        else:
            # Determine all the types of filters we're about to apply
            filter_types = []
            for i, filt in enumerate(user_filter):
                if filt in VALID_STATUS_FILTERS and "status" not in filter_types:
                    filter_types.append("status")
                
                if filt in VALID_RETURN_CODES:
                    user_filter[i] = f"MERLIN_{filt}"
                    if "return code" not in filter_types:
                        filter_types.append("return code")
            
            # Apply the filters and tell the user how many tasks match the filters
            apply_filters(filter_types, status_info, user_filter)
            num_tasks = len(status_info["Step Name"])
            LOG.info(f"Found {num_tasks} tasks matching your filter.")


def display_low_lvl_status(statuses_to_display: Dict[str, List], unstarted_steps: List[str], workspace: str, args: "Namespace"):
    """
    Displays a low level overview of the status of a study. This is a task-by-task
    status display where each task will show:
    step name, step status, return code, elapsed time, run time, num restarts, dir path, task queue, worker name
    in that order. If too many tasks are found, prompts will appear for the user to decide
    what to do that way we don't overload the terminal (unless the no-prompts or max-tasks flags are provided).

    :param `statuses_to_display`: A dict of statuses to display. Keys are column headers, values are columns.
    :param `unstarted_steps`: A list of step names that have yet to start executing
    :param `workspace`: the filepath of the study output
    :param `args`: The CLI arguments provided via the user
    """
    try:
        status_renderer = status_renderer_factory.get_renderer(args.layout, args.disable_theme, args.disable_pager)
    except ValueError:
        LOG.error(f"Layout '{args.layout}' not implemented.")
        raise

    # Get the number of tasks we're about to display
    num_tasks = len(statuses_to_display["Step Name"])
    cancel_display = False

    # If the pager is disabled then we need to be careful not to overload the terminal with a bazillion tasks
    if args.disable_pager and not args.no_prompts:
        # Setting the limit at 250 tasks before asking for additional filters
        while num_tasks > 250:
            # See if the user wants to apply additional filters
            apply_additional_filters = input(f"About to display {num_tasks} tasks without a pager. Would you like to apply additional filters? (y/n/c) ").lower()
            while apply_additional_filters not in ("y", "n", "c"):
                apply_additional_filters = input("Invalid input. You must enter either 'y' for yes, 'n' for no, or 'c' for cancel: ").lower()

            # Apply filters if necessary or break the loop
            if apply_additional_filters == "y":
                filter_via_prompts(statuses_to_display)
            elif apply_additional_filters == "n":
                print(f"Not filtering further. Displaying {num_tasks} tasks...")
                break
            else:
                print("Cancelling status display.")
                cancel_display = True
                break

            num_tasks = len(statuses_to_display["Step Name"])

    # Display the statuses
    if num_tasks > 0 and not cancel_display:
        status_renderer.layout(status_data=statuses_to_display, study_title=workspace)
        status_renderer.render()

    if not cancel_display:
        for ustep in unstarted_steps:
            print(f"\n{ustep} has not started yet.")
            print()


def _display_summary(state_info: Dict[str, str], cb_help: bool):
    """
    Given a dict of state info for a step, print a summary of the task states.

    :param `state_info`: A dictionary of information related to task states for a step
    :param `cb_help`: True if colorblind assistance (using symbols) is needed. False otherwise.
    """
    # Build a summary list of task info
    print(f"\nSUMMARY:")
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
        if "count" in val and val["count"] > 0:
            value = val["count"]
        elif "total" in val:
            value = val["total"]
        elif "name" in val:
            value = val["name"]
        # Add the label and value as an entry to the summary
        if value:
            summary.append([label, value])

    # Display the summary
    print(tabulate(summary))
    print()


def display_high_lvl_status(tasks_per_step: Dict[str, int], step_tracker: Dict[str, List[str]], workspace: str, cb_help: bool):
    """
    Displays a high level overview of the status of a study. This includes
    progress bars for each step and a summary of the number of initialized,
    running, finished, cancelled, dry ran, failed, and unknown tasks.

    :param `tasks_per_step`: A dict that says how many tasks each step takes
    :param `step_tracker`:  a dict that says which steps have started and which haven't
    :param `workspace`:     the filepath of the study output
    :param `cb_help`: True if colorblind assistance (using symbols) is needed. False otherwise.
    """
    from merlin.study.status import get_step_statuses, status_list_to_dict  # pylint: disable=C0415
    print(f"{ANSI_COLORS['YELLOW']}Status for {workspace} as of {datetime.now()}:{ANSI_COLORS['RESET']}")
    terminal_size = shutil.get_terminal_size()
    progress_bar_width = terminal_size.columns//4

    for sstep in step_tracker["started_steps"]:
        # This dict will keep track of the number of tasks at each status
        state_info: Dict[State, str] = {
            "FINISHED": {"count": 0, "color": ANSI_COLORS["GREEN"], "fill": "█"},
            "CANCELLED": {"count": 0, "color": ANSI_COLORS["YELLOW"], "fill": "/"},
            "FAILED": {"count": 0, "color": ANSI_COLORS["RED"], "fill": "⣿"},
            "UNKNOWN": {"count": 0, "color": ANSI_COLORS["GREY"], "fill": "?"},
            "INITIALIZED": {"count": 0, "color": ANSI_COLORS["LIGHT_BLUE"]},
            "RUNNING": {"count": 0, "color": ANSI_COLORS["BLUE"]},
            "DRY_RUN": {"count": 0, "color": ANSI_COLORS["ORANGE"], "fill": "\\"},
            "TOTAL_TASKS": {"total": tasks_per_step[sstep]},
        }

        # Read in the statuses for this step
        step_workspace = f"{workspace}/{sstep}"
        status_dict = get_step_statuses(step_workspace, tasks_per_step[sstep])

        num_completed_tasks = 0
        num_rows = len(status_dict["Step Name"])
        for i in range(num_rows):
            # Increment the count for whatever the task status is
            state_info[status_dict["Status"][i]]["count"] += 1

            # Increment the number of completed tasks (not running or initialized)
            if status_dict["Status"][i] not in ("INITIALIZED", "RUNNING"):
                num_completed_tasks += 1
            
            # Add entries for task queue and worker name if applicable
            if "Task Queue" in status_dict and "TASK_QUEUE" not in state_info:
                state_info["TASK_QUEUE"] = {"name": status_dict["Task Queue"][i]}
            if "Worker Name" in status_dict and "WORKER_NAME" not in state_info:
                state_info["WORKER_NAME"] = {"name": status_dict["Worker Name"][i]}

        # Display the progress bar and summary for the step
        print(f"\n{sstep}\n")
        progress_bar(num_completed_tasks, tasks_per_step[sstep], state_info=state_info, suffix="Complete", length=progress_bar_width, cb_help=cb_help)
        _display_summary(state_info, cb_help)
        print("-"*(terminal_size.columns//2))

    # For each unstarted step, print an empty progress bar
    for ustep in step_tracker["unstarted_steps"]:
        print(f"\n{ustep}\n")
        progress_bar(0, 100, suffix="Complete", length=progress_bar_width)
        print(f"\n{ustep} has not started yet.\n")
        print("-"*(terminal_size.columns//2))


# Credit to this stack overflow post: https://stackoverflow.com/a/34325723
def progress_bar(current, total, state_info=None, prefix="", suffix="", decimals=1, length=80, fill="█", printEnd="\n", color=None, cb_help=False):
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
    :param `printEnd`:    end character (e.g. "\r", "\r\n") (Str)
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
        print(f'\r{prefix} |', end="")
        for key, val in state_info.items():
            # Only fill bar with completed tasks
            if key in ("INITIALIZED", "RUNNING", "TASK_QUEUE", "WORKER_NAME", "TOTAL_TASKS"):
                continue

            # Get the length to fill for this specific state
            partial_filled_length = int(length * val["count"] // total)

            if partial_filled_length > 0:
                if cb_help:
                    fill = val['fill']
                bar = fill * partial_filled_length
                print(f"{val['color']}{bar}", end="")
        
        # The remaining bar represents the number of tasks still incomplete
        remaining_bar = '-' * (length - total_filled_length)
        print(f'{ANSI_COLORS["RESET"]}{remaining_bar}| {percent}% {suffix}', end=printEnd)
    # Print a normal progress bar
    else:
        bar = fill * total_filled_length + '-' * (length - total_filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)


def tabulate_info(info, headers=None, color=None):
    """
    Display info in a table. Colorize the table if you'd like.
    Intended for use for functions outside of this file so they don't
    need to import tabulate.
    :param `info`: The info you want to tabulate.
    :param `headers`: A string or list stating what you'd like the headers to be.
                      Options: "firstrow", "keys", or List[str]
    :param `color`: An ANSI color.
    """
    # Adds the color at the start of the print
    if color:
        print(color, end="")

    # \033[0m resets color to white
    if headers:
        print(tabulate(info, headers=headers), ANSI_COLORS["RESET"])
    else:
        print(tabulate(info), ANSI_COLORS["RESET"])
