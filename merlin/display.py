##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Manages formatting for displaying information to the console.
"""
import logging
import os
import pprint
import shutil
import time
import traceback
from argparse import Namespace
from datetime import datetime
from multiprocessing import Pipe, Process
from multiprocessing.connection import Connection
from typing import Any, Dict, List, Union

from kombu import Connection as KombuConnection
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
    An extension of the multiprocessing's Process class that allows for
    custom handling of exceptions and inter-process communication.

    This class overrides the `run` method to capture exceptions that occur
    during the execution of the process and sends them back to the parent
    process via a pipe. It also provides a property to retrieve any
    exceptions that were raised during execution.

    Attributes:
        _pconn: The parent connection for inter-process communication.
        _cconn: The child connection for inter-process communication.
        exception: Stores the exception raised during the process run.

    Methods:
        run: Executes the process's main logic.
    """

    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self._pconn: Connection
        self._cconn: Connection
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        """
        Executes the process's main logic.

        This method overrides the default run method of the Process class.
        It attempts to run the process and captures any exceptions that occur.
        If an exception is raised, it sends the exception and its traceback
        back to the parent process via the child connection.
        """
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as e:  # pylint: disable=C0103,W0703
            trace_back = traceback.format_exc()
            self._cconn.send((e, trace_back))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self) -> Union[Exception, None]:
        """
        Retrieves the exception raised during the process execution.

        This property checks if there is an exception available from the
        parent connection. If an exception was raised, it is received and
        stored for later access.

        Returns:
            The exception raised during the process run, or None if no exception occurred.
        """
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


def check_server_access(sconf: Dict[str, Any]):
    """
    Check if there are any issues connecting to the servers.
    If there are, output the errors.

    This function iterates through a predefined list of servers and checks
    their connectivity based on the provided server configuration. If any
    connection issues are detected, the exceptions are collected and printed.

    Args:
        sconf: A dictionary containing server configurations, where keys
            represent server names and values contain connection details.
            The function expects keys corresponding to the servers being checked.
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


def _examine_connection(server: str, sconf: Dict[str, Any], excpts: Dict[str, Exception]):
    """
    Examine the connection to a specified server and handle any exceptions.

    This function attempts to establish a connection to the given server using
    the configuration provided in `sconf`. It utilizes a separate process to
    manage the connection attempt and checks for timeouts. If the connection
    fails or times out, the error is recorded in the `excpts` dictionary.

    Args:
        server: A string representing the name of the server to connect to.
            This should correspond to a key in the `sconf` dictionary.
        sconf: A dictionary containing server configurations, where keys
            represent server names and values contain connection details.
        excpts: A dictionary to store exceptions encountered during the
            connection attempt, with server names as keys and exceptions
            as values.
    """
    from merlin.config import broker, results_backend  # pylint: disable=C0415

    connect_timeout = 60
    try:
        ssl_conf = None
        if "broker" in server:
            ssl_conf = broker.get_ssl_config()
        if "results" in server:
            ssl_conf = results_backend.get_ssl_config()
        conn = KombuConnection(sconf[server], ssl=ssl_conf)
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
    Prints useful configuration information for the Merlin application to the console.

    This function retrieves and displays the connection strings and SSL configurations
    for the broker and results servers. It handles any exceptions that may occur during
    the retrieval process, providing error messages for any issues encountered.
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


def display_multiple_configs(files: List[str], configs: List[Dict]):
    """
    Logic for displaying multiple Merlin config files.

    Args:
        files: List of merlin config files
        configs: List of merlin configurations
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
def print_info(args: Namespace):  # pylint: disable=W0613
    """
    Provide version and location information about python and packages to
    facilitate user troubleshooting. Also provides info about server connections
    and configurations.

    Note:
        The `args` parameter is currently unused but is included for
        compatibility with the command-line interface (CLI) in case we decide to use
        args here in the future.

    Args:
        args: parsed CLI arguments (currently unused).
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
    Displays a low-level overview of the status of a study in a task-by-task format.

    Each task will display the following details:
        - Step name
        - Worker name
        - Task queue
        - Command and restart parameters
        - Step workspace
        - Step status
        - Return code
        - Elapsed time
        - Run time
        - Number of restarts

    If the number of tasks exceeds a certain limit and the pager is disabled, the user
    will be prompted to apply additional filters to avoid overwhelming the terminal output,
    unless the prompts are disabled through the no-prompts flag.

    Args:
        status_obj (study.status.DetailedStatus): An instance of
            [`DetailedStatus`][study.status.DetailedStatus] containing information about
            the current state of tasks.
        test_mode: If True, runs the function in testing mode, suppressing output and
            reducing the task limit for prompts. Defaults to False.
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
    Prints a summary of task states based on the provided state information.

    This function takes a dictionary of state information for a step and
    prints a formatted summary, including optional colorblind assistance using
    symbols if specified.

    Args:
        state_info: A dictionary containing information related to task states
            for a step. Each entry should correspond to a specific task state
            with its associated properties (e.g., count, total, name).
        cb_help: If True, provides colorblind assistance by using symbols in the
            display. Defaults to False for standard output.
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
    status_obj: "Status", non_workspace_keys: set, test_mode: bool = False  # noqa: F821
) -> Dict:
    """
    Displays a high-level overview of the status of a study, including progress bars for each step
    and a summary of the number of initialized, running, finished, cancelled, dry ran, failed, and
    unknown tasks.

    The function prints a summary for each step and collects state information. In test mode,
    it suppresses output and returns a dictionary of state information instead.

    Args:
        status_obj (study.status.Status): An instance of [`Status`][study.status.Status] containing
            information about task states and associated data for the study.
        non_workspace_keys: A set of keys in requested_statuses that are not workspace keys.
            Typically includes keys like "parameters", "task_queue", and "workers".
        test_mode: If True, runs in test mode; suppresses printing and returns a dictionary
            of state information for each step. Defaults to False.

    Returns:
        An empty dictionary in regular mode. In test mode, returns a dictionary containing
            the state information for each step.
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
    current: int,
    total: int,
    state_info: Dict[str, Any] = None,
    prefix: str = "",
    suffix: str = "",
    decimals: int = 1,
    length: int = 80,
    fill: str = "█",
    print_end: str = "\n",
    color: str = None,
    cb_help: bool = False,
):
    """
    Prints a customizable progress bar that visually represents the completion percentage
    relative to a given total.

    The function can display additional state information for detailed tracking, including
    support for color customization and adaptation for color-blind users. It updates the
    display based on current progress and optionally accepts state information to adjust the
    appearance of the progress bar.

    Args:
        current: Current progress value.
        total: Total value representing 100% completion.
        state_info: Dictionary containing state information about tasks. This can override
            color settings and modifies how the progress bar is displayed.
        prefix: Optional prefix string to display before the progress bar.
        suffix: Optional suffix string to display after the progress bar.
        decimals: Number of decimal places to display in the percentage (default is 1).
        length: Character length of the progress bar (default is 80).
        fill: Character used to fill the progress bar (default is "█").
        print_end: Character(s) to print at the end of the line (e.g., '\\r', '\\n').
        color: ANSI color string for the progress bar. Overrides state_info colors.
        cb_help: If True, provides color-blind assistance by adapting the fill characters.
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
