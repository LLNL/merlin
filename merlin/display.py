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
from datetime import datetime
from multiprocessing import Pipe, Process
from typing import Dict, List, Union

from kombu import Connection
from tabulate import tabulate

from merlin.ascii_art import banner_small
from merlin.config import broker, results_backend
from merlin.config.configfile import default_config_info, CONFIG
from merlin.study.celeryadapter import get_queues, query_celery_queues
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


# TODO is this the same as merlin status --queue-info? Should we scrap this?
def print_queue_info(queues):
    """
    For each queue provided in the `queues` argument this function will
    print the name of the queue, the number of tasks attached to it,
    and the number of workers assigned to it.

    :param `queues`: a list of queue names that we'll print info for
    """
    print(banner_small)

    existing_queues, _ = get_queues(app)
    existing_queue_names = existing_queues.keys()
    # Case 1: no queues exist
    if len(existing_queues) == 0:
        LOG.warning(f"No queues found. Are your workers running this queue yet?")
        return
    # Case 2: no specific queue provided, display info for all queues
    elif len(queues) == 0:
        found_queues = query_celery_queues(existing_queue_names)
    # Case 3: specific queue provided, display info for that(those) queue(s)
    else:
        full_queue_names = []
        
        for queue in queues:
            full_queue_names.append(f"{CONFIG.celery.queue_tag}{queue}")

        found_queues = query_celery_queues(full_queue_names)

    found_queues.insert(0, ("Queue Name", "Task Count", "Worker Count"))
    print(tabulate(found_queues, headers="firstrow"), "\n")


def paginate_statuses(status_info: List[List[str]], num_tasks: int):
    """
    Use pagination to display statuses. Here we display 50 tasks at a time and
    ask the user if they want to see the next 50 or not.

    :param `status_info`: A list of task statuses to display here
    :param `num_tasks`: The total number of task statuses in status_info (len(status_info))
    """
    # Display 50 tasks at a time so we don't overload the terminal
    for i in range(1, num_tasks, 50):
        if i == 1:
            print(tabulate(status_info[0:i+50], headers="firstrow"))
        else:
            print(tabulate(status_info[i:i+50]))
        
        # Only need to ask the user this if there will be more tasks to display on the next iteration
        if i+50 < num_tasks:
            # See if the user wants to continue displaying tasks
            user_continue = input("Display the next 50 tasks? (y/n) ").lower()
            while user_continue != "y" and user_continue != "n":
                user_continue = input("Invalid input. Must be either 'y' for yes or 'n' for no.").lower()
            if user_continue == 'n':
                break

def _filter_by_status(status_filters: List[str], status_info: List[List[str]]):
    """
    Filter the list of status info by certain status filters provided by the user.

    :param `status_filters`: A list of filters provided by the user from the --task-status flag
    :param `status_info`: A list of task statuses read from MERLIN_STATUS files
    """
    # Add the column label here so we don't accidentally remove that line
    status_filters.append("Status")

    for entry in status_info[:]:
        # entry[1] is the status of that specific task; if it doesn't match the filter, remove it
        if entry[1] not in status_filters:
            status_info.remove(entry)

    # If just the header line is left then there were no tasks found for the filters provided
    if len(status_info) == 1:
        status_filters.remove("Status")
        print(f"{ANSI_COLORS['RED']}No tasks found for the filters {status_filters}.{ANSI_COLORS['RESET']}")


def get_user_filter() -> Union[int, List[str]]:
    """
    Get a filter on the statuses to display from the user. Possible options
    for filtering:
        - An int representing the max number of tasks to display -> equivalent to the --max-tasks flag
        - A list of statuses (see valid_status_filters below) -> equivalent to the --task-status flag
        - An exit keyword to leave the merlin status command without displaying anything

    :returns: An int or a list of strings to filter by
    """
    # Print the filtering options for the user
    print("\nFilter Options:")
    valid_status_filters = ('INITIALIZED', 'RUNNING', 'FINISHED', 'FAILED', 'RESTART', 'CANCELLED', 'UNKNOWN')
    valid_exit_filters = ('E', 'EXIT')
    filter_options = {
        "Display a specific number of tasks": "Enter an integer greater than 0",
        "Filter by status": f"Enter a comma separated list of the following statuses you'd like to see: {valid_status_filters}",
        "Exit without filtering": f"Enter one of the following: {valid_exit_filters}"
    }
    print(tabulate([(k, v) for k, v in filter_options.items()]))

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
            for entry in user_filter:
                entry = entry.upper()
                if entry not in valid_status_filters and entry not in valid_exit_filters:
                    invalid_filter = True
                    print(f"Invalid input: {entry}. Input must be one of {valid_status_filters} or one of {valid_exit_filters}")
                    break
                else:
                    invalid_filter = False

    return user_filter


def _low_lvl_with_prompts(status_info: List[List[str]], num_tasks: int):
    """
    Interact with the user to manage how many/which tasks are displayed. This helps to
    prevent us from overloading the terminal by displaying a bazillion tasks at once.

    :param `status_info`: A list of task statuses read from MERLIN_STATUS files
    :param `num_tasks`: The total number of task statuses in status_info (len(status_info))
    """
    print(f"{ANSI_COLORS['YELLOW']}{num_tasks - 1} tasks found.{ANSI_COLORS['RESET']}")
    # See what the user would like to do in this case
    user_input = input("Would you like to display all tasks, filter the tasks, or cancel this operation? (a/f/c): ").lower()
    while user_input != 'a' and user_input != 'f' and user_input != 'c':
        print("Invalid input. ", end="")
        user_input = input("Please enter 'a' for all, 'f' for filter, or 'c' for cancel: ")
    
    # Display all tasks
    if user_input == 'a':
        paginate_statuses(status_info, num_tasks)
    # Filter tasks
    elif user_input == 'f':
        # Get the filter from the user
        user_filter = get_user_filter()
        # Display a certain amount of tasks provided by the user
        if isinstance(user_filter, int):
            if user_filter > num_tasks:
                user_filter = num_tasks
            print(tabulate(status_info[:user_filter+1], headers="firstrow"))
        # Apply the non-numerical filter given by the user
        else:
            # Exit without displaying anything
            if "E" in user_filter or "EXIT" in user_filter:
                pass
            # Filter by status and display the results using pagination
            else:
                _filter_by_status(user_filter, status_info)
                num_tasks = len(status_info)
                print(f"{ANSI_COLORS['YELLOW']}Found {num_tasks - 1} tasks matching your filter.{ANSI_COLORS['RESET']}")
                paginate_statuses(status_info, num_tasks)

    # Cancel the display
    elif user_input == 'c':
        pass
    # We should never be here
    else:
        raise ValueError("Something went wrong while getting user input.")



def _display_low_lvl(step_tracker: Dict[str, List[str]], workspace: str, args: "Argparse Namespace"):
    """
    Displays a low level overview of the status of a study. This is a task-by-task
    status display where each task will show:
    step name, step status, return code, elapsed time, run time, num restarts, dir path, task queue, worker name
    in that order. If too many tasks are found, prompts will appear for the user to decide
    what to do (that way we don't overload the terminal).

    :param `step_tracker`: A dict that says which steps have started and which haven't
    :param `workspace`: the filepath of the study output
    :param `args`: The CLI arguments provided via the user
    """
    from merlin.study.status import read_status  # pylint: disable=C0415
    print(f"{ANSI_COLORS['YELLOW']}Status for {workspace} as of {datetime.now()}:{ANSI_COLORS['RESET']}")

    # Initialize a list of lists that we'll use to display the status info
    status_info = [["Step Name", "Status", "Return Code", "Elapsed Time", "Run Time", "Restarts", "Step Workspace", "Task Queue", "Worker Name"]]
    # TODO:
    # - Find out how Maestro displays their status table and use that
    # - Figure out what to do with restarted tasks

    # Read in the statuses for the tasks in this step
    for sstep in step_tracker["started_steps"]:
        for root, dirs, files in os.walk(f"{workspace}/{sstep}"):
            if "MERLIN_STATUS" in files:
                timeout_message = f"Timed out while reading {root}/MERLIN_STATUS for low level display."
                all_statuses = read_status(root, timeout_message=timeout_message).split("\n")
                all_statuses.remove('')
                for status in all_statuses:
                    status_info.append(status.split(" "))

    # Filter by task status if necessary
    if args.task_status:
        _filter_by_status(args.task_status, status_info)

    # Get the number of tasks associated with this step
    num_tasks = len(status_info)

    # Only display a certain amount of tasks
    if (args.no_prompts and args.max_tasks) or args.max_tasks:
        print(f"{ANSI_COLORS['YELLOW']}Found {num_tasks - 1} tasks.{ANSI_COLORS['RESET']}")
        if args.max_tasks > num_tasks:
            args.max_tasks = num_tasks
        print(f"Displaying {args.max_tasks} of these tasks...")
        print(tabulate(status_info[:args.max_tasks+1], headers="firstrow"))
    # Don't show any prompts, just display everything
    elif args.no_prompts or (0 < num_tasks < 251):
        print(f"{ANSI_COLORS['YELLOW']}Found {num_tasks - 1} tasks.{ANSI_COLORS['RESET']}")
        print(tabulate(status_info, headers="firstrow"))
    # Filter has already been applied, now just paginate the display
    elif args.task_status:
        print(f"{ANSI_COLORS['YELLOW']}Found {num_tasks - 1} tasks matching your filter.{ANSI_COLORS['RESET']}")
        paginate_statuses(status_info, num_tasks)
    # No filters have been applied, ask the user for prompts to help limit the display
    else:
        _low_lvl_with_prompts(status_info, num_tasks)

    for ustep in step_tracker["unstarted_steps"]:
        print(f"\n{ustep} has not started yet.")
        print()


def _display_high_lvl(tasks_per_step: Dict[str, int], step_tracker: Dict[str, List[str]], workspace: str, cb_help: bool):
    """
    Displays a high level overview of the status of a study. This includes
    progress bars for each step and a summary of the number of initialized,
    running, finished, cancelled, dry ran, failed, and unknown tasks.

    :param `tasks_per_step`: A dict that says how many tasks each step takes
    :param `step_tracker`:  a dict that says which steps have started and which haven't
    :param `workspace`:     the filepath of the study output
    :param `cb_help`: True if colorblind assistance (using symbols) is needed. False otherwise.
    """
    from merlin.study.status import read_status  # pylint: disable=C0415
    print(f"{ANSI_COLORS['YELLOW']}Status for {workspace} as of {datetime.now()}:{ANSI_COLORS['RESET']}")
    print()
    terminal_size = shutil.get_terminal_size()
    progress_bar_width = terminal_size.columns//4

    for sstep in step_tracker["started_steps"]:
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

        # Count number of tasks in each state
        for root, dirs, files in os.walk(f"{workspace}/{sstep}"):
            if "MERLIN_STATUS" in files:
                # Read in the statuses for the tasks in this step
                status_file = f"{root}/MERLIN_STATUS"
                timeout_message = f"Timed out while reading {status_file} for high level display."
                all_statuses = read_status(root, timeout_message=timeout_message).split("\n")
                all_statuses.remove('')

                for status in all_statuses:
                    # Parse the task_info into a list
                    task_info = status.split(" ")
                    # Increment the count for whatever the task status is
                    state_info[task_info[1]]["count"] += 1

                    # If the length is 9 rather than 7 then we have a task queue and worker to show
                    if len(task_info) == 9:
                        # Save the task queue and worker name if they don't exist yet
                        if "TASK_QUEUE" not in state_info:
                            state_info["TASK_QUEUE"] = {"name": task_info[7]}
                        if "WORKER_NAME" not in state_info:
                            state_info["WORKER_NAME"] = {"name": task_info[8]}

        # Get the number of finished tasks (not running or initialized)
        completed_tasks = [
            state_info["FINISHED"]["count"],
            state_info["FAILED"]["count"],
            state_info["DRY_RUN"]["count"],
            state_info["CANCELLED"]["count"],
            state_info["UNKNOWN"]["count"]
        ]
        num_completed_tasks = sum(completed_tasks)

        # Display the progress bar
        progress_bar(num_completed_tasks, state_info["TOTAL_TASKS"]["total"], state_info=state_info, prefix=f"{sstep}", suffix="Complete", length=progress_bar_width, cb_help=cb_help)

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

    # For each unstarted step, print an empty progress bar
    for ustep in step_tracker["unstarted_steps"]:
        progress_bar(0, 100, prefix=f"{ustep}", suffix="Complete", length=progress_bar_width)
        print(f"\n{ustep} has not started yet.")
        print()
    
    print(f"{ANSI_COLORS['YELLOW']}If you'd like to see task-by-task info for a step, use the --steps flag with the 'merlin status' command.{ANSI_COLORS['RESET']}")


def display_status(workspace: str, tasks_per_step: Dict[str, int], step_tracker: Dict[str, List[str]], low_lvl: bool, args: "Argparse Namespace"):
    """
    Displays the status of a study.

    :param `workspace`: The output directory for a study
    :param `tasks_per_step`: A dict that says how many tasks each step takes
    :param `step_tracker`: A dict that says which steps have started and which haven't
    :param `low_lvl`: A boolean to determine whether to display the low or high level display
    :param `args`: The CLI arguments provided via the user
    """
    if low_lvl:
        _display_low_lvl(step_tracker, workspace, args)
    else:
        _display_high_lvl(tasks_per_step, step_tracker, workspace, args.cb_help)


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
                # bar = fill * partial_filled_length
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
