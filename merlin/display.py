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
from multiprocessing import Pipe, Process

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

# TODO: make these color blind compliant (see https://mikemol.github.io/technique/colorblind/2018/02/11/color-safe-palette.html)
ANSI_COLORS = {
    "RESET": "\033[0m",
    "GREY": "\033[90m",
    "RED": "\033[91m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "BLUE": "\033[94m",
    "MAGENTA": "\033[95m",
    "CYAN": "\033[96m",
    "WHITE": "\033[97m"
}


# TODO: make these color blind compliant
# (see https://mikemol.github.io/technique/colorblind/2018/02/11/color-safe-palette.html)
ANSI_COLORS = {
    "RESET": "\033[0m",
    "GREY": "\033[90m",
    "RED": "\033[91m",
    "GREEN": "\033[92m",
    "YELLOW": "\033[93m",
    "BLUE": "\033[94m",
    "MAGENTA": "\033[95m",
    "CYAN": "\033[96m",
    "WHITE": "\033[97m",
}


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


def _filter_tasks(user_filter, status_info):
    """Helper function to filter tasks based on user input."""
    filter_key = {
        "i": "INITIALIZED",
        "run": "RUNNING",
        "finished": "FINISHED",
        "failed": "FAILED",
        "restart": "RESTARTED",
        "c": "CANCELLED",
        "u": "UNKNOWN"
    }
    for entry in status_info[:]:
        if entry[1] != "Status" and entry[1] != filter_key[user_filter]:
            status_info.remove(entry)
    
    if len(status_info) == 1:
        print(f"{ANSI_COLORS['RED']}No {filter_key[user_filter]} tasks found.{ANSI_COLORS['RESET']}")


def _display_low_lvl(step_tracker, workspace):
    """
    Displays a low level overview of the status of a study. This is a task-by-task
    status display where each task will show:
    step name, step status, return code, elapsed time, run time, num restarts, dir path, task queue, worker name
    in that order. If too many tasks are found, prompts will appear for the user to decide
    what to do (that way we don't overload the terminal).

    :param `step_tracker`:  a dict that says which steps have started and which haven't (Dict)
    :param `workspace`:     the filepath of the study output (Str)
    """
    status_info = [["Step Name", "Status", "Return Code", "Elapsed Time", "Run Time", "Restarts", "Step Workspace", "Task Queue", "Worker Name", "Num Samples"]]
    # TODO:
    # - Find out how Maestro displays their status table and use that
    # - Figure out what to do with restarted tasks
    for sstep in step_tracker["started_steps"]:
        for root, dirs, files in os.walk(f"{workspace}/{sstep}"):
            if "MERLIN_STATUS" in files:
                with open(f"{root}/MERLIN_STATUS", "r") as f:
                    status_info.append(f.readline().split(" "))
                
    # Subtract 1 for the header row
    num_available_tasks = len(status_info) - 1
    # Check how many tasks we're about to display in case it's a ton
    if num_available_tasks > 250:
        print(f"{ANSI_COLORS['YELLOW']}{num_available_tasks} tasks found.{ANSI_COLORS['RESET']}")
        # See what the user would like to do in this case
        user_input = input("Would you like to display all tasks, filter the tasks, or cancel this operation? (a/f/c): ").lower()
        while user_input != 'a' and user_input != 'f' and user_input != 'c':
            print("Invalid input. ", end="")
            user_input = input("Please enter 'a' for all, 'f' for filter, or 'c' for cancel: ")
        
        # Display all tasks
        if user_input == 'a':
            print(tabulate(status_info, headers="firstrow"))
        # Filter tasks
        elif user_input == 'f':
            # Print the filtering options for the user
            print("\nFilter Options:")
            filter_options = {"Specific Number of Tasks": "Enter an integer", "Initialized Tasks": "i", "Running Tasks": "run", "Finished Tasks": "finished", "Failed Tasks": "failed", "Restarted Tasks": "restart", "Cancelled Tasks": "c", "Tasks with Unknown Status": "u", "Exit Without Filtering": "e"}
            print(tabulate([(k, v) for k, v in filter_options.items()]))
            
            # Obtain and validate the filter provided by the user
            invalid_filter = True
            while invalid_filter:
                user_filter = input("How would you like to filter the tasks? ")
                try:
                    user_filter = int(user_filter)
                    if user_filter > 0 and user_filter < num_available_tasks:
                        invalid_filter = False
                    else:
                        print(f"Integer must be greater than 0 and less than {num_available_tasks}.")
                except ValueError:
                    user_filter.lower()
                    if user_filter in filter_options.values() or user_filter == "enter an integer":
                        invalid_filter = False
                    else:
                        print(f"Invalid input.")

            # Display a certain amount of tasks provided by the user
            if isinstance(user_filter, int):
                print(tabulate(status_info[:user_filter+1], headers="firstrow"))
            # Apply the non-numerical filter given by the user
            else:
                # Exit without printing
                if user_filter == "e":
                    pass
                # Filter the tasks and print
                else:
                    _filter_tasks(user_filter, status_info)
                    print(f"Found {len(status_info) - 1} tasks matching your filter.")
                    # Display 50 tasks at a time so we don't overload the terminal
                    for i in range(1, len(status_info), 50):
                        if i == 1:
                            print(tabulate(status_info[0:i+50], headers="firstrow"))
                        else:
                            print(tabulate(status_info[i:i+50]))
                        
                        # Only need to ask the user this if there will be more tasks to display on the next iteration
                        if i+50 < len(status_info):
                            # See if the user wants to continue displaying tasks matching their filter
                            user_continue = input("Display the next 50 tasks? (y/n) ").lower()
                            while user_continue != "y" and user_continue != "n":
                                user_continue = input("Invalid input. Must be either 'y' for yes or 'n' for no.").lower()
                            if user_continue == 'n':
                                break

        # Cancel the display
        elif user_input == 'c':
            pass
        # We should never be here
        else:
            raise ValueError("Something went wrong while getting user input.")

    # Less than 250 tasks to display so just show them all
    elif num_available_tasks < 250 and num_available_tasks > 0:
        print(tabulate(status_info, headers="firstrow"))

    for ustep in step_tracker["unstarted_steps"]:
        print(f"\n{ustep} has not started yet.")
        print()


def _display_high_lvl(step_tracker, workspace):
    """
    Displays a high level overview of the status of a study. This includes
    progress bars for each step and a summary of the number of initialized,
    running, finished, cancelled, dry ran, failed, and unknown tasks.

    :param `step_tracker`:  a dict that says which steps have started and which haven't (Dict)
    :param `workspace`:     the filepath of the study output (Str)
    """
    print()
    terminal_size = shutil.get_terminal_size()
    progress_bar_width = terminal_size.columns//4

    for sstep in step_tracker["started_steps"]:
        state_info: Dict[State, str] = {
            "INITIALIZED": [0, ANSI_COLORS["CYAN"]],
            "RUNNING": [0, ANSI_COLORS["MAGENTA"]],
            "FINISHED": [0, ANSI_COLORS["GREEN"]],
            "CANCELLED": [0, ANSI_COLORS["YELLOW"]],
            "DRY_RUN": [0, ANSI_COLORS["BLUE"]],
            "FAILED": [0, ANSI_COLORS["RED"]],
            "UNKNOWN": [0, ANSI_COLORS["GREY"]],
            "TOTAL_TASKS": [0, ANSI_COLORS["WHITE"]],
            "TASK_QUEUE": ["", ANSI_COLORS["WHITE"]],
            "WORKER_NAME": ["", ANSI_COLORS["WHITE"]],
        }

        # Count number of tasks in each state
        # TODO: get the total number of tasks, this one below isn't correct
        # - can we get the parameter names and compare those against the dirs
        #   at the top of the directory heirarchy? Does not give us a number tho...
        for root, dirs, files in os.walk(f"{workspace}/{sstep}"):
            if "MERLIN_STATUS" in files:
                status_path = f"{root}/MERLIN_STATUS"
                with open(status_path, "r") as f:
                    task_info = f.readline().split(" ")
                
                state_info[task_info[1]][0] += 1
                state_info["TOTAL_TASKS"][0] += 1

                if not state_info["TASK_QUEUE"][0]:
                    state_info["TASK_QUEUE"][0] = task_info[7]
                if not state_info["WORKER_NAME"][0]:
                    state_info["WORKER_NAME"][0] = task_info[8]

        # Get the number of finished tasks (not running or initialized)
        finished_tasks = state_info["TOTAL_TASKS"][0] - (state_info['INITIALIZED'][0] + state_info['RUNNING'][0])

        # Display the progress bar
        progress_bar(finished_tasks, state_info["TOTAL_TASKS"][0], state_info=state_info, prefix=f"{sstep}", suffix="Complete", length=progress_bar_width)

        # Display a table summary of task info
        print(f"\nSUMMARY:")
        summary = []
        for key, val in state_info.items():
            if (isinstance(val[0], int) and val[0] > 0) or isinstance(val[0], str):
                key = f"{val[1]}{key}{ANSI_COLORS['RESET']}"
                summary.append([key, val[0]])
        print(tabulate(summary))
        print()

    # For each unstarted step, print an empty progress bar
    for ustep in step_tracker["unstarted_steps"]:
        progress_bar(0, 100, prefix=f"{ustep}", suffix="Complete", length=progress_bar_width)
        print(f"\n{ustep} has not started yet.")
        print()
    
    print(f"{ANSI_COLORS['YELLOW']}If you'd like to see task-by-task info for a step, use the --steps flag with the 'merlin status' command.{ANSI_COLORS['RESET']}")


def display_status(workspace, step_tracker, low_lvl):
    """
    Displays the status of a study.

    :param `workspace`: The output directory for a study (Str)
    :param `step_tracker`: A dict that says which steps have started and which haven't (Dict)
    :param `low_lvl`: A boolean to determine whether to display the low or high level display (Bool)
    """
    # TODO:
    # - Get total number of tasks for the study (needed for both displays)
    if low_lvl:
        _display_low_lvl(step_tracker, workspace)
    else:
        _display_high_lvl(step_tracker, workspace)


# Credit to this stack overflow post: https://stackoverflow.com/a/34325723
def progress_bar(current, total, state_info=None, prefix="", suffix="", decimals=1, length=80, fill="█", printEnd="\n", color=None):
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
    """
    # Set the color of the bar
    if color:
        fill = f"{color}{fill}{ANSI_COLORS['RESET']}"
    
    # Get the percentage done and the total fill length of the bar
    percent = ("{0:." + str(decimals) + "f}").format(100 * (current / float(total)))
    total_filled_length = int(length * current // total)

    # Print a progress bar based on state of the study
    if state_info:
        print(f'\r{prefix} |', end="")
        for key, val in state_info.items():
            # Only fill bar with completed tasks
            if key == "INITIALIZED" or key == "RUNNING" or key == "TASK_QUEUE" or key == "WORKER_NAME" or key == "TOTAL_TASKS":
                continue

            # Get the length to fill for this specific state
            partial_filled_length = int(length * val[0] // total)
            if partial_filled_length > 0:
                fill = f"{val[1]}{fill}{ANSI_COLORS['RESET']}"
                bar = fill * partial_filled_length
                print(f'{bar}', end="")
        
        # The remaining bar represents the number of tasks still incomplete
        remaining_bar = '-' * (length - total_filled_length)
        print(f'{ANSI_COLORS["RESET"]}{remaining_bar}| {percent}% {suffix}', end=printEnd)
    # Print a normal progress bar
    else:
        bar = fill * total_filled_length + '-' * (length - total_filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)


# TODO: do we even want this function?
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
