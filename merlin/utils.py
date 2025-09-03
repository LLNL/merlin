##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for project-wide utility functions.
"""
import getpass
import logging
import os
import pickle
import re
import socket
import subprocess
import sys
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any, Callable, Dict, Generator, List, Tuple, Union

import numpy as np
import pkg_resources
import psutil
import yaml
from tabulate import tabulate


LOG = logging.getLogger(__name__)
ARRAY_FILE_FORMATS = ".npy, .csv, .tab"
DEFAULT_FLUX_VERSION = "0.48.0"


def get_user_process_info(user: str = None, attrs: List[str] = None) -> List[Dict]:
    """
    Return a list of process information for all of the user's running processes.

    This function retrieves and returns details about the currently running processes
    for a specified user. If no user is specified, it defaults to the current user.
    It can also return information for all users if specified.

    Args:
        user: The username for which to retrieve process information. If set to
            'all_users', retrieves processes for all users. Defaults to the current
            user's username if not provided.
        attrs: A list of attributes to include in the process information. Defaults
            to ["pid", "name", "username", "cmdline"] if None. If "username" is not
            included in the list, it will be added.

    Returns:
        A list of dictionaries containing the specified attributes for each process
            belonging to the specified user or all users if 'all_users' is specified.
    """
    if attrs is None:
        attrs = ["pid", "name", "username", "cmdline"]

    if "username" not in attrs:
        attrs.extend("username")

    if user is None:
        user = getpass.getuser()

    if user == "all_users":
        return [p.info for p in psutil.process_iter(attrs=attrs)]
    return [p.info for p in psutil.process_iter(attrs=attrs) if user in p.info["username"]]


def check_pid(pid: int, user: str = None) -> bool:
    """
    Check if a given process ID (PID) is in the process list for a specified user.

    This function determines whether a specific PID is currently running
    for the specified user. If no user is specified, it defaults to the
    current user. It can also check for all users if specified.

    Args:
        pid: The process ID to check for in the process list.
        user: The username for which to check the process. If set to 'all_users',
            checks processes for all users. Defaults to the current user's username
            if not provided.

    Returns:
        True if the specified PID is found in the process list for the given user,
            False otherwise.
    """
    user_processes = get_user_process_info(user=user)
    for process in user_processes:
        if int(process["pid"]) == pid:
            return True
    return False


def get_pid(name: str, user: str = None) -> List[int]:
    """
    Return the process ID(s) (PID) of processes with the specified name.

    This function retrieves the PID(s) of all running processes that match
    the given name for a specified user. If no user is specified, it defaults
    to the current user. It can also retrieve PIDs for all users if specified.

    Args:
        name: The name of the process to search for.
        user: The username for which to retrieve the process IDs. If set to
            'all_users', retrieves processes for all users. Defaults to the
            current user's username if not provided.

    Returns:
        A list of PIDs for processes matching the specified name. Returns None
            if no matching processes are found.
    """
    user_processes = get_user_process_info(user=user)
    name_list = [p["pid"] for p in user_processes if name in p["name"]]
    if name_list:
        return name_list
    return None


def get_procs(name: str, user: str = None) -> List[Tuple[int, str]]:
    """
    Return a list of tuples containing the process ID (PID) and command line
    of processes with the specified name.

    This function retrieves all running processes that match the given name
    for a specified user. If no user is specified, it defaults to the current
    user. It can also retrieve processes for all users if specified.

    Args:
        name: The name of the process to search for.
        user: The username for which to retrieve the process information.
            If set to 'all_users', retrieves processes for all users.
            Defaults to the current user's username if not provided.

    Returns:
        A list of tuples, each containing the PID and command line of processes
            matching the specified name. Returns an empty list if no matching
            processes are found.
    """
    user_processes = get_user_process_info(user=user)
    procs = [(p["pid"], p["cmdline"]) for p in user_processes if name in p["name"]]
    return procs


def is_running_psutil(cmd: str, user: str = None) -> bool:
    """
    Determine if a process with the given command is currently running.

    This function checks for the existence of any running processes that
    match the specified command. It uses the `psutil` library to gather
    process information instead of making a call to the 'ps' command.

    Args:
        cmd: The command or command line snippet to search for in running
            processes.
        user: The username for which to check running processes. If set to
            'all_users', checks processes for all users. Defaults to the
            current user's username if not provided.

    Returns:
        True if at least one matching process is found; otherwise, False.
    """
    user_processes = get_user_process_info(user=user)
    return any(cmd in " ".join(p["cmdline"]) for p in user_processes)


def is_running(name: str, all_users: bool = False) -> bool:
    """
    Determine if a process with the specified name is currently running.

    This function checks for the existence of a running process with the
    provided name by executing the 'ps' command. It can be configured to
    check processes for all users or just the current user.

    Args:
        name: The name of the process to search for.
        all_users: If True, checks for processes across all users. Defaults
            to False, which checks only the current user's processes.

    Returns:
        True if a process with the specified name is found; otherwise, False.
    """
    cmd = ["ps", "ux"]

    if all_users:
        cmd[1] = "aux"

    # pylint: disable=consider-using-with
    try:
        process_status = subprocess.Popen(cmd, stdout=subprocess.PIPE, encoding="utf8").communicate()[0]
    except TypeError:
        process_status = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    # pylint: enable=consider-using-with

    if name in process_status:
        return True

    return False


def expandvars2(path: str) -> str:
    """
    Replace shell variables in the given path with their corresponding
    environment variable values.

    This function expands shell-style variable references (e.g., $VAR)
    in the input path using the current environment variables. It also
    ensures that any escaped dollar signs (e.g., \\$) are not expanded.

    Args:
        path: The input path containing shell variable references to be expanded.

    Returns:
        The path with shell variables replaced by their corresponding values
            from the environment, with unescaped variables expanded.
    """
    return re.sub(r"(?<!\\)\$[A-Za-z_][A-Za-z0-9_]*", "", os.path.expandvars(path))


def regex_list_filter(regex: str, list_to_filter: List[str], match: bool = True) -> List[str]:
    """
    Apply a regex filter to a list.

    This function filters a given list based on a specified regular expression.
    Depending on the `match` parameter, it can either match the entire string
    or search for the regex pattern within the strings of the list.

    Args:
        regex: The regular expression to use for filtering the list.
        list_to_filter: The list of strings to be filtered based on the regex.
        match: If True, uses re.match to filter items that match the regex from
            the start. If False, uses re.search to filter items that contain the
            regex pattern.

    Returns:
        A new list containing the filtered items that match the regex.
    """
    r = re.compile(regex)  # pylint: disable=C0103
    if match:
        return list(filter(r.match, list_to_filter))
    return list(filter(r.search, list_to_filter))


def apply_list_of_regex(
    regex_list: List[str], list_to_filter: List[str], result_list: List[str], match: bool = False, display_warning: bool = True
):
    """
    Apply a list of regex patterns to a list and accumulate the results.

    This function takes each regex from the provided list of regex patterns
    and applies it to the specified list. The results of each successful
    match or search are appended to a result list. Optionally, it can display
    a warning if a regex does not match any item in the list.

    Args:
        regex_list: A list of regular expressions to apply to the `list_to_filter`.
        list_to_filter: The list of strings that the regex patterns will be applied to.
        result_list: The list where results of the regex filters will be appended.
        match: If True, uses re.match for applying the regex. If False, uses re.search.
        display_warning: If True, displays a warning message when no matches are
            found for a regex.

    Side Effect:
        This function modifies the `result_list` in place.
    """
    for regex in regex_list:
        filter_results = set(regex_list_filter(regex, list_to_filter, match))

        if not filter_results:
            if display_warning:
                LOG.warning(f"No regex match for {regex}.")
        else:
            result_list += filter_results


def load_yaml(filepath: str) -> Dict:
    """
    Safely read a YAML file and return its contents.

    Args:
        filepath: The file path to the YAML file to be read.

    Returns:
        A dict representing the contents of the YAML file.
    """
    with open(filepath, "r") as _file:
        return yaml.safe_load(_file)


def get_yaml_var(entry: Dict[str, Any], var: str, default: Any) -> Any:
    """
    Retrieve the value associated with a specified key from a YAML dictionary.

    This function attempts to return the value of `var` from the provided `entry`
    dictionary. If the key does not exist, it will try to access it as an attribute
    of the entry object. If neither is found, the function returns the specified
    `default` value.

    Args:
        entry: A dictionary representing the contents of a YAML file.
        var: The key or attribute name to retrieve from the entry.
        default: The default value to return if the key or attribute is not found.

    Returns:
        The value associated with `var` in the entry, or `default` if not found.
    """
    try:
        return entry[var]
    except (TypeError, KeyError):
        try:
            return getattr(entry, var)
        except AttributeError:
            return default


def load_array_file(filename: str, ndmin: int = 2) -> np.ndarray:
    """
    Load an array from a file based on its extension.

    This function reads an array stored in the specified `filename`.
    It supports three file types based on their extensions:

    - `.npy` for NumPy binary files
    - `.csv` for comma-separated values
    - `.tab` for whitespace (or tab) separated values

    The function ensures that the loaded array has at least `ndmin` dimensions.
    If the array is in binary format, it checks the dimensions without altering the data.

    Args:
        filename: The path to the file to load.
        ndmin: The minimum number of dimensions the array should have.

    Returns:
        The loaded array.

    Raises:
        TypeError: If the file extension is not one of the supported types
            (`.npy`, `.csv`, `.tab`).
    """
    protocol = determine_protocol(filename)

    # Don't change binary-stored numpy arrays; just check dimensions
    if protocol == "npy":
        array = np.load(filename, allow_pickle=True)
        if array.ndim < ndmin:
            LOG.error(
                f"Array in {filename} has fewer than the required \
                       minimum dimensions ({array.ndim} < {ndmin})!"
            )
    # Make sure text files load as strings with minimum number of dimensions
    elif protocol == "csv":
        array = np.loadtxt(filename, delimiter=",", ndmin=ndmin, dtype=str)
    elif protocol == "tab":
        array = np.loadtxt(filename, ndmin=ndmin, dtype=str)
    else:
        raise TypeError(
            f"{protocol} is not a valid array file extension.\
                         Choices: {ARRAY_FILE_FORMATS}"
        )

    return array


def determine_protocol(fname: str) -> str:
    """
    Determine the file protocol based on the file name extension.

    Args:
        fname: The name of the file whose protocol is to be determined.

    Returns:
        The protocol corresponding to the file extension (e.g., 'hdf5').

    Raises:
        ValueError: If the provided file name does not have a valid extension.
    """
    _, ext = os.path.splitext(fname)
    if ext.startswith("."):
        protocol = ext.lower().strip(".")
    else:
        raise ValueError(f"{fname} needs an ext (eg .hdf5) to determine protocol!")
    # Map .h5 to .hdf5
    if protocol == "h5":
        protocol = "hdf5"
    return protocol


def verify_filepath(filepath: str) -> str:
    """
    Verify that the given file path is valid and return its absolute form.

    This function checks if the specified `filepath` points to an existing file.
    It expands any user directory shortcuts (e.g., `~`) and environment variables
    in the provided path before verifying its existence. If the file does not exist,
    a ValueError is raised.

    Args:
        filepath: The path of the file to verify.

    Returns:
        The verified absolute file path with expanded environment variables.

    Raises:
        ValueError: If the provided file path does not point to a valid file.
    """
    filepath = os.path.abspath(os.path.expandvars(os.path.expanduser(filepath)))
    if not os.path.isfile(filepath):
        raise ValueError(f"'{filepath}' is not a valid filepath")
    return filepath


def verify_dirpath(dirpath: str) -> str:
    """
    Verify that the given directory path is valid and return its absolute form.

    This function checks if the specified `dirpath` points to an existing directory.
    It expands any user directory shortcuts (e.g., `~`) and environment variables
    in the provided path before verifying its existence. If the directory does not exist,
    a ValueError is raised.

    Args:
        dirpath: The path of the directory to verify.

    Returns:
        The verified absolute directory path with expanded environment variables.

    Raises:
        ValueError: If the provided directory path does not point to a valid directory.
    """
    dirpath: str = os.path.abspath(os.path.expandvars(os.path.expanduser(dirpath)))
    if not os.path.isdir(dirpath):
        raise ValueError(f"'{dirpath}' is not a valid directory path")
    return dirpath


@contextmanager
def cd(path: str) -> Generator[None, None, None]:  # pylint: disable=C0103
    """
    Context manager for changing the current working directory.

    This context manager changes the current working directory to the specified `path`
    while executing the block of code within the context. Once the block is exited,
    it restores the original working directory.

    Args:
        path: The path to the directory to change to.

    Yields:
        Control is yielded back to the block of code within the context.
    """
    old_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_dir)


def pickle_data(filepath: str, content: Any):
    """
    Dump content to a pickle file.

    This function serializes the given `content` and writes it to a specified file
    in pickle format. The file is opened in write mode, which will overwrite any
    existing content in the file.

    Args:
        filepath: The path to the file where the content will be saved.
        content: The data to be serialized and saved to the pickle file.
    """
    with open(filepath, "w") as f:  # pylint: disable=C0103
        pickle.dump(content, f)


def get_source_root(filepath: str) -> str:
    """
    Find the absolute project path given a file path from within the project.

    This function determines the root directory of a project by analyzing the given
    file path. It works by traversing the directory structure upwards until it
    encounters a directory name that is not an integer, which is assumed to be the
    project root.

    Args:
        filepath: The file path from within the project for which to find the root.

    Returns:
        The absolute path to the root directory of the project. Returns None if
            the path corresponds to the root directory itself.
    """
    filepath = os.path.abspath(filepath)
    sep = os.path.sep
    if filepath == sep:
        return None

    parent = os.path.dirname(filepath)
    # Walk backwards testing for integers.
    break_point = parent.split(sep)[-1]  # Initial value for lgtm.com
    for _, _dir in enumerate(parent.split(sep)[::-1]):
        try:
            int(_dir)
        except ValueError:
            break_point = _dir
            break

    root, _ = parent.split(break_point)
    root = os.path.split(root)[0]
    return root


def ensure_directory_exists(**kwargs: Dict[Any, Any]) -> bool:
    """
    Ensure that the directory for the specified aggregate file exists.

    This function checks if the directory for the given `aggregate_file` exists.
    If it does not exist, the function creates the necessary directories.

    Args:
        **kwargs: Keyword arguments that must include:\n
            - `aggregate_file` (str): The file path for which the directory needs to be ensured.

    Returns:
        True if the directory already existed. False otherwise.
    """
    aggregate_bundle = kwargs["aggregate_file"]
    dirname = os.path.dirname(aggregate_bundle)

    if not os.path.exists(dirname):
        LOG.info(f"making directories to {dirname}.")
        os.makedirs(dirname)
        return False
    return True


def nested_dict_to_namespaces(dic: Dict) -> SimpleNamespace:
    """
    Convert a nested dictionary into a nested SimpleNamespace structure.

    This function recursively transforms a dictionary (which may contain other
    dictionaries) into a structure of SimpleNamespace objects. Each key in the
    dictionary becomes an attribute of a SimpleNamespace, allowing for attribute-style
    access to the data.

    Args:
        dic: The nested dictionary to be converted.

    Returns:
        A SimpleNamespace object representing the nested structure of the input dictionary.

    Raises:
        TypeError: If the input is not a dictionary.
    """

    def recurse(dic):
        if not isinstance(dic, dict):
            return dic
        for key, val in list(dic.items()):
            dic[key] = recurse(val)
        return SimpleNamespace(**dic)

    if not isinstance(dic, dict):
        raise TypeError(f"{dic} is not a dict")

    new_dic = deepcopy(dic)
    return recurse(new_dic)


def nested_namespace_to_dicts(namespaces: SimpleNamespace) -> Dict:
    """
    Convert a nested SimpleNamespace structure into a nested dictionary.

    This function recursively transforms a SimpleNamespace (which may contain
    other SimpleNamespaces) into a dictionary structure. Each attribute of the
    SimpleNamespace becomes a key in the resulting dictionary.

    Args:
        namespaces: The nested SimpleNamespace to be converted.

    Returns:
        A dictionary representing the nested structure of the input
            SimpleNamespace.

    Raises:
        TypeError: If the input is not a SimpleNamespace.
    """

    def recurse(namespaces):
        if not isinstance(namespaces, SimpleNamespace):
            return namespaces
        for key, val in list(namespaces.__dict__.items()):
            setattr(namespaces, key, recurse(val))
        return namespaces.__dict__

    if not isinstance(namespaces, SimpleNamespace):
        raise TypeError(f"{namespaces} is not a SimpleNamespace")

    new_ns = deepcopy(namespaces)
    return recurse(new_ns)


def get_flux_version(flux_path: str, no_errors: bool = False) -> str:
    """
    Retrieve the version of Flux as a string.

    This function executes the Flux binary located at `flux_path` with the
    "version" command and parses the output to return the version number.
    If the command fails or the Flux binary cannot be found, it can either
    raise an error or return a default version based on the `no_errors` flag.

    Args:
        flux_path: The full path to the Flux binary.
        no_errors: A flag to suppress error messages and exceptions. If set to
            True, errors will be logged but not raised.

    Returns:
        The version of Flux as a string.

    Raises:
        FileNotFoundError: If the Flux binary cannot be found and `no_errors`
            is set to False.
        ValueError: If the version cannot be determined from the output and
            `no_errors` is set to False.
    """
    cmd = [flux_path, "version"]

    process = None

    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, encoding="utf8").communicate()  # pylint: disable=R1732
    except FileNotFoundError as e:  # pylint: disable=C0103
        if not no_errors:
            LOG.error(f"The flux path {flux_path} canot be found")
            LOG.error("Suppress this error with no_errors=True")
            raise e

    try:
        flux_ver = re.search(r"\s*([\d.]+)", process[0]).group(1)
    except (ValueError, TypeError) as e:  # pylint: disable=C0103
        if not no_errors:
            LOG.error("The flux version cannot be determined")
            LOG.error("Suppress this error with no_errors=True")
            raise e
        flux_ver = DEFAULT_FLUX_VERSION
        LOG.warning(f"Using syntax for default version: {flux_ver}")

    return flux_ver


def get_flux_cmd(flux_path: str, no_errors: bool = False) -> str:
    """
    Generate the Flux run command based on the installed version.

    This function determines the appropriate Flux command to use for
    running jobs, depending on the version of Flux installed at the
    specified `flux_path`. It defaults to "flux run" for versions
    greater than or equal to 0.48.x. For older versions, it adjusts
    the command accordingly.

    Args:
        flux_path: The full path to the Flux binary.
        no_errors: A flag to suppress error messages and exceptions
            if set to True.

    Returns:
        The appropriate Flux run command as a string.
    """
    # The default is for flux version >= 0.48.x
    # this may change in the future.
    flux_cmd = "flux run"

    flux_ver = get_flux_version(flux_path, no_errors=no_errors)

    vers = [int(n) for n in flux_ver.split(".")]
    if vers[0] == 0 and vers[1] < 48:
        flux_cmd = "flux mini run"

    if vers[0] == 0 and vers[1] < 13:
        flux_cmd = "flux wreckrun"

    return flux_cmd


def get_flux_alloc(flux_path: str, no_errors: bool = False) -> str:
    """
    Generate the `flux alloc` command based on the installed version.

    This function constructs the appropriate command for allocating
    resources with Flux, depending on the version of Flux installed
    at the specified `flux_path`. It defaults to "{flux_path} alloc"
    for versions greater than or equal to 0.48.x. For older versions,
    it adjusts the command accordingly.

    Args:
        flux_path: The full path to the Flux binary.
        no_errors: A flag to suppress error messages and exceptions
            if set to True.

    Returns:
        The appropriate Flux allocation command as a string.
    """
    # The default is for flux version >= 0.48.x
    # this may change in the future.
    flux_alloc = f"{flux_path} alloc"

    flux_ver = get_flux_version(flux_path, no_errors=no_errors)

    vers = [int(n) for n in flux_ver.split(".")]

    if vers[0] == 0 and vers[1] < 48:
        flux_alloc = f"{flux_path} mini alloc"

    return flux_alloc


def check_machines(machines: Union[str, List[str], Tuple[str]]) -> bool:
    """
    Check if the current machine is in the list of specified machines.

    This function determines whether the hostname of the current
    machine matches any entry in a provided list of machine names.
    It returns True if a match is found, otherwise it returns False.

    Args:
        machines: A single machine name or a list/tuple of machine
            names to compare with the current machine's hostname.

    Returns:
        True if the current machine's hostname matches any of the
            specified machines; False otherwise.
    """
    local_hostname = socket.gethostname()

    if not isinstance(machines, (list, tuple)):
        machines = [machines]

    for mach in machines:
        if mach in local_hostname:
            return True

    return False


def contains_token(string: str) -> bool:
    """
    Check if the given string contains a token of the form $(STR).

    This function uses a regular expression to search for tokens
    that match the pattern $(<word\\>), where <word\\> consists of
    alphanumeric characters and underscores. It returns True if
    such a token is found; otherwise, it returns False.

    Args:
        string: The input string to be checked for tokens.

    Returns:
        True if the input string contains a token of the form
            $(STR); False otherwise.
    """
    if re.search(r"\$\(\w+\)", string):
        return True
    return False


def contains_shell_ref(string: str) -> bool:
    """
    Check if the given string contains a shell variable reference.

    This function searches for shell variable references in the
    format of $<variable\\> or ${<variable\\>}, where <variable\\>
    consists of alphanumeric characters and underscores. It returns
    True if a match is found; otherwise, it returns False.

    Args:
        string: The input string to be checked for shell
            variable references.

    Returns:
        True if the input string contains a shell variable
            reference of the form $STR or ${STR}; False otherwise.
    """
    if re.search(r"\$\w+", string) or re.search(r"\$\{\w+\}", string):
        return True
    return False


def needs_merlin_expansion(cmd: str, restart_cmd: str, labels: List[str], include_sample_keywords: bool = True) -> bool:
    """
    Check if the provided command or restart command contains variables that require expansion.

    This function checks both the command (`cmd`) and the restart command (`restart_cmd`)
    for the presence of specified labels or sample keywords that indicate a need for variable
    expansion.

    Args:
        cmd: The command inside a study step to check for variable expansion.
        restart_cmd: The restart command inside a study step to check for variable expansion.
        labels: A list of labels to check for inside `cmd` and `restart_cmd`.
        include_sample_keywords: Flag to indicate whether to include default sample keywords
            in the label check.

    Returns:
        True if either `cmd` or `restart_cmd` contains any of the specified labels
            or default sample keywords, indicating a need for expansion. False otherwise.
    """
    sample_keywords = ["MERLIN_SAMPLE_ID", "MERLIN_SAMPLE_PATH", "merlin_sample_id", "merlin_sample_path"]
    if include_sample_keywords:
        labels += sample_keywords

    for label in labels:
        if f"$({label})" in cmd:
            return True
        # The restart may need expansion while the cmd does not.
        if restart_cmd and f"$({label})" in restart_cmd:
            return True

    # If we got through all the labels and no expansion was needed then these commands don't need expansion
    return False


def dict_deep_merge(dict_a: Dict, dict_b: Dict, path: str = None, conflict_handler: Callable = None):
    """
    Recursively merges `dict_b` into `dict_a`, performing a deep merge.

    This function combines two dictionaries by recursively merging
    the contents of `dict_b` into `dict_a`. Unlike Python's built-in
    dictionary merge, this function performs a deep merge, meaning
    it will merge nested dictionaries instead of just updating top-level keys.
    Existing keys in `dict_a` will not be updated unless a conflict handler
    is provided to resolve key conflicts.

    Credit to [this stack overflow post](https://stackoverflow.com/a/7205107).

    Args:
        dict_a: The dictionary that will be merged into.
        dict_b: The dictionary to merge into `dict_a`.
        path: The current path in the dictionary tree. This is used for logging
            purposes during recursion.
        conflict_handler: A function to handle conflicts when both dictionaries
            have the same key with different values. The function should return
            the value to be used in the merged dictionary. If not provided, a
            warning will be logged for conflicts.
    """

    # Check to make sure we have valid dict_a and dict_b input
    msgs = [
        f"{name} '{actual_dict}' is not a dict"
        for name, actual_dict in [("dict_a", dict_a), ("dict_b", dict_b)]
        if not isinstance(actual_dict, dict)
    ]
    if len(msgs) > 0:
        LOG.warning(f"Problem with dict_deep_merge: {', '.join(msgs)}. Ignoring this merge call.")
        return

    if path is None:
        path = []
    for key in dict_b:
        if key in dict_a:
            if isinstance(dict_a[key], dict) and isinstance(dict_b[key], dict):
                dict_deep_merge(dict_a[key], dict_b[key], path=path + [str(key)], conflict_handler=conflict_handler)
            elif isinstance(dict_a[key], list) and isinstance(dict_a[key], list):
                dict_a[key] += dict_b[key]
            elif dict_a[key] == dict_b[key]:
                pass  # same leaf value
            else:
                if conflict_handler is not None:
                    merged_val = conflict_handler(
                        dict_a_val=dict_a[key], dict_b_val=dict_b[key], key=key, path=path + [str(key)]
                    )
                    dict_a[key] = merged_val
                else:
                    # Want to just output a warning instead of raising an exception so that the workflow doesn't crash
                    LOG.warning(f"Conflict at {'.'.join(path + [str(key)])}. Ignoring the update to key '{key}'.")
        else:
            dict_a[key] = dict_b[key]


def find_vlaunch_var(vlaunch_var: str, step_cmd: str, accept_no_matches: bool = False) -> str:
    """
    Find and return the specified VLAUNCHER variable from the step command.

    This function searches for a variable defined in the VLAUNCHER context
    within the provided step command string. It looks for the variable in
    the format `MERLIN_<vlaunch_var>=<value>`. If the variable is found,
    it returns the variable in a format suitable for use in a command string.
    If the variable is not found, the behavior depends on the `accept_no_matches` flag.

    Args:
        vlaunch_var: The name of the VLAUNCHER variable (without the prefix 'MERLIN_').
        step_cmd: The command string of a step where the variable may be defined.
        accept_no_matches: If True, returns None if the variable is not found.
            If False, raises a ValueError. Defaults to False.

    Returns:
        The variable in the format '${MERLIN_<vlaunch_var\\>}' if found, otherwise None
            (if `accept_no_matches` is True) or raises a ValueError (if False).

    Raises:
        ValueError: If the variable is not found and `accept_no_matches` is False.
    """
    matches = list(re.findall(rf"^(?!#).*MERLIN_{vlaunch_var}=\d+", step_cmd, re.MULTILINE))

    if matches:
        return f"${{MERLIN_{vlaunch_var}}}"

    if accept_no_matches:
        return None
    raise ValueError(f"VLAUNCHER used but could not find MERLIN_{vlaunch_var} in the step.")


# Time utilities
def convert_to_timedelta(timestr: Union[str, int]) -> timedelta:
    """
    Convert a time string or integer to a timedelta object.

    The function takes a time string formatted as
    '[days]:[hours]:[minutes]:seconds', where days, hours, and minutes
    are optional. If an integer is provided, it is interpreted as the
    total number of seconds.

    Args:
        timestr: The time string in the specified format or an integer
            representing seconds.

    Returns:
        A timedelta object representing the duration specified by the input
            string or integer.

    Raises:
        ValueError: If the input string does not conform to the expected
            format or contains more than four time fields.
    """
    # make sure it's a string in case we get an int
    timestr = str(timestr)

    # remove time unit characters (if any exist)
    time_unit_chars = r"[dhms]"
    timestr = re.sub(time_unit_chars, "", timestr)

    nfields = len(timestr.split(":"))
    if nfields > 4:
        raise ValueError(f"Cannot convert {timestr} to a timedelta. Valid format: days:hours:minutes:seconds.")
    _, d, h, m, s = (":0" * 10 + timestr).rsplit(":", 4)  # pylint: disable=C0103
    tdelta = timedelta(days=int(d), hours=int(h), minutes=int(m), seconds=int(s))
    return tdelta


def _repr_timedelta_HMS(time_delta: timedelta) -> str:  # pylint: disable=C0103
    """
    Represent a timedelta object as a string in 'HH:MM:SS' format.

    This function converts a given timedelta object into a string that
    represents the duration in hours, minutes, and seconds. The output
    is formatted as 'HH:MM:SS', with leading zeros for single-digit
    hours, minutes, or seconds.

    Args:
        time_delta: The timedelta object to be converted.

    Returns:
        A string representation of the timedelta in the format 'HH:MM:SS'.
    """
    hours, remainder = divmod(time_delta.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    hours, minutes, seconds = int(hours), int(minutes), int(seconds)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _repr_timedelta_FSD(time_delta: timedelta) -> str:  # pylint: disable=C0103
    """
    Represent a timedelta as a Flux Standard Duration (FSD) string in seconds.

    The FSD format represents a duration as a floating-point number followed
    by a suffix indicating the time unit. This function simplifies the
    representation by using seconds and appending an 's' suffix.

    Args:
        time_delta: The timedelta object to be converted.

    Returns:
        A string representation of the timedelta in FSD format, expressed
            in seconds (e.g., '123.45s').
    """
    fsd = f"{time_delta.total_seconds()}s"
    return fsd


def repr_timedelta(time_delta: timedelta, method: str = "HMS") -> str:
    """
    Represent a timedelta object as a string using a specified format method.

    This function formats a given timedelta object according to the chosen
    method. The available methods are:

    - HMS: Represents the duration in 'hours:minutes:seconds' format.
    - FSD: Represents the duration in Flux Standard Duration (FSD),
      expressed as a floating-point number of seconds with an 's' suffix.

    Args:
        time_delta: The timedelta object to be formatted.
        method: The method to use for formatting. Must be either 'HMS' or 'FSD'.

    Returns:
        A string representation of the timedelta formatted according
            to the specified method.

    Raises:
        ValueError: If an invalid method is provided.
    """
    if method == "HMS":
        return _repr_timedelta_HMS(time_delta)
    if method == "FSD":
        return _repr_timedelta_FSD(time_delta)
    raise ValueError("Invalid method for formatting timedelta! Valid choices: HMS, FSD")


def convert_timestring(timestring: Union[str, int], format_method: str = "HMS") -> str:
    """
    Converts a timestring to a specified format.

    This function accepts a timestring in a specific format or an integer
    representing seconds, and converts it to a formatted string based on
    the chosen format method. The available format methods are:

    - HMS: Represents the duration in 'hours:minutes:seconds' format.
    - FSD: Represents the duration in Flux Standard Duration (FSD),
      expressed as a floating-point number of seconds with an 's' suffix.

    Args:
        timestring: A string representing time in the format
            '[days]:[hours]:[minutes]:seconds' (where days, hours, and
            minutes are optional), or an integer representing time in seconds.
        format_method: The method to use for formatting. Must be either
            'HMS' or 'FSD'.

    Returns:
        A string representation of the converted timestring formatted
            according to the specified method.
    """
    LOG.debug(f"Timestring is: {timestring}")
    tdelta = convert_to_timedelta(timestring)
    LOG.debug(f"Timedelta object is: {tdelta}")
    return repr_timedelta(tdelta, method=format_method)


def pretty_format_hms(timestring: str) -> str:
    """
    Format an HMS timestring to remove blank entries and add appropriate labels.

    This function takes a timestring in the 'HH:MM:SS' format and formats
    it by removing any components that are zero and appending the relevant
    labels (days, hours, minutes, seconds). The output is a cleaner string
    representation of the time.

    Args:
        timestring: A timestring formatted as 'DD:HH:MM:SS'. Each component
            represents days, hours, minutes, and seconds, respectively.
            Only the last four components are relevant and may include
            leading zeros.

    Returns:
        A formatted timestring with non-zero components labeled appropriately.

    Raises:
        ValueError: If the input timestring contains more than four components
            or is not in the expected format.

    Examples:
        ```python
        >>> pretty_format_hms("00:00:34:00")
        '34m'
        >>> pretty_format_hms("01:00:00:25")
        '01d:25s'
        >>> pretty_format_hms("00:19:44:28")
        '19h:44m:28s'
        >>> pretty_format_hms("00:00:00:00")
        '00s'
        ```
    """
    # Create labels and split the timestring
    labels = ["d", "h", "m", "s"]
    parsed_ts = timestring.split(":")
    if len(parsed_ts) > 4:
        raise ValueError("The timestring to label must be in the format DD:HH:MM:SS")

    # Label each integer with its corresponding unit
    labeled_time_list = []
    for i in range(1, len(parsed_ts) + 1):
        if parsed_ts[-i] != "00":
            labeled_time_list.append(parsed_ts[-i] + labels[-i])

    # Join the labeled time list into a string.
    if len(labeled_time_list) == 0:
        labeled_time_list.append("00s")
    labeled_time_list.reverse()
    labeled_time_string = ":".join(labeled_time_list)

    return labeled_time_string


def ws_time_to_dt(ws_time: str) -> datetime:
    """
    Convert a workspace timestring to a datetime object.

    This function takes a workspace timestring formatted as 'YYYYMMDD-HHMMSS'
    and converts it into a corresponding datetime object. The input string
    must adhere to the specified format to ensure accurate conversion.

    Args:
        ws_time: A workspace timestring in the format 'YYYYMMDD-HHMMSS', where:\n
            - YYYY is the four-digit year,
            - MM is the two-digit month (01 to 12),
            - DD is the two-digit day (01 to 31),
            - HH is the two-digit hour (00 to 23),
            - MM is the two-digit minute (00 to 59),
            - SS is the two-digit second (00 to 59).

    Returns:
        A datetime object constructed from the provided workspace timestring.
    """
    year = int(ws_time[:4])
    month = int(ws_time[4:6])
    day = int(ws_time[6:8])
    hour = int(ws_time[9:11])
    minute = int(ws_time[11:13])
    second = int(ws_time[13:])
    return datetime(year, month, day, hour=hour, minute=minute, second=second)


def get_package_versions(package_list: List[str]) -> str:
    """
    Generate a formatted table of installed package versions and their locations.

    This function takes a list of package names and checks for their installed
    versions and locations. If a package is not installed, it indicates that
    the package is "Not installed". The output includes the Python version
    and its executable location at the top of the table.

    Args:
        package_list: A list of package names to check for installed versions.

    Returns:
        A formatted string representing a table of package names, their versions,
            and installation locations.
    """
    table = []
    for package in package_list:
        try:
            distribution = pkg_resources.get_distribution(package)
            version = distribution.version
            location = distribution.location
            table.append([package, version, location])
        except pkg_resources.DistributionNotFound:
            table.append([package, "Not installed", "N/A"])

    table.insert(0, ["python", sys.version.split()[0], sys.executable])
    table_str = tabulate(table, headers=["Package", "Version", "Location"], tablefmt="simple")
    return f"Python Packages\n\n{table_str}\n"
