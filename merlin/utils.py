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
Module for project-wide utility functions.
"""
import getpass
import logging
import os
import re
import socket
import subprocess
import sys
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Callable, List, Optional, Union

import numpy as np
import pkg_resources
import psutil
import yaml
from tabulate import tabulate


try:
    import cPickle as pickle
except ImportError:
    import pickle


LOG = logging.getLogger(__name__)
ARRAY_FILE_FORMATS = ".npy, .csv, .tab"
DEFAULT_FLUX_VERSION = "0.48.0"


def get_user_process_info(user=None, attrs=None):
    """
    Return a list of process info for all of the user's running processes.

    :param `user`: user name (default from getpass). Option: 'all_users': get
        all processes
    :param `atts`: the attributes to include
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


def check_pid(pid, user=None):
    """
    Check if pid is in process list.

    :param `pid`: process id
    :param `user`: user name (default from getpass). Option: 'all_users': get
        all processes
    """
    user_processes = get_user_process_info(user=user)
    for process in user_processes:
        if int(process["pid"]) == pid:
            return True
    return False


def get_pid(name, user=None):
    """
    Return pid of process with name.

    :param `name`: process name
    :param `user`: user name (default from getpass). Option: 'all_users': get
        all processes
    """
    user_processes = get_user_process_info(user=user)
    name_list = [p["pid"] for p in user_processes if name in p["name"]]
    if name_list:
        return name_list
    return None


def get_procs(name, user=None):
    """
    Return a list of (pid, cmdline) tuples of process with name.

    :param `name`: process name
    :param `user`: user name (default from getpass). Option: 'all_users': get
        all processes
    """
    user_processes = get_user_process_info(user=user)
    procs = [(p["pid"], p["cmdline"]) for p in user_processes if name in p["name"]]
    return procs


def is_running_psutil(cmd, user=None):
    """
    Determine if process with given command is running.
    Uses psutil command instead of call to 'ps'

    :param `cmd`: process cmd
    :param `user`: user name (default from getpass). Option: 'all_users': get
        all processes
    """
    user_processes = get_user_process_info(user=user)
    return any(cmd in " ".join(p["cmdline"]) for p in user_processes)


def is_running(name, all_users=False):
    """
    Determine if process with name is running.

    :param `name`: process name
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


def expandvars2(path):
    """
    Replace shell strings from the current environment variables

    :param `path`: a path
    """
    return re.sub(r"(?<!\\)\$[A-Za-z_][A-Za-z0-9_]*", "", os.path.expandvars(path))


def regex_list_filter(regex, list_to_filter, match=True):
    """
    Apply a regex filter to a list

    :param `regex`          : the regular expression
    :param `list_to_filter` : the list to filter

    :return `new_list`
    """
    r = re.compile(regex)  # pylint: disable=C0103
    if match:
        return list(filter(r.match, list_to_filter))
    return list(filter(r.search, list_to_filter))


def apply_list_of_regex(regex_list, list_to_filter, result_list, match=False, display_warning: bool = True):
    """
    Take a list of regex's, apply each regex to a list we're searching through,
    and append each result to a result list.

    :param `regex_list`: A list of regular expressions to apply to the list_to_filter
    :param `list_to_filter`: A list that we'll apply regexs to
    :param `result_list`: A list that we'll append results of the regex filters to
    :param `match`: A bool where when true we use re.match for applying the regex,
                    when false we use re.search for applying the regex.
    """
    for regex in regex_list:
        filter_results = set(regex_list_filter(regex, list_to_filter, match))

        if not filter_results:
            if display_warning:
                LOG.warning(f"No regex match for {regex}.")
        else:
            result_list += filter_results


def load_yaml(filepath):
    """
    Safely read a yaml file.

    :param `filepath`: a filepath to a yaml file
    :type filepath: str

    :returns: Python objects holding the contents of the yaml file
    """
    with open(filepath, "r") as _file:
        return yaml.safe_load(_file)


def get_yaml_var(entry, var, default):
    """
    Return entry[var], else return default

    :param `entry`: a yaml dict
    :param `var`: a yaml key
    :param `default`: default value in the absence of data
    """

    try:
        return entry[var]
    except (TypeError, KeyError):
        try:
            return getattr(entry, var)
        except AttributeError:
            return default


def load_array_file(filename, ndmin=2):
    """
    Loads up an array stored in filename, based on extension.

    Valid filename extensions:
        '.npy'  :  numpy binary file
        '.csv'  :  comma separated text file
        '.tab'  :  whitespace (or tab) separated text file

    :param `filename` : The file to load
    :param `ndmin`    : The minimum number of dimensions to load
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


def determine_protocol(fname):
    """
    Determines a file protocol based on file name extension.
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
    Verify that the filepath argument is a valid
    file.

    :param [str] `filepath`: the path of a file

    :return: the verified absolute filepath with expanded environment variables.
    :rtype: str
    """
    filepath = os.path.abspath(os.path.expandvars(os.path.expanduser(filepath)))
    if not os.path.isfile(filepath):
        raise ValueError(f"'{filepath}' is not a valid filepath")
    return filepath


def verify_dirpath(dirpath: str) -> str:
    """
    Verify that the dirpath argument is a valid
    directory.

    :param [str] `dirpath`: the path of a directory

    :return: returns the absolute path with expanded environment vars for a given dirpath.
    :rtype: str
    """
    dirpath: str = os.path.abspath(os.path.expandvars(os.path.expanduser(dirpath)))
    if not os.path.isdir(dirpath):
        raise ValueError(f"'{dirpath}' is not a valid directory path")
    return dirpath


@contextmanager
def cd(path):  # pylint: disable=C0103
    """
    TODO
    """
    old_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_dir)


def pickle_data(filepath, content):
    """Dump content to a pickle file"""
    with open(filepath, "w") as f:  # pylint: disable=C0103
        pickle.dump(content, f)


def get_source_root(filepath):
    """Used to find the absolute project path given a sample file path from
    within the project.
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


def ensure_directory_exists(**kwargs):
    """
    TODO
    """
    aggregate_bundle = kwargs["aggregate_file"]
    dirname = os.path.dirname(aggregate_bundle)

    if not os.path.exists(dirname):
        LOG.info(f"making directories to {dirname}.")
        os.makedirs(dirname)
        return False
    return True


def nested_dict_to_namespaces(dic):
    """Code for recursively converting dictionaries of dictionaries
    into SimpleNamespaces instead.
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


def nested_namespace_to_dicts(namespaces):
    """Code for recursively converting namespaces of namespaces
    into dictionaries instead.
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


def get_flux_version(flux_path, no_errors=False):
    """
    Return the flux version as a string

    :param `flux_path`: the full path to the flux bin
    :param `no_errors`: a flag to determine if this a test run to ignore errors
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


def get_flux_cmd(flux_path, no_errors=False):
    """
    Return the flux run command as string

    :param `flux_path`: the full path to the flux bin
    :param `no_errors`: a flag to determine if this a test run to ignore errors
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


def get_flux_alloc(flux_path, no_errors=False):
    """
    Return the flux alloc command as string

    :param `flux_path`: the full path to the flux bin
    :param `no_errors`: a flag to determine if this a test run to ignore errors
    """
    # The default is for flux version >= 0.48.x
    # this may change in the future.
    flux_alloc = f"{flux_path} alloc"

    flux_ver = get_flux_version(flux_path, no_errors=no_errors)

    vers = [int(n) for n in flux_ver.split(".")]

    if vers[0] == 0 and vers[1] < 48:
        flux_alloc = f"{flux_path} mini alloc"

    return flux_alloc


def check_machines(machines):
    """
    Return a True if the current machine is in the list of machines.

    :param `machines`: A single machine or list of machines to compare
                       with the current machine.
    """
    local_hostname = socket.gethostname()

    if not isinstance(machines, (list, tuple)):
        machines = [machines]

    for mach in machines:
        if mach in local_hostname:
            return True

    return False


def contains_token(string):
    """
    Return True if given string contains a token of the form $(STR).
    """
    if re.search(r"\$\(\w+\)", string):
        return True
    return False


def contains_shell_ref(string):
    """
    Return True if given string contains a shell variable reference
    of the form $STR or ${STR}.
    """
    if re.search(r"\$\w+", string) or re.search(r"\$\{\w+\}", string):
        return True
    return False


def needs_merlin_expansion(
    cmd: str, restart_cmd: str, labels: List[str], include_sample_keywords: Optional[bool] = True
) -> bool:
    """
    Check if the cmd or restart cmd provided have variables that need expansion.

    :param `cmd`: The command inside a study step to check for expansion
    :param `restart_cmd`: The restart command inside a study step to check for expansion
    :param `labels`: A list of labels to check for inside `cmd` and `restart_cmd`
    :return : True if the cmd has any of the default keywords or spec
        specified sample column labels. False otherwise.
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


def dict_deep_merge(dict_a: dict, dict_b: dict, path: str = None, conflict_handler: Callable = None):
    """
    This function recursively merges dict_b into dict_a. The built-in
    merge of dictionaries in python (dict(dict_a) | dict(dict_b)) does not do a
    deep merge so this function is necessary. This will only merge in new keys,
    it will NOT update existing ones, unless you specify a conflict handler function.
    Credit to this stack overflow post: https://stackoverflow.com/a/7205107.

    :param `dict_a`: A dict that we'll merge dict_b into
    :param `dict_b`: A dict that we want to merge into dict_a
    :param `path`: The path down the dictionary tree that we're currently at
    :param `conflict_handler`: An optional function to handle conflicts between values at the same key.
                               The function should return the value to be used in the merged dictionary.
                               The default behavior without this argument is to log a warning.
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


def find_vlaunch_var(vlaunch_var: str, step_cmd: str, accept_no_matches=False) -> str:
    """
    Given a variable used for VLAUNCHER and the step cmd value, find
    the variable.

    :param `vlaunch_var`: The name of the VLAUNCHER variable (without MERLIN_)
    :param `step_cmd`: The string for the cmd of a step
    :param `accept_no_matches`: If True, return None if we couldn't find the variable. Otherwise, raise an error.
    :returns: the `vlaunch_var` variable or None
    """
    matches = list(re.findall(rf"^(?!#).*MERLIN_{vlaunch_var}=\d+", step_cmd, re.MULTILINE))

    if matches:
        return f"${{MERLIN_{vlaunch_var}}}"

    if accept_no_matches:
        return None
    raise ValueError(f"VLAUNCHER used but could not find MERLIN_{vlaunch_var} in the step.")


# Time utilities
def convert_to_timedelta(timestr: Union[str, int]) -> timedelta:
    """Convert a timestring to a timedelta object.
    Timestring is given in in the format '[days]:[hours]:[minutes]:seconds'
    with days, hours, minutes all optional add ons.
    If passed as an int, will convert to a string first and interpreted as seconds.
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
    """Represent a timedelta object as a string in hours:minutes:seconds"""
    hours, remainder = divmod(time_delta.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    hours, minutes, seconds = int(hours), int(minutes), int(seconds)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _repr_timedelta_FSD(time_delta: timedelta) -> str:  # pylint: disable=C0103
    """Represent a timedelta as a flux standard duration string, using seconds.

    flux standard duration (FSD) is a floating point number with a single character suffix: s,m,h or d.
    This uses seconds for simplicity.
    """
    fsd = f"{time_delta.total_seconds()}s"
    return fsd


def repr_timedelta(time_delta: timedelta, method: str = "HMS") -> str:
    """Represent a timedelta object as a string using a particular method.

    method - HMS: 'hours:minutes:seconds'
    method - FSD: flux standard duration: 'seconds.s'"""
    if method == "HMS":
        return _repr_timedelta_HMS(time_delta)
    if method == "FSD":
        return _repr_timedelta_FSD(time_delta)
    raise ValueError("Invalid method for formatting timedelta! Valid choices: HMS, FSD")


def convert_timestring(timestring: Union[str, int], format_method: str = "HMS") -> str:
    """Converts a timestring to a different format.

    timestring: -either-
                a timestring in in the format '[days]:[hours]:[minutes]:seconds'
                    days, hours, minutes are all optional add ons
                -or-
                an integer representing seconds
    format_method: HMS - 'hours:minutes:seconds'
                   FSD - 'seconds.s' (flux standard duration)

    """
    LOG.debug(f"Timestring is: {timestring}")
    tdelta = convert_to_timedelta(timestring)
    LOG.debug(f"Timedelta object is: {tdelta}")
    return repr_timedelta(tdelta, method=format_method)


def pretty_format_hms(timestring: str) -> str:
    """
    Given an HMS timestring, format it so it removes blank entries and adds
    labels.

    :param `timestring`: the HMS timestring we'll format
    :returns: a formatted timestring

    Examples:
        - "00:00:34:00" -> "34m"
        - "01:00:00:25" -> "01d:25s"
        - "00:19:44:28" -> "19h:44m:28s"
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
    Converts a workspace timestring to a datetime object.

    :param `ws_time`: A workspace timestring in the format YYYYMMDD-HHMMSS
    :returns: A datetime object created from the workspace timestring
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
    Return a table of the versions and locations of installed packages, including python.
    If the package is not installed says "Not installed"

    :param `package_list`: A list of packages.
    :returns: A string that's a formatted table.
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
