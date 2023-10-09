###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace
from typing import Union

import numpy as np
import psutil
import yaml


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


def apply_list_of_regex(regex_list, list_to_filter, result_list, match=False):
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
