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
Manages formatting for displaying information to the console.
"""
import pprint
import subprocess
import time
import traceback
from multiprocessing import Pipe, Process

from kombu import Connection
from tabulate import tabulate

from merlin.ascii_art import banner_small
from merlin.config import broker, results_backend
from merlin.config.configfile import default_config_info


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
