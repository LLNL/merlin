###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.3.
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


class ConnProcess(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = Pipe()
        self._exception = None

    def run(self):
        try:
            Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


def check_server_access(sconf):
    servers = ["broker server", "results server"]

    if sconf.keys():
        print("\nChecking server connections:")
        print("-" * 28)

    excpts = {}
    connect_timeout = 60
    for s in servers:
        if s in sconf:
            try:
                conn = Connection(sconf[s])
                conn_check = ConnProcess(target=conn.connect)
                conn_check.start()
                counter = 0
                while conn_check.is_alive():
                    time.sleep(1)
                    counter += 1
                    if counter > connect_timeout:
                        conn_check.kill()
                        raise Exception(
                            f"Connection was killed due to timeout ({connect_timeout}s)"
                        )
                conn.release()
                if conn_check.exception:
                    error, traceback = conn_check.exception
                    raise error
            except Exception as e:
                print(f"{s} connection: Error")
                excpts[s] = e
            else:
                print(f"{s} connection: OK")

    if excpts:
        print("\nExceptions:")
        for k, v in excpts.items():
            print(f"{k}: {v}")


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
    except Exception as e:
        conf["broker server"] = "Broker server error."
        excpts["broker server"] = e

    try:
        conf["results server"] = results_backend.get_connection_string(
            include_password=False
        )
        sconf["results server"] = results_backend.get_connection_string()
        conf["results ssl"] = results_backend.get_ssl_config()
    except Exception as e:
        conf["results server"] = "No results server configured or error."
        excpts["results server"] = e

    print(tabulate(conf.items(), tablefmt="presto"))

    if excpts:
        print("\nExceptions:")
        for k, v in excpts.items():
            print(f"{k}: {v}")

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


def print_info(args):
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
    for x in info_calls:
        info_str += 'echo " $ ' + x + '" && ' + x + "\n"
        info_str += "echo \n"
    info_str += r"echo \"echo \$PYTHONPATH\" && echo $PYTHONPATH"
    subprocess.call(info_str, shell=True)
    print("")
