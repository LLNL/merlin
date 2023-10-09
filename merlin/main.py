"""The top level main function for invoking Merlin."""

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

from __future__ import print_function

import glob
import logging
import os
import sys
import time
import traceback
from argparse import (
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    Namespace,
    RawDescriptionHelpFormatter,
    RawTextHelpFormatter,
)
from contextlib import suppress
from typing import Dict, List, Optional, Union

from merlin import VERSION, router
from merlin.ascii_art import banner_small
from merlin.examples.generator import list_examples, setup_example
from merlin.log_formatter import setup_logging
from merlin.server.server_commands import config_server, init_server, restart_server, start_server, status_server, stop_server
from merlin.spec.expansion import RESERVED, get_spec_with_expansion
from merlin.spec.specification import MerlinSpec
from merlin.study.study import MerlinStudy
from merlin.utils import ARRAY_FILE_FORMATS


LOG = logging.getLogger("merlin")
DEFAULT_LOG_LEVEL = "INFO"


class HelpParser(ArgumentParser):
    """This class overrides the error message of the argument parser to
    print the help message when an error happens."""

    def error(self, message):
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


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


def parse_override_vars(
    variables_list: Optional[List[str]],
) -> Optional[Dict[str, Union[str, int]]]:
    """
    Parse a list of variables from command line syntax
    into a valid dictionary of variable keys and values.

    :param [List[str]] `variables_list`: an optional list of strings, e.g. ["KEY=val",...]

    :return: returns either None or a Dict keyed with strs, linked to strs and ints.
    :rtype: Dict
    """
    if variables_list is None:
        return None
    LOG.debug(f"Command line override variables = {variables_list}")
    result: Dict[str, Union[str, int]] = {}
    arg: str
    for arg in variables_list:
        try:
            if "=" not in arg:
                raise ValueError("--vars requires '=' operator. See 'merlin run --help' for an example.")
            entry: str = arg.split("=")
            if len(entry) != 2:
                raise ValueError("--vars requires ONE '=' operator (without spaces) per variable assignment.")
            key: str = entry[0]
            if key is None or key == "" or "$" in key:
                raise ValueError("--vars requires valid variable names comprised of alphanumeric characters and underscores.")
            if key in RESERVED:
                raise ValueError(f"Cannot override reserved word '{key}'! Reserved words are: {RESERVED}.")

            val: Union[str, int] = entry[1]
            with suppress(ValueError):
                int(val)
                val = int(val)
            result[key] = val

        except Exception as excpt:
            raise ValueError(
                f"{excpt} Bad '--vars' formatting on command line. See 'merlin run --help' for an example."
            ) from excpt
    return result


def get_merlin_spec_with_override(args):
    """
    Shared command to return the spec object.

    :param 'args': parsed CLI arguments
    """
    filepath = verify_filepath(args.specification)
    variables_dict = parse_override_vars(args.variables)
    spec = get_spec_with_expansion(filepath, override_vars=variables_dict)
    return spec, filepath


def process_run(args: Namespace) -> None:
    """
    CLI command for running a study.

    :param [Namespace] `args`: parsed CLI arguments
    """
    print(banner_small)
    filepath: str = verify_filepath(args.specification)
    variables_dict: str = parse_override_vars(args.variables)
    samples_file: Optional[str] = None
    if args.samples_file:
        samples_file = verify_filepath(args.samples_file)

    # pgen checks
    if args.pargs and not args.pgen_file:
        raise ValueError("Cannot use the 'pargs' parameter without specifying a 'pgen'!")
    if args.pgen_file:
        verify_filepath(args.pgen_file)

    study: MerlinStudy = MerlinStudy(
        filepath,
        override_vars=variables_dict,
        samples_file=samples_file,
        dry_run=args.dry,
        no_errors=args.no_errors,
        pgen_file=args.pgen_file,
        pargs=args.pargs,
    )
    router.run_task_server(study, args.run_mode)


def process_restart(args: Namespace) -> None:
    """
    CLI command for restarting a study.

    :param [Namespace] `args`: parsed CLI arguments
    """
    print(banner_small)
    restart_dir: str = verify_dirpath(args.restart_dir)
    filepath: str = os.path.join(args.restart_dir, "merlin_info", "*.expanded.yaml")
    possible_specs: Optional[List[str]] = glob.glob(filepath)
    if not possible_specs:  # len == 0
        raise ValueError(f"'{filepath}' does not match any provenance spec file to restart from.")
    if len(possible_specs) > 1:
        raise ValueError(f"'{filepath}' matches more than one provenance spec file to restart from.")
    filepath: str = verify_filepath(possible_specs[0])
    LOG.info(f"Restarting workflow at '{restart_dir}'")
    study: MerlinStudy = MerlinStudy(filepath, restart_dir=restart_dir)
    router.run_task_server(study, args.run_mode)


def launch_workers(args):
    """
    CLI command for launching workers.

    :param `args`: parsed CLI arguments
    """
    if not args.worker_echo_only:
        print(banner_small)
    spec, filepath = get_merlin_spec_with_override(args)
    if not args.worker_echo_only:
        LOG.info(f"Launching workers from '{filepath}'")
    status = router.launch_workers(spec, args.worker_steps, args.worker_args, args.disable_logs, args.worker_echo_only)
    if args.worker_echo_only:
        print(status)
    else:
        LOG.debug(f"celery command: {status}")


def purge_tasks(args):
    """
    CLI command for purging tasks.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    spec, _ = get_merlin_spec_with_override(args)
    ret = router.purge_tasks(
        spec.merlin["resources"]["task_server"],
        spec,
        args.purge_force,
        args.purge_steps,
    )

    LOG.info(f"Purge return = {ret} .")


def query_status(args):
    """
    CLI command for querying queue status.

    :param 'args': parsed CLI arguments
    """
    print(banner_small)
    spec, _ = get_merlin_spec_with_override(args)
    ret = router.query_status(args.task_server, spec, args.steps)
    for name, jobs, consumers in ret:
        print(f"{name:30} - Workers: {consumers:10} - Queued Tasks: {jobs:10}")
    if args.csv is not None:
        router.dump_status(ret, args.csv)


def query_workers(args):
    """
    CLI command for finding all workers.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)

    # Get the workers from the spec file if --spec provided
    worker_names = []
    if args.spec:
        spec_path = verify_filepath(args.spec)
        spec = MerlinSpec.load_specification(spec_path)
        worker_names = spec.get_worker_names()
        for worker_name in worker_names:
            if "$" in worker_name:
                LOG.warning(f"Worker '{worker_name}' is unexpanded. Target provenance spec instead?")
        LOG.debug(f"Searching for the following workers to stop based on the spec {args.spec}: {worker_names}")

    router.query_workers(args.task_server, worker_names, args.queues, args.workers)


def stop_workers(args):
    """
    CLI command for stopping all workers.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    worker_names = []

    # Load in the spec if one was provided via the CLI
    if args.spec:
        spec_path = verify_filepath(args.spec)
        spec = MerlinSpec.load_specification(spec_path)
        worker_names = spec.get_worker_names()
        for worker_name in worker_names:
            if "$" in worker_name:
                LOG.warning(f"Worker '{worker_name}' is unexpanded. Target provenance spec instead?")

    # Send stop command to router
    router.stop_workers(args.task_server, worker_names, args.queues, args.workers)


def print_info(args):
    """
    CLI command to print merlin config info.

    :param `args`: parsed CLI arguments
    """
    # if this is moved to the toplevel per standard style, merlin is unable to generate the (needed) default config file
    from merlin import display  # pylint: disable=import-outside-toplevel

    display.print_info(args)


def config_merlin(args: Namespace) -> None:
    """
    CLI command to setup default merlin config.

    :param [Namespace] `args`: parsed CLI arguments
    """
    output_dir: Optional[str] = args.output_dir
    if output_dir is None:
        user_home: str = os.path.expanduser("~")
        output_dir: str = os.path.join(user_home, ".merlin")

    router.create_config(args.task_server, output_dir, args.broker, args.test)


def process_example(args: Namespace) -> None:
    """Either lists all example workflows, or sets up an example as a workflow to be run at root dir.

    :param [Namespace] `args`: parsed CLI arguments
    """
    if args.workflow == "list":
        print(list_examples())
    else:
        print(banner_small)
        setup_example(args.workflow, args.path)


def process_monitor(args):
    """
    CLI command to monitor merlin workers and queues to keep
    the allocation alive

    :param `args`: parsed CLI arguments
    """
    LOG.info("Monitor: checking queues ...")
    spec, _ = get_merlin_spec_with_override(args)
    while router.check_merlin_status(args, spec):
        LOG.info("Monitor: found tasks in queues")
        time.sleep(args.sleep)
    LOG.info("Monitor: ... stop condition met")


def process_server(args: Namespace):
    """
    Route to the correct function based on the command
    given via the CLI
    """
    if args.commands == "init":
        init_server()
    elif args.commands == "start":
        start_server()
    elif args.commands == "stop":
        stop_server()
    elif args.commands == "status":
        status_server()
    elif args.commands == "restart":
        restart_server()
    elif args.commands == "config":
        config_server(args)


# Pylint complains that there's too many statements here and wants us
# to split the function up but that wouldn't make much sense so we ignore it
def setup_argparse() -> None:  # pylint: disable=R0915
    """
    Setup argparse and any CLI options we want available via the package.
    """
    parser: HelpParser = HelpParser(
        prog="merlin",
        description=banner_small,
        formatter_class=RawDescriptionHelpFormatter,
        epilog="See merlin <command> --help for more info",
    )
    parser.add_argument("-v", "--version", action="version", version=VERSION)
    subparsers: ArgumentParser = parser.add_subparsers(dest="subparsers")
    subparsers.required = True

    # merlin --level
    parser.add_argument(
        "-lvl",
        "--level",
        action="store",
        dest="level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        help="Set the log level. Options: DEBUG, INFO, WARNING, ERROR. [Default: %(default)s]",
    )

    # merlin run
    run: ArgumentParser = subparsers.add_parser(
        "run",
        help="Run a workflow using a Merlin or Maestro YAML study " "specification.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    run.set_defaults(func=process_run)
    run.add_argument("specification", type=str, help="Path to a Merlin or Maestro YAML file")
    run.add_argument(
        "--local",
        action="store_const",
        dest="run_mode",
        const="local",
        default="distributed",
        help="Run locally instead of distributed",
    )
    run.add_argument(
        "--vars",
        action="store",
        dest="variables",
        type=str,
        nargs="+",
        default=None,
        help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
        "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
    )
    # TODO add all supported formats to doc string  # pylint: disable=fixme
    run.add_argument(
        "--samplesfile",
        action="store",
        dest="samples_file",
        type=str,
        default=None,
        help=f"Specify file containing samples. Valid choices: {ARRAY_FILE_FORMATS}",
    )
    run.add_argument(
        "--dry",
        action="store_true",
        dest="dry",
        default=False,
        help="Flag to dry-run a workflow, which sets up the workspace but does not launch tasks.",
    )
    run.add_argument(
        "--no-errors",
        action="store_true",
        dest="no_errors",
        default=False,
        help="Flag to ignore some flux errors for testing (often used with --dry --local).",
    )
    run.add_argument(
        "--pgen",
        action="store",
        dest="pgen_file",
        type=str,
        default=None,
        help="Provide a pgen file to override global.parameters.",
    )
    run.add_argument(
        "--pargs",
        type=str,
        action="append",
        default=[],
        help="A string that represents a single argument to pass "
        "a custom parameter generation function. Reuse '--parg' "
        "to pass multiple arguments. [Use with '--pgen']",
    )

    # merlin restart
    restart: ArgumentParser = subparsers.add_parser(
        "restart",
        help="Restart a workflow using an existing Merlin workspace.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    restart.set_defaults(func=process_restart)
    restart.add_argument("restart_dir", type=str, help="Path to an existing Merlin workspace directory")
    restart.add_argument(
        "--local",
        action="store_const",
        dest="run_mode",
        const="local",
        default="distributed",
        help="Run locally instead of distributed",
    )

    # merlin purge
    purge: ArgumentParser = subparsers.add_parser(
        "purge",
        help="Remove all tasks from all merlin queues (default).              "
        "If a user would like to purge only selected queues use:    "
        "--steps to give a steplist, the queues will be defined from the step list",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    purge.set_defaults(func=purge_tasks)
    purge.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
    purge.add_argument(
        "-f",
        "--force",
        action="store_true",
        dest="purge_force",
        default=False,
        help="Purge the tasks without confirmation",
    )
    purge.add_argument(
        "--steps",
        nargs="+",
        type=str,
        dest="purge_steps",
        default=["all"],
        help="The specific steps in the YAML file from which you want to purge the queues. \
        The input is a space separated list.",
    )
    purge.add_argument(
        "--vars",
        action="store",
        dest="variables",
        type=str,
        nargs="+",
        default=None,
        help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
        "Example: '--vars MY_QUEUE=hello'",
    )

    mconfig: ArgumentParser = subparsers.add_parser(
        "config",
        help="Create a default merlin server config file in ~/.merlin",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    mconfig.set_defaults(func=config_merlin)
    mconfig.add_argument(
        "--task_server",
        type=str,
        default="celery",
        help="Task server type for which to create the config.\
                            Default: %(default)s",
    )
    mconfig.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default=None,
        help="Optional directory to place the default config file.\
                            Default: ~/.merlin",
    )
    mconfig.add_argument(
        "--broker",
        type=str,
        default=None,
        help="Optional broker type, backend will be redis\
                            Default: rabbitmq",
    )
    mconfig.add_argument(
        "--test",
        type=str,
        default=None,
        help="A config used in the testing suite (or for exemplative purposes).\
                            Default: rabbitmq",
    )

    # merlin example
    example: ArgumentParser = subparsers.add_parser(
        "example",
        help="Generate an example merlin workflow.",
        formatter_class=RawTextHelpFormatter,
    )
    example.add_argument(
        "workflow",
        action="store",
        type=str,
        help="The name of the example workflow to setup. Use 'merlin example list' to see available options.",
    )
    example.add_argument(
        "-p",
        "--path",
        action="store",
        type=str,
        default=None,
        help="Specify a path to write the workflow to. Defaults to current working directory",
    )
    example.set_defaults(func=process_example)

    generate_worker_touching_parsers(subparsers)

    generate_diagnostic_parsers(subparsers)

    # merlin server
    server: ArgumentParser = subparsers.add_parser(
        "server",
        help="Manage broker and results server for merlin workflow.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server.set_defaults(func=process_server)

    server_commands: ArgumentParser = server.add_subparsers(dest="commands")

    server_init: ArgumentParser = server_commands.add_parser(
        "init",
        help="Initialize merlin server resources.",
        description="Initialize merlin server",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server_init.set_defaults(func=process_server)

    server_status: ArgumentParser = server_commands.add_parser(
        "status",
        help="View status of the current server containers.",
        description="View status",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server_status.set_defaults(func=process_server)

    server_start: ArgumentParser = server_commands.add_parser(
        "start",
        help="Start a containerized server to be used as an broker and results server.",
        description="Start server",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server_start.set_defaults(func=process_server)

    server_stop: ArgumentParser = server_commands.add_parser(
        "stop",
        help="Stop an instance of redis containers currently running.",
        description="Stop server.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server_stop.set_defaults(func=process_server)

    server_stop: ArgumentParser = server_commands.add_parser(
        "restart",
        help="Restart merlin server instance",
        description="Restart server.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server_stop.set_defaults(func=process_server)

    server_config: ArgumentParser = server_commands.add_parser(
        "config",
        help="Making configurations for to the merlin server instance.",
        description="Config server.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    server_config.add_argument(
        "-ip",
        "--ipaddress",
        action="store",
        type=str,
        # default="127.0.0.1",
        help="Set the binded IP address for the merlin server container.",
    )
    server_config.add_argument(
        "-p",
        "--port",
        action="store",
        type=int,
        # default=6379,
        help="Set the binded port for the merlin server container.",
    )
    server_config.add_argument(
        "-pwd",
        "--password",
        action="store",
        type=str,
        # default="~/.merlin/redis.pass",
        help="Set the password file to be used for merlin server container.",
    )
    server_config.add_argument(
        "--add-user",
        action="store",
        nargs=2,
        type=str,
        help="Create a new user for merlin server instance. (Provide both username and password)",
    )
    server_config.add_argument("--remove-user", action="store", type=str, help="Remove an exisiting user.")
    server_config.add_argument(
        "-d",
        "--directory",
        action="store",
        type=str,
        # default="./",
        help="Set the working directory of the merlin server container.",
    )
    server_config.add_argument(
        "-ss",
        "--snapshot-seconds",
        action="store",
        type=int,
        # default=300,
        help="Set the number of seconds merlin server waits before checking if a snapshot is needed.",
    )
    server_config.add_argument(
        "-sc",
        "--snapshot-changes",
        action="store",
        type=int,
        # default=100,
        help="Set the number of changes that are required to be made to the merlin server before a snapshot is made.",
    )
    server_config.add_argument(
        "-sf",
        "--snapshot-file",
        action="store",
        type=str,
        # default="dump.db",
        help="Set the snapshot filename for database dumps.",
    )
    server_config.add_argument(
        "-am",
        "--append-mode",
        action="store",
        type=str,
        # default="everysec",
        help="The appendonly mode to be set. The avaiable options are always, everysec, no.",
    )
    server_config.add_argument(
        "-af",
        "--append-file",
        action="store",
        type=str,
        # default="appendonly.aof",
        help="Set append only filename for merlin server container.",
    )

    return parser


def generate_worker_touching_parsers(subparsers: ArgumentParser) -> None:
    """All CLI arg parsers directly controlling or invoking workers are generated here.

    :param [ArgumentParser] `subparsers`: the subparsers needed for every CLI command that directly controls or invokes
        workers.
    """
    # merlin run-workers
    run_workers: ArgumentParser = subparsers.add_parser(
        "run-workers",
        help="Run the workers associated with the Merlin YAML study "
        "specification. Does -not- queue tasks, just workers tied "
        "to the correct queues.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    run_workers.set_defaults(func=launch_workers)
    run_workers.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
    run_workers.add_argument(
        "--worker-args",
        type=str,
        dest="worker_args",
        default="",
        help="celery worker arguments in quotes.",
    )
    run_workers.add_argument(
        "--steps",
        nargs="+",
        type=str,
        dest="worker_steps",
        default=["all"],
        help="The specific steps in the YAML file you want workers for",
    )
    run_workers.add_argument(
        "--echo",
        action="store_true",
        default=False,
        dest="worker_echo_only",
        help="Just echo the command; do not actually run it",
    )
    run_workers.add_argument(
        "--vars",
        action="store",
        dest="variables",
        type=str,
        nargs="+",
        default=None,
        help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
        "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
    )
    run_workers.add_argument(
        "--disable-logs",
        action="store_true",
        help="Turn off the logs for the celery workers. Note: having the -l flag "
        "in your workers' args section will overwrite this flag for that worker.",
    )

    # merlin query-workers
    query: ArgumentParser = subparsers.add_parser("query-workers", help="List connected task server workers.")
    query.set_defaults(func=query_workers)
    query.add_argument(
        "--task_server",
        type=str,
        default="celery",
        help="Task server type from which to query workers.\
                            Default: %(default)s",
    )
    query.add_argument(
        "--spec",
        type=str,
        default=None,
        help="Path to a Merlin YAML spec file from which to read worker names to query.",
    )
    query.add_argument("--queues", type=str, default=None, nargs="+", help="Specific queues to query workers from.")
    query.add_argument(
        "--workers",
        type=str,
        action="store",
        nargs="+",
        default=None,
        help="Regex match for specific workers to query.",
    )

    # merlin stop-workers
    stop: ArgumentParser = subparsers.add_parser("stop-workers", help="Attempt to stop all task server workers.")
    stop.set_defaults(func=stop_workers)
    stop.add_argument(
        "--spec",
        type=str,
        default=None,
        help="Path to a Merlin YAML spec file from which to read worker names to stop.",
    )
    stop.add_argument(
        "--task_server",
        type=str,
        default="celery",
        help="Task server type from which to stop workers.\
                            Default: %(default)s",
    )
    stop.add_argument("--queues", type=str, default=None, nargs="+", help="specific queues to stop")
    stop.add_argument(
        "--workers",
        type=str,
        action="store",
        nargs="+",
        default=None,
        help="regex match for specific workers to stop",
    )

    # merlin monitor
    monitor: ArgumentParser = subparsers.add_parser(
        "monitor",
        help="Check for active workers on an allocation.",
        formatter_class=RawTextHelpFormatter,
    )
    monitor.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
    monitor.add_argument(
        "--steps",
        nargs="+",
        type=str,
        dest="steps",
        default=["all"],
        help="The specific steps (tasks on the server) in the YAML file defining the queues you want to monitor",
    )
    monitor.add_argument(
        "--vars",
        action="store",
        dest="variables",
        type=str,
        nargs="+",
        default=None,
        help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
        "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
    )
    monitor.add_argument(
        "--task_server",
        type=str,
        default="celery",
        help="Task server type for which to monitor the workers.\
                              Default: %(default)s",
    )
    monitor.add_argument(
        "--sleep",
        type=int,
        default=60,
        help="Sleep duration between checking for workers.\
                                    Default: %(default)s",
    )
    monitor.set_defaults(func=process_monitor)


def generate_diagnostic_parsers(subparsers: ArgumentParser) -> None:
    """All CLI arg parsers generally used diagnostically are generated here.

    :param [ArgumentParser] `subparsers`: the subparsers needed for every CLI command that handles diagnostics for a
        Merlin job.
    """
    # merlin status
    status: ArgumentParser = subparsers.add_parser(
        "status",
        help="List server stats (name, number of tasks to do, \
                              number of connected workers) for a workflow spec.",
    )
    status.set_defaults(func=query_status)
    status.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
    status.add_argument(
        "--steps",
        nargs="+",
        type=str,
        dest="steps",
        default=["all"],
        help="The specific steps in the YAML file you want to query",
    )
    status.add_argument(
        "--task_server",
        type=str,
        default="celery",
        help="Task server type.\
                            Default: %(default)s",
    )
    status.add_argument(
        "--vars",
        action="store",
        dest="variables",
        type=str,
        nargs="+",
        default=None,
        help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
        "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
    )
    status.add_argument("--csv", type=str, help="csv file to dump status report to", default=None)

    # merlin info
    info: ArgumentParser = subparsers.add_parser(
        "info",
        help="display info about the merlin configuration and the python configuration. Useful for debugging.",
    )
    info.set_defaults(func=print_info)


def main():
    """
    High-level CLI operations.
    """
    parser = setup_argparse()
    if len(sys.argv) == 1:
        parser.print_help(sys.stdout)
        return 1
    args = parser.parse_args()

    setup_logging(logger=LOG, log_level=args.level.upper(), colors=True)

    try:
        args.func(args)
        # pylint complains that this exception is too broad - being at the literal top of the program stack,
        # it's ok.
    except Exception as excpt:  # pylint: disable=broad-except
        LOG.debug(traceback.format_exc())
        LOG.error(str(excpt))
        sys.exit(1)
    # All paths in a function ought to return an exit code, or none of them should. Given the
    # distributed nature of Merlin, maybe it doesn't make sense for it to exit 0 until the work is completed, but
    # if the work is dispatched with no errors, that is a 'successful' Merlin run - any other failures are runtime.
    sys.exit()


if __name__ == "__main__":
    main()
