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
    RawDescriptionHelpFormatter,
    RawTextHelpFormatter,
)
from contextlib import suppress

from merlin import VERSION, router
from merlin.ascii_art import banner_small
from merlin.examples.generator import list_examples, setup_example
from merlin.log_formatter import setup_logging
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
        sys.stderr.write("error: %s\n" % message)
        self.print_help()
        sys.exit(2)


def verify_filepath(filepath):
    """
    Verify that the filepath argument is a valid
    file.

    :param `filepath`: the path of a file
    """
    filepath = os.path.abspath(os.path.expandvars(os.path.expanduser(filepath)))
    if not os.path.isfile(filepath):
        raise ValueError(f"'{filepath}' is not a valid filepath")
    return filepath


def verify_dirpath(dirpath):
    """
    Verify that the dirpath argument is a valid
    directory.

    :param `dirpath`: the path of a directory
    """
    dirpath = os.path.abspath(os.path.expandvars(os.path.expanduser(dirpath)))
    if not os.path.isdir(dirpath):
        raise ValueError(f"'{dirpath}' is not a valid directory path")
    return dirpath


def parse_override_vars(variables_list):
    """
    Parse a list of variables from command line syntax
    into a valid dictionary of variable keys and values.

    :param `variables_list`: a list of strings, e.g. ["KEY=val",...]
    """
    if variables_list is None:
        return None
    LOG.debug(f"Command line override variables = {variables_list}")
    result = {}
    for arg in variables_list:
        try:
            if "=" not in arg:
                raise ValueError(
                    "--vars requires '=' operator. See 'merlin run --help' for an example."
                )
            entry = arg.split("=")
            if len(entry) != 2:
                raise ValueError(
                    "--vars requires ONE '=' operator (without spaces) per variable assignment."
                )
            key = entry[0]
            if key is None or key == "" or "$" in key:
                raise ValueError(
                    "--vars requires valid variable names comprised of alphanumeric characters and underscores."
                )
            if key in RESERVED:
                raise ValueError(
                    f"Cannot override reserved word '{key}'! Reserved words are: {RESERVED}."
                )

            val = entry[1]
            with suppress(ValueError):
                int(val)
                val = int(val)
            result[key] = val

        except BaseException as e:
            raise ValueError(
                f"{e} Bad '--vars' formatting on command line. See 'merlin run --help' for an example."
            )
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


def process_run(args):
    """
    CLI command for running a study.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    filepath = verify_filepath(args.specification)
    variables_dict = parse_override_vars(args.variables)
    samples_file = None
    if args.samples_file:
        samples_file = verify_filepath(args.samples_file)

    # pgen checks
    if args.pargs and not args.pgen_file:
        raise ValueError(
            "Cannot use the 'pargs' parameter without specifying a 'pgen'!"
        )
    if args.pgen_file:
        verify_filepath(args.pgen_file)

    study = MerlinStudy(
        filepath,
        override_vars=variables_dict,
        samples_file=samples_file,
        dry_run=args.dry,
        no_errors=args.no_errors,
        pgen_file=args.pgen_file,
        pargs=args.pargs,
    )
    router.run_task_server(study, args.run_mode)


def process_restart(args):
    """
    CLI command for restarting a study.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    restart_dir = verify_dirpath(args.restart_dir)
    filepath = os.path.join(args.restart_dir, "merlin_info", "*.expanded.yaml")
    possible_specs = glob.glob(filepath)
    if len(possible_specs) == 0:
        raise ValueError(
            f"'{filepath}' does not match any provenance spec file to restart from."
        )
    elif len(possible_specs) > 1:
        raise ValueError(
            f"'{filepath}' matches more than one provenance spec file to restart from."
        )
    filepath = verify_filepath(possible_specs[0])
    LOG.info(f"Restarting workflow at '{restart_dir}'")
    study = MerlinStudy(filepath, restart_dir=restart_dir)
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
    status = router.launch_workers(
        spec, args.worker_steps, args.worker_args, args.worker_echo_only
    )
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
    router.query_workers(args.task_server)


def stop_workers(args):
    """
    CLI command for stopping all workers.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    worker_names = []
    if args.spec:
        spec_path = verify_filepath(args.spec)
        spec = MerlinSpec.load_specification(spec_path)
        worker_names = spec.get_worker_names()
        for worker_name in worker_names:
            if "$" in worker_name:
                LOG.warning(
                    f"Worker '{worker_name}' is unexpanded. Target provenance spec instead?"
                )
    router.stop_workers(args.task_server, worker_names, args.queues, args.workers)


def print_info(args):
    """
    CLI command to print merlin config info.

    :param `args`: parsed CLI arguments
    """
    from merlin import display

    display.print_info(args)


def config_merlin(args):
    """
    CLI command to setup default merlin config.

    :param `args`: parsed CLI arguments
    """
    output_dir = args.output_dir
    if output_dir is None:
        USER_HOME = os.path.expanduser("~")
        output_dir = os.path.join(USER_HOME, ".merlin")
    _ = router.create_config(args.task_server, output_dir, args.broker)


def process_example(args):
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


def setup_argparse():
    """
    Setup argparse and any CLI options we want available via the package.
    """
    parser = HelpParser(
        prog="merlin",
        description=banner_small,
        formatter_class=RawDescriptionHelpFormatter,
        epilog="See merlin <command> --help for more info",
    )
    parser.add_argument("-v", "--version", action="version", version=VERSION)
    subparsers = parser.add_subparsers(dest="subparsers")
    subparsers.required = True

    # merlin --level
    parser.add_argument(
        "-lvl",
        "--level",
        action="store",
        dest="level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        help="Set the log level. Options: DEBUG, INFO, WARNING, ERROR. "
        "[Default: %(default)s]",
    )

    # merlin run
    run = subparsers.add_parser(
        "run",
        help="Run a workflow using a Merlin or Maestro YAML study " "specification.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    run.set_defaults(func=process_run)
    run.add_argument(
        "specification", type=str, help="Path to a Merlin or Maestro YAML file"
    )
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
    # TODO add all supported formats to doc string
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
        help="Flag to ignore some errors for testing.",
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
    restart = subparsers.add_parser(
        "restart",
        help="Restart a workflow using an existing Merlin workspace.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    restart.set_defaults(func=process_restart)
    restart.add_argument(
        "restart_dir", type=str, help="Path to an existing Merlin workspace directory"
    )
    restart.add_argument(
        "--local",
        action="store_const",
        dest="run_mode",
        const="local",
        default="distributed",
        help="Run locally instead of distributed",
    )

    # merlin run-workers
    run_workers = subparsers.add_parser(
        "run-workers",
        help="Run the workers associated with the Merlin YAML study "
        "specification. Does -not- queue tasks, just workers tied "
        "to the correct queues.",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    run_workers.set_defaults(func=launch_workers)
    run_workers.add_argument(
        "specification", type=str, help="Path to a Merlin YAML spec file"
    )
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

    # merlin purge
    purge = subparsers.add_parser(
        "purge",
        help="Remove all tasks from all merlin queues (default).              "
        "If a user would like to purge only selected queues use:    "
        "--steps to give a steplist, the queues will be defined from the step list",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    purge.set_defaults(func=purge_tasks)
    purge.add_argument(
        "specification", type=str, help="Path to a Merlin YAML spec file"
    )
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
        help="The specific steps in the YAML file from which you want to purge the queues. The input is a space separated list.",
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

    # merlin stop-workers
    stop = subparsers.add_parser(
        "stop-workers", help="Attempt to stop all task server workers."
    )
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
    stop.add_argument(
        "--queues", type=str, default=None, nargs="+", help="specific queues to stop"
    )
    stop.add_argument(
        "--workers",
        type=str,
        default=None,
        help="regex match for specific workers to stop",
    )

    # merlin query-workers
    query = subparsers.add_parser(
        "query-workers", help="List connected task server workers."
    )
    query.set_defaults(func=query_workers)
    query.add_argument(
        "--task_server",
        type=str,
        default="celery",
        help="Task server type from which to query workers.\
                            Default: %(default)s",
    )

    # merlin status
    status = subparsers.add_parser(
        "status",
        help="List server stats (name, number of tasks to do, \
                              number of connected workers) for a workflow spec.",
    )
    status.set_defaults(func=query_status)
    status.add_argument(
        "specification", type=str, help="Path to a Merlin YAML spec file"
    )
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
    status.add_argument(
        "--csv", type=str, help="csv file to dump status report to", default=None
    )

    # merlin info
    info = subparsers.add_parser(
        "info",
        help="display info about the merlin configuration and the python configuration. Useful for debugging.",
    )
    info.set_defaults(func=print_info)

    mconfig = subparsers.add_parser(
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

    # merlin example
    example = subparsers.add_parser(
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
        help="Specify a path to write the workflow to. Defaults to current "
        "working directory",
    )
    example.set_defaults(func=process_example)

    # merlin monitor
    monitor = subparsers.add_parser(
        "monitor",
        help="Check for active workers on an allocation.",
        formatter_class=RawTextHelpFormatter,
    )
    monitor.add_argument(
        "specification", type=str, help="Path to a Merlin YAML spec file"
    )
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

    return parser


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
    except Exception as e:
        LOG.debug(traceback.format_exc())
        LOG.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
