###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.0.5.
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
import traceback
from argparse import (
    ArgumentDefaultsHelpFormatter,
    ArgumentParser,
    RawDescriptionHelpFormatter,
)
from contextlib import suppress

from merlin import VERSION, router
from merlin.ascii_art import banner_small
from merlin.log_formatter import setup_logging
from merlin.spec.expansion import RESERVED, get_spec_with_expansion
from merlin.study.study import MerlinStudy
from merlin.utils import ARRAY_FILE_FORMATS


LOG = logging.getLogger("merlin")
DEFAULT_LOG_LEVEL = "INFO"


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
                raise ValueError("--vars requires valid variable names.")
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
    study = MerlinStudy(
        filepath,
        override_vars=variables_dict,
        samples_file=samples_file,
        dry_run=args.dry,
    )
    router.run_task_server(study, args.run_mode)


def process_restart(args):
    """
    CLI command for restarting a study.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    restart_dir = verify_dirpath(args.restart_dir)
    filepath = os.path.join(args.restart_dir, "merlin_info", "*.yaml")
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
    filepath = verify_filepath(args.specification)
    LOG.info(f"Lauching workers from '{filepath}'")
    variables_dict = parse_override_vars(args.variables)
    spec = get_spec_with_expansion(filepath, override_vars=variables_dict)
    status = router.launch_workers(
        spec, args.worker_steps, args.worker_args, args.worker_echo_only
    )
    if args.worker_echo_only:
        print(status)
    else:
        LOG.info(status)


def purge_tasks(args):
    """
    CLI command for purging tasks.

    :param `args`: parsed CLI arguments
    """
    print(banner_small)
    filepath = verify_filepath(args.specification)
    variables_dict = parse_override_vars(args.variables)
    spec = get_spec_with_expansion(filepath, override_vars=variables_dict)
    ret = router.purge_tasks(
        spec.merlin["resources"]["task_server"],
        spec,
        args.purge_force,
        args.purge_steps,
    )

    LOG.info(f"Purge return = {ret} .")


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
    router.stop_workers(args.task_server, args.queues, args.workers)


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
    _ = router.create_config(args.task_server, output_dir)


def setup_argparse():
    """
    Setup argparse and any CLI options we want available via the package.
    """
    parser = ArgumentParser(
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
        help="Task server type from which to stop workers.\
                            Default: %(default)s",
    )

    # merlin info
    info = subparsers.add_parser(
        "info", help="show pip and python versions and locations"
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
