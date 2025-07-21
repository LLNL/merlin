##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for launching Merlin worker processes.

This module defines the `RunWorkersCommand` class, which implements the `run-workers`
subcommand in the Merlin CLI. The command starts worker processes that execute tasks
defined in a Merlin YAML workflow specification, associating workers with the
correct task queues without queuing tasks themselves.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from typing import List, Set, Union

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import get_merlin_spec_with_override
from merlin.config.configfile import initialize_config
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.spec.specification import MerlinSpec
from merlin.workers.celery_worker import CeleryWorker


LOG = logging.getLogger("merlin")


class RunWorkersCommand(CommandEntryPoint):
    """
    Handles `run-workers` CLI command for launching Merlin workers.

    Methods:
        add_parser: Adds the `run-workers` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `run-workers` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `run-workers` command parser will be added.
        """
        run_workers: ArgumentParser = subparsers.add_parser(
            "run-workers",
            help="Run the workers associated with the Merlin YAML study "
            "specification. Does -not- queue tasks, just workers tied "
            "to the correct queues.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        run_workers.set_defaults(func=self.process_command)
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

    # TODO when we move the queues setting to within the worker then this will no longer be necessary
    def _get_workers_to_start(self, spec: MerlinSpec, steps: Union[List[str], None]) -> Set[str]:
        """
        Determine the set of workers to start based on the specified steps (if any)

        This helper function retrieves a mapping of steps to their corresponding workers
        from a [`MerlinSpec`][spec.specification.MerlinSpec] object and returns a unique
        set of workers that should be started for the provided list of steps. If a step
        is not found in the mapping, a warning is logged.

        Args:
            spec (spec.specification.MerlinSpec): An instance of the
                [`MerlinSpec`][spec.specification.MerlinSpec] class that contains the
                mapping of steps to workers.
            steps: A list of steps for which workers need to be started or None if the user
                didn't provide specific steps.

        Returns:
            A set of unique workers to be started based on the specified steps.
        """
        steps_provided = False if "all" in steps else True

        if steps_provided:
            workers_to_start = []
            step_worker_map = spec.get_step_worker_map()
            for step in steps:
                try:
                    workers_to_start.extend(step_worker_map[step])
                except KeyError:
                    LOG.warning(f"Cannot start workers for step: {step}. This step was not found.")

            workers_to_start = set(workers_to_start)
        else:
            workers_to_start = set(spec.merlin["resources"]["workers"])

        LOG.debug(f"workers_to_start: {workers_to_start}")
        return workers_to_start

    # TODO this should move to TaskServerInterface and be abstracted (build_worker_list ?)
    def _get_worker_instances(self, workers_to_start: Set[str], spec: MerlinSpec) -> List[CeleryWorker]:
        """
        """
        workers = []
        all_workers = spec.merlin["resources"]["workers"]
        overlap = spec.merlin["resources"]["overlap"]
        full_env = spec.get_full_environment()

        for worker_name in workers_to_start:
            settings = all_workers[worker_name]
            config = {
                "args": settings.get("args", ""),
                "machines": settings.get("machines", []),
                "queues": spec.get_queue_list(settings["steps"]),
                "batch": settings["batch"] if settings["batch"] is not None else spec.batch.copy()
            }

            if "nodes" in settings and settings["nodes"] is not None:
                if config["batch"]:
                    config["batch"]["nodes"] = settings["nodes"]
                else:
                    config["batch"] = {"nodes": settings["nodes"]}

            LOG.debug(f"config for worker '{worker_name}': {config}")

            workers.append(CeleryWorker(name=worker_name, config=config, env=full_env, overlap=overlap))
            LOG.debug(f"Created CeleryWorker object for worker '{worker_name}'.")

        return workers

    def process_command(self, args: Namespace):
        """
        CLI command for launching workers.

        This function initializes worker processes for executing tasks as defined
        in the Merlin specification.

        Args:
            args: Parsed CLI arguments containing:\n
                - `worker_echo_only`: If True, don't start the workers and just echo the launch command
                - Additional worker-related parameters such as:
                    - `worker_steps`: Only start workers for these steps.
                    - `worker_args`: Arguments to pass to the worker processes.
                    - `disable_logs`: If True, disables logging for the worker processes.
        """
        if not args.worker_echo_only:
            print(banner_small)
        else:
            initialize_config(local_mode=True)

        spec, filepath = get_merlin_spec_with_override(args)
        if not args.worker_echo_only:
            LOG.info(f"Launching workers from '{filepath}'")

        # Initialize the database
        merlin_db = MerlinDatabase()

        # Create logical worker entries
        step_queue_map = spec.get_task_queues()
        for worker, steps in spec.get_worker_step_map().items():
            worker_queues = {step_queue_map[step] for step in steps}
            merlin_db.create("logical_worker", worker, worker_queues)

        # Get the names of the workers that the user is requesting to start
        workers_to_start = self._get_workers_to_start(spec, args.worker_steps)

        # Build a list of MerlinWorker instances
        worker_instances = self._get_worker_instances(workers_to_start, spec)

        # Launch the workers or echo out the command that will be used to launch the workers
        for worker in worker_instances:
            if args.worker_echo_only:
                LOG.debug(f"Not launching worker '{worker.name}', just echoing command.")
                launch_cmd = worker.get_launch_command(override_args=args.worker_args, disable_logs=args.disable_logs)
                print(launch_cmd)
            else:
                LOG.debug(f"Launching worker '{worker.name}'.")
                worker.launch_worker(override_args=args.worker_args, disable_logs=args.disable_logs)
