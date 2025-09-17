##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module will contain the testing logic
for the `merlin purge` command.
"""

import os
import subprocess
from typing import Dict, List, Tuple, Union

from merlin.spec.expansion import get_spec_with_expansion
from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.fixture_data_classes import RedisBrokerAndBackend
from tests.fixture_types import FixtureRedis, FixtureStr
from tests.integration.conditions import HasRegex, HasReturnCode
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd


class TestPurgeCommand:
    """
    Tests for the `merlin purge` command.
    """

    demo_workflow = os.path.join("examples", "workflows", "feature_demo", "feature_demo.yaml")

    def setup_test(self, path_to_merlin_codebase: FixtureStr, merlin_server_dir: FixtureStr) -> str:
        """
        Setup the test environment for these tests by:
        1. Copying the app.yaml file created by the `redis_server` fixture to the cwd so that
           Merlin can connect to the test server.
        2. Obtaining the path to the feature_demo spec that we'll use for these tests.

        Args:
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.

        Returns:
            The path to the feature_demo spec file.
        """
        copy_app_yaml_to_cwd(merlin_server_dir)
        return os.path.join(path_to_merlin_codebase, self.demo_workflow)

    def setup_tasks(self, celery_task_manager: CeleryTaskManager, spec_file: str) -> Tuple[Dict[str, str], int]:
        """
        Helper method to setup tasks in the specified queues.

        This method sends tasks named 'task_for_{queue}' to each queue defined in the
        provided spec file and returns the total number of queues that received tasks.

        Args:
            celery_task_manager:
                A context manager for managing Celery tasks, used to send tasks to the server.
            spec_file:
                The path to the spec file from which queues will be extracted.

        Returns:
            A tuple with:
                - A dictionary where the keys are step names and values are their associated queues.
                - The number of queues that received tasks
        """
        spec = get_spec_with_expansion(spec_file)
        queues_in_spec = spec.get_task_queues()

        for queue in queues_in_spec.values():
            celery_task_manager.send_task(f"task_for_{queue}", queue=queue)

        return queues_in_spec, len(queues_in_spec.values())

    def run_purge(
        self,
        spec_file: str,
        input_value: str = None,
        force: bool = False,
        steps_to_purge: List[str] = None,
    ) -> Dict[str, Union[str, int]]:
        """
        Helper method to run the purge command.

        Args:
            spec_file: The path to the spec file from which queues will be purged.
            input_value: Any input we need to send to the subprocess.
            force: If True, add the `-f` option to the purge command.
            steps_to_purge: An optional list of steps to send to the purge command.

        Returns:
            The result from executing the command in a subprocess.
        """
        purge_cmd = (
            "merlin purge"
            + (" -f" if force else "")
            + f" {spec_file}"
            + (f" --steps {' '.join(steps_to_purge)}" if steps_to_purge is not None else "")
        )
        result = subprocess.run(purge_cmd, shell=True, capture_output=True, text=True, input=input_value)
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "return_code": result.returncode,
        }

    def check_queues(
        self,
        redis_client: FixtureRedis,
        queues_in_spec: Dict[str, str],
        expected_task_count: int,
        steps_to_purge: List[str] = None,
    ):
        """
        Check the state of queues in Redis against expected task counts.

        When `steps_to_purge` is set, the `expected_task_count` will represent the
        number of expected tasks in the queues that _are not_ associated with the
        steps in the `steps_to_purge` list.

        Args:
            redis_client: The Redis client instance.
            queues_in_spec: A dictionary of queues to check.
            expected_task_count: The expected number of tasks in the queues (0 or 1).
            steps_to_purge: Optional list of steps to determine which queues should be purged.
        """
        for queue in queues_in_spec.values():
            # Brackets are special chars in regex so we have to add \ to make them literal
            queue = queue.replace("[", "\\[").replace("]", "\\]")
            matching_queues_on_server = redis_client.keys(pattern=f"{queue}*")

            for matching_queue in matching_queues_on_server:
                tasks = redis_client.lrange(matching_queue, 0, -1)
                if steps_to_purge and matching_queue in [queues_in_spec[step] for step in steps_to_purge]:
                    assert len(tasks) == 0, f"Expected 0 tasks in {matching_queue}, found {len(tasks)}."
                else:
                    assert (
                        len(tasks) == expected_task_count
                    ), f"Expected {expected_task_count} tasks in {matching_queue}, found {len(tasks)}."

    def test_no_options_tasks_exist_y(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_merlin_codebase: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin purge` command with no options added and
        tasks sent to the server. This should come up with a y/N
        prompt in which we type 'y'. This should then purge the
        tasks from the server.

        Args:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

        feature_demo = self.setup_test(path_to_merlin_codebase, merlin_server_dir)

        with CeleryTaskManager(celery_app, redis_broker_and_backend_function.client) as celery_task_manager:
            # Send tasks to the server for every queue in the spec
            queues_in_spec, num_queues = self.setup_tasks(celery_task_manager, feature_demo)

            # Run the purge test
            test_info = self.run_purge(feature_demo, input_value="y")

            # Make sure the subprocess ran and the correct output messages are given
            conditions = [
                HasReturnCode(),
                HasRegex("Are you sure you want to delete all tasks?"),
                HasRegex(f"Purged {num_queues} messages from {num_queues} known task queues."),
            ]
            check_test_conditions(conditions, test_info)

            # Check on the Redis queues to ensure they were purged
            self.check_queues(redis_broker_and_backend_function.client, queues_in_spec, expected_task_count=0)

    def test_no_options_no_tasks_y(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_merlin_codebase: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin purge` command with no options added and
        no tasks sent to the server. This should come up with a y/N
        prompt in which we type 'y'. This should then give us a "No
        messages purged" log.

        Args:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

        feature_demo = self.setup_test(path_to_merlin_codebase, merlin_server_dir)

        with CeleryTaskManager(celery_app, redis_broker_and_backend_function.client):
            # Get the queues from the spec file
            spec = get_spec_with_expansion(feature_demo)
            queues_in_spec = spec.get_task_queues()
            num_queues = len(queues_in_spec.values())

            # Check that there are no tasks in the queues before we run the purge command
            self.check_queues(redis_broker_and_backend_function.client, queues_in_spec, expected_task_count=0)

            # Run the purge test
            test_info = self.run_purge(feature_demo, input_value="y")

            # Make sure the subprocess ran and the correct output messages are given
            conditions = [
                HasReturnCode(),
                HasRegex("Are you sure you want to delete all tasks?"),
                HasRegex(f"No messages purged from {num_queues} queues."),
            ]
            check_test_conditions(conditions, test_info)

            # Check that the Redis server still has no tasks
            self.check_queues(redis_broker_and_backend_function.client, queues_in_spec, expected_task_count=0)

    def test_no_options_n(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_merlin_codebase: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin purge` command with no options added and
        tasks sent to the server. This should come up with a y/N
        prompt in which we type 'N'. This should take us out of the
        command without purging the tasks.

        Args:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

        feature_demo = self.setup_test(path_to_merlin_codebase, merlin_server_dir)

        with CeleryTaskManager(celery_app, redis_broker_and_backend_function.client) as celery_task_manager:
            # Send tasks to the server for every queue in the spec
            queues_in_spec, num_queues = self.setup_tasks(celery_task_manager, feature_demo)

            # Run the purge test
            test_info = self.run_purge(feature_demo, input_value="N")

            # Make sure the subprocess ran and the correct output messages are given
            conditions = [
                HasReturnCode(),
                HasRegex("Are you sure you want to delete all tasks?"),
                HasRegex(f"Purged {num_queues} messages from {num_queues} known task queues.", negate=True),
            ]
            check_test_conditions(conditions, test_info)

            # Check on the Redis queues to ensure they were not purged
            self.check_queues(redis_broker_and_backend_function.client, queues_in_spec, expected_task_count=1)

    def test_force_option(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_merlin_codebase: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin purge` command with the `--force` option
        enabled. This should not bring up a y/N prompt and should
        immediately purge all tasks.

        Args:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

        feature_demo = self.setup_test(path_to_merlin_codebase, merlin_server_dir)

        with CeleryTaskManager(celery_app, redis_broker_and_backend_function.client) as celery_task_manager:
            # Send tasks to the server for every queue in the spec
            queues_in_spec, num_queues = self.setup_tasks(celery_task_manager, feature_demo)

            # Run the purge test
            test_info = self.run_purge(feature_demo, force=True)

            # Make sure the subprocess ran and the correct output messages are given
            conditions = [
                HasReturnCode(),
                HasRegex("Are you sure you want to delete all tasks?", negate=True),
                HasRegex(f"Purged {num_queues} messages from {num_queues} known task queues."),
            ]
            check_test_conditions(conditions, test_info)

            # Check on the Redis queues to ensure they were purged
            self.check_queues(redis_broker_and_backend_function.client, queues_in_spec, expected_task_count=0)

    def test_steps_option(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_merlin_codebase: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin purge` command with the `--steps` option
        enabled. This should only purge the tasks in the task queues
        associated with the steps provided.

        Args:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

        feature_demo = self.setup_test(path_to_merlin_codebase, merlin_server_dir)

        with CeleryTaskManager(celery_app, redis_broker_and_backend_function.client) as celery_task_manager:
            # Send tasks to the server for every queue in the spec
            queues_in_spec, _ = self.setup_tasks(celery_task_manager, feature_demo)

            # Run the purge test
            steps_to_purge = ["hello", "collect"]
            test_info = self.run_purge(feature_demo, input_value="y", steps_to_purge=steps_to_purge)

            # Make sure the subprocess ran and the correct output messages are given
            num_steps_to_purge = len(steps_to_purge)
            conditions = [
                HasReturnCode(),
                HasRegex("Are you sure you want to delete all tasks?"),
                HasRegex(f"Purged {num_steps_to_purge} messages from {num_steps_to_purge} known task queues."),
            ]
            check_test_conditions(conditions, test_info)

            # Check on the Redis queues to ensure they were not purged
            self.check_queues(
                redis_broker_and_backend_function.client, queues_in_spec, expected_task_count=1, steps_to_purge=steps_to_purge
            )
