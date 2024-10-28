"""
This module contains tests for the feature_demo workflow.
"""
import inspect
import os
import signal
import subprocess
from time import sleep
from subprocess import TimeoutExpired

# from tests.context_managers.celery_task_manager import CeleryTaskManager
# from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_types import FixtureInt, FixtureModification, FixtureRedis, FixtureStr, FixtureTuple
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd, load_workers_from_spec


# NOTE maybe this workflow only needs to run twice?
# - once for a normal run
#   - can test e2e, data passing, and step execution order with a single run
# - another time for error testing

class TestFeatureDemo:
    """
    Tests for the feature_demo workflow.
    """
    demo_workflow = os.path.join("examples", "workflows", "feature_demo", "feature_demo.yaml")

    def get_test_name(self):
        """
        """
        stack = inspect.stack()
        return stack[1].function

    def test_end_to_end_run(
        self,
        feature_demo_testing_dir: FixtureStr,
        feature_demo_num_samples: FixtureInt,
        feature_demo_name: FixtureStr,
        feature_demo_run_workflow: FixtureTuple[str, str],
    ):
        """
        Test that the workflow runs from start to finish with no problems.
        """
        # TODO check if the workflow ran to completion in 30 seconds
        # if not assert a failure happened
        # - to check, see if all MERLIN_FINISHED files exist

        StepFinishedFilesCount(
            step="hello",
            study_name=feature_demo_name,
            output_path=feature_demo_testing_dir,
            num_parameters=1,
            num_samples=feature_demo_num_samples,
        )

    def test_step_execution_order(self):
        """
        Test that steps are executed in the correct order.
        """
        # TODO build a list with the correct order that steps should be ran
        # TODO compare the list against the logs from the worker

    def test_workflow_error_handling(self):
        """
        Test the behavior when errors arise during the worfklow.

        TODO should this test both soft and hard fails? should this test all return codes?
        """

    def test_data_passing(self):
        """
        Test that data can be successfully passed between steps using built-in Merlin variables.
        """
