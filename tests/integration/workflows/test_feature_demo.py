"""
This module contains tests for the feature_demo workflow.
"""
import subprocess

from tests.fixture_types import FixtureInt, FixtureStr
from tests.integration.conditions import ProvenanceYAMLFileHasRegex, StepFinishedFilesCount


class TestFeatureDemo:
    """
    Tests for the feature_demo workflow.
    """

    def test_end_to_end_run(
        self,
        feature_demo_testing_dir: FixtureStr,
        feature_demo_num_samples: FixtureInt,
        feature_demo_name: FixtureStr,
        feature_demo_run_workflow: subprocess.CompletedProcess,
    ):
        """
        Test that the workflow runs from start to finish with no problems.

        This will check that each step has the proper amount of `MERLIN_FINISHED` files.
        The workflow will be run via the
        [`feature_demo_run_workflow`][fixtures.feature_demo.feature_demo_run_workflow]
        fixture.

        Args:
            feature_demo_testing_dir: The directory containing the output of the feature
                demo run.
            feature_demo_num_samples: The number of samples we give to the feature demo run.
            feature_demo_name: The name of the feature demo study.
            feature_demo_run_workflow: A fixture to run the feature demo study.
        """
        conditions = [
            ProvenanceYAMLFileHasRegex(  # This condition will check that variable substitution worked
                regex=f"N_SAMPLES: {feature_demo_num_samples}",
                spec_file_name="feature_demo",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                provenance_type="expanded",
            ),
            StepFinishedFilesCount(  # The rest of the conditions will ensure every step ran to completion
                step="hello",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=feature_demo_num_samples,
            ),
            StepFinishedFilesCount(
                step="python2_hello",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="python3_hello",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="collect",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="translate",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="learn",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="make_new_samples",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="predict",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="verify",
                study_name=feature_demo_name,
                output_path=feature_demo_testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
        ]
        for condition in conditions:
            assert condition.passes

    # TODO implement the below tests
    # def test_step_execution_order(self):
    #     """
    #     Test that steps are executed in the correct order.
    #     """
    #     # TODO build a list with the correct order that steps should be ran
    #     # TODO compare the list against the logs from the worker

    # def test_workflow_error_handling(self):
    #     """
    #     Test the behavior when errors arise during the worfklow.

    #     TODO should this test both soft and hard fails? should this test all return codes?
    #     """

    # def test_data_passing(self):
    #     """
    #     Test that data can be successfully passed between steps using built-in Merlin variables.
    #     """
