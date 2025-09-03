##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains tests for the feature_demo workflow.
"""

import shutil
import subprocess

from tests.fixture_data_classes import FeatureDemoSetup
from tests.integration.conditions import ProvenanceYAMLFileHasRegex, StepFinishedFilesCount


class TestFeatureDemo:
    """
    Tests for the feature_demo workflow.
    """

    def test_end_to_end_run(
        self, feature_demo_setup: FeatureDemoSetup, feature_demo_run_workflow: subprocess.CompletedProcess
    ):
        """
        Test that the workflow runs from start to finish with no problems.

        This will check that each step has the proper amount of `MERLIN_FINISHED` files.
        The workflow will be run via the
        [`feature_demo_run_workflow`][fixtures.feature_demo.feature_demo_run_workflow]
        fixture.

        Args:
            feature_demo_setup: A fixture that returns a
                [`FeatureDemoSetup`][fixture_data_classes.FeatureDemoSetup] instance.
            feature_demo_run_workflow: A fixture to run the feature demo study.
        """
        conditions = [
            ProvenanceYAMLFileHasRegex(  # This condition will check that variable substitution worked
                regex=f"N_SAMPLES: {feature_demo_setup.num_samples}",
                spec_file_name="feature_demo",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                provenance_type="expanded",
            ),
            StepFinishedFilesCount(  # The rest of the conditions will ensure every step ran to completion
                step="hello",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=feature_demo_setup.num_samples,
            ),
            StepFinishedFilesCount(
                step="python3_hello",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="collect",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="translate",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="learn",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="make_new_samples",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="predict",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
            StepFinishedFilesCount(
                step="verify",
                study_name=feature_demo_setup.name,
                output_path=feature_demo_setup.testing_dir,
                num_parameters=1,
                num_samples=0,
            ),
        ]

        # GitHub actions doesn't have a python2 path so we'll conditionally add this check
        if shutil.which("python2"):
            conditions.append(
                StepFinishedFilesCount(
                    step="python2_hello",
                    study_name=feature_demo_setup.name,
                    output_path=feature_demo_setup.testing_dir,
                    num_parameters=1,
                    num_samples=0,
                )
            )

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
