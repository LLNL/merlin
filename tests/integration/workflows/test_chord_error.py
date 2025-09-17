##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains tests for the feature_demo workflow.
"""

import subprocess

from tests.fixture_data_classes import ChordErrorSetup
from tests.integration.conditions import HasRegex, StepFinishedFilesCount
from tests.integration.helper_funcs import check_test_conditions


class TestChordError:
    """
    Tests for the chord error workflow.
    """

    def test_chord_error_continues(
        self,
        chord_err_setup: ChordErrorSetup,
        chord_err_run_workflow: subprocess.CompletedProcess,
    ):
        """
        Test that this workflow continues through to the end of its execution, even
        though a ChordError will be raised.

        Args:
            chord_err_setup: A fixture that returns a [`ChordErrorSetup`][fixture_data_classes.ChordErrorSetup]
                instance.
            chord_err_run_workflow: A fixture to run the chord error study.
        """

        conditions = [
            HasRegex("Exception raised by request from the user"),
            StepFinishedFilesCount(  # Check that the `process_samples` step has only 2 MERLIN_FINISHED files
                step="process_samples",
                study_name=chord_err_setup.name,
                output_path=chord_err_setup.testing_dir,
                expected_count=2,
                num_samples=3,
            ),
            StepFinishedFilesCount(  # Check that the `samples_and_params` step has all of its MERLIN_FINISHED files
                step="samples_and_params",
                study_name=chord_err_setup.name,
                output_path=chord_err_setup.testing_dir,
                num_parameters=2,
                num_samples=3,
            ),
            StepFinishedFilesCount(  # Check that the final step has a MERLIN_FINISHED file
                step="step_3",
                study_name=chord_err_setup.name,
                output_path=chord_err_setup.testing_dir,
                num_parameters=0,
                num_samples=0,
            ),
        ]

        info = {
            "return_code": chord_err_run_workflow.returncode,
            "stdout": chord_err_run_workflow.stdout.read(),
            "stderr": chord_err_run_workflow.stderr.read(),
        }

        check_test_conditions(conditions, info)
