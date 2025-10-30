##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/study/manager.py` module.
"""

import logging
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.common.enums import RunStatus
from merlin.exceptions import RunNotFoundError, StudyNotFoundError
from merlin.study.manager import StudyManager


@pytest.fixture
def mock_db(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that provides a mocked MerlinDatabase instance.

    Returns:
        A mocked MerlinDatabase instance with pre-configured behavior.
    """
    mock_db = mocker.MagicMock()
    
    # Mock study entity
    mock_study = mocker.MagicMock()
    mock_study.get_runs.return_value = ["run1", "run2", "run3"]
    mock_db.get.return_value = mock_study
    
    return mock_db


@pytest.fixture
def mock_spec(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that provides a mocked MerlinSpec instance.

    Returns:
        A mocked MerlinSpec with pre-configured worker names and queues.
    """
    mock_spec = mocker.MagicMock()
    mock_spec.name = "test_study"
    mock_spec.get_worker_names.return_value = ["worker1", "worker2"]
    mock_spec.get_queue_list.return_value = ["queue1", "queue2", "queue3"]
    return mock_spec


@pytest.fixture
def mock_stop_workers(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the stop_celery_workers function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked stop_celery_workers function.
    """
    return mocker.patch("merlin.study.manager.stop_celery_workers")


@pytest.fixture
def mock_purge_tasks(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the purge_celery_tasks function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked purge_celery_tasks function.
    """
    return mocker.patch("merlin.study.manager.purge_celery_tasks")


class TestStudyManagerCancel:
    """
    Test suite for the StudyManager.cancel method.
    
    This class contains all tests related to cancelling studies, including
    full cancellation, partial cancellation, error handling, and edge cases.
    """

    def test_cancel_full_cancellation_with_defaults(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel performs all three steps by default.

        Verifies that:
        - Workers are stopped
        - Queues are purged
        - Runs are marked as cancelled
        - Result dictionary contains correct information

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        # Setup run entities
        mock_run1 = MagicMock()
        mock_run1.is_active.return_value = True
        mock_run2 = MagicMock()
        mock_run2.is_active.return_value = True
        mock_run3 = MagicMock()
        mock_run3.is_active.return_value = False  # Already completed

        mock_db.get.side_effect = [
            mock_db.get.return_value,  # Study entity
            mock_run1,  # First run
            mock_run2,  # Second run
            mock_run3,  # Third run (inactive)
        ]

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec)

        # Verify workers were stopped
        mock_stop_workers.assert_called_once_with(spec_worker_names=["worker1", "worker2"])

        # Verify queues were purged
        mock_purge_tasks.assert_called_once_with("queue1,queue2,queue3", True)

        # Verify runs were marked as cancelled (only active ones)
        mock_run1.set_status.assert_called_once_with(RunStatus.CANCELLED)
        mock_run2.set_status.assert_called_once_with(RunStatus.CANCELLED)
        mock_run3.set_status.assert_not_called()

        # Verify result
        assert result["study_name"] == "test_study"
        assert result["runs_cancelled"] == 2
        assert result["queues_purged"] == ["queue1", "queue2", "queue3"]
        assert result["workers_stopped"] == ["worker1", "worker2"]

    def test_cancel_with_purge_disabled(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel skips queue purging when purge_queues=False.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_run = MagicMock()
        mock_run.is_active.return_value = True
        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run,
            mock_run,
            mock_run,
        ]

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec, purge_queues=False)

        # Verify queues were NOT purged
        mock_purge_tasks.assert_not_called()

        # Verify other steps still executed
        mock_stop_workers.assert_called_once()
        mock_run.set_status.assert_called()

        # Verify result shows no queues purged
        assert result["queues_purged"] == []

    def test_cancel_with_stop_workers_disabled(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel skips stopping workers when stop_workers=False.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_run = MagicMock()
        mock_run.is_active.return_value = True
        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run,
            mock_run,
            mock_run,
        ]

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec, stop_workers=False)

        # Verify workers were NOT stopped
        mock_stop_workers.assert_not_called()

        # Verify other steps still executed
        mock_purge_tasks.assert_called_once()
        mock_run.set_status.assert_called()

        # Verify result shows no workers stopped
        assert result["workers_stopped"] == []

    def test_cancel_with_mark_cancelled_disabled(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel skips marking runs as cancelled when mark_runs_cancelled=False.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec, mark_runs_cancelled=False)

        # Verify runs were NOT queried or marked as cancelled
        # The get method should only be called for stopping workers and purging queues
        mock_stop_workers.assert_called_once()
        mock_purge_tasks.assert_called_once()

        # Verify result shows no runs cancelled
        assert result["runs_cancelled"] == 0

    def test_cancel_all_options_disabled(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel does nothing when all options are disabled.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(
            mock_spec,
            purge_queues=False,
            stop_workers=False,
            mark_runs_cancelled=False,
        )

        # Verify nothing was executed
        mock_stop_workers.assert_not_called()
        mock_purge_tasks.assert_not_called()

        # Verify result shows no actions taken
        assert result["runs_cancelled"] == 0
        assert result["queues_purged"] == []
        assert result["workers_stopped"] == []

    def test_cancel_handles_study_not_found(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
        caplog,
    ):
        """
        Test that cancel handles StudyNotFoundError gracefully.

        Verifies that:
        - Workers and queues are still processed
        - Error is logged
        - Method doesn't crash

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
            caplog: PyTest fixture for capturing log output.
        """
        mock_db.get.side_effect = StudyNotFoundError("Study not found")

        manager = StudyManager(merlin_db=mock_db)
        
        with caplog.at_level(logging.ERROR):
            result = manager.cancel(mock_spec)

        # Verify workers and queues were still processed
        mock_stop_workers.assert_called_once()
        mock_purge_tasks.assert_called_once()

        # Verify error was logged
        assert "Study 'test_study' not found in database" in caplog.text

        # Verify result shows no runs cancelled
        assert result["runs_cancelled"] == 0

    def test_cancel_handles_run_not_found(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
        caplog,
    ):
        """
        Test that cancel handles RunNotFoundError gracefully.

        Verifies that:
        - Processing continues for other runs
        - Error is logged for missing run
        - Other runs are still processed

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
            caplog: PyTest fixture for capturing log output.
        """
        mock_run1 = MagicMock()
        mock_run1.is_active.return_value = True
        mock_run3 = MagicMock()
        mock_run3.is_active.return_value = True

        mock_db.get.side_effect = [
            mock_db.get.return_value,  # Study entity
            mock_run1,  # First run succeeds
            RunNotFoundError("Run not found"),  # Second run fails
            mock_run3,  # Third run succeeds
        ]

        manager = StudyManager(merlin_db=mock_db)
        
        with caplog.at_level(logging.ERROR):
            result = manager.cancel(mock_spec)

        # Verify error was logged
        assert "Run 'run2' not found in database" in caplog.text

        # Verify other runs were still cancelled
        mock_run1.set_status.assert_called_once_with(RunStatus.CANCELLED)
        mock_run3.set_status.assert_called_once_with(RunStatus.CANCELLED)

        # Verify result shows correct count (2 out of 3 runs)
        assert result["runs_cancelled"] == 2

    def test_cancel_only_marks_active_runs(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel only marks active runs as cancelled, not completed ones.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_run1 = MagicMock()
        mock_run1.is_active.return_value = True
        mock_run2 = MagicMock()
        mock_run2.is_active.return_value = False
        mock_run3 = MagicMock()
        mock_run3.is_active.return_value = False

        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run1,
            mock_run2,
            mock_run3,
        ]

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec)

        # Verify only active run was marked as cancelled
        mock_run1.set_status.assert_called_once_with(RunStatus.CANCELLED)
        mock_run2.set_status.assert_not_called()
        mock_run3.set_status.assert_not_called()

        # Verify result shows correct count
        assert result["runs_cancelled"] == 1

    def test_cancel_with_unexpanded_worker_names(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
        caplog,
    ):
        """
        Test that cancel logs a warning for unexpanded worker names.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
            caplog: PyTest fixture for capturing log output.
        """
        mock_spec.get_worker_names.return_value = ["worker1", "$(UNEXPANDED_WORKER)", "worker2"]

        mock_run = MagicMock()
        mock_run.is_active.return_value = True
        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run,
            mock_run,
            mock_run,
        ]

        manager = StudyManager(merlin_db=mock_db)
        
        with caplog.at_level(logging.WARNING):
            manager.cancel(mock_spec)

        # Verify warning was logged
        assert "Worker '$(UNEXPANDED_WORKER)' is unexpanded" in caplog.text
        assert "Target provenance spec instead?" in caplog.text

        # Verify workers were still stopped (including unexpanded one)
        mock_stop_workers.assert_called_once_with(
            spec_worker_names=["worker1", "$(UNEXPANDED_WORKER)", "worker2"]
        )

    def test_cancel_queue_formatting(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel formats queue names correctly for purge_celery_tasks.

        Verifies that queues are joined with commas.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_spec.get_queue_list.return_value = ["q1", "q2", "q3", "q4"]

        mock_run = MagicMock()
        mock_run.is_active.return_value = True
        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run,
            mock_run,
            mock_run,
        ]

        manager = StudyManager(merlin_db=mock_db)
        manager.cancel(mock_spec)

        # Verify queues were formatted correctly
        mock_purge_tasks.assert_called_once_with("q1,q2,q3,q4", True)

    def test_cancel_with_empty_worker_list(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel handles empty worker list gracefully.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_spec.get_worker_names.return_value = []

        mock_run = MagicMock()
        mock_run.is_active.return_value = True
        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run,
            mock_run,
            mock_run,
        ]

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec)

        # Verify stop_workers was still called with empty list
        mock_stop_workers.assert_called_once_with(spec_worker_names=[])

        # Verify result reflects empty worker list
        assert result["workers_stopped"] == []

    def test_cancel_with_empty_queue_list(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel handles empty queue list gracefully.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_spec.get_queue_list.return_value = []

        mock_run = MagicMock()
        mock_run.is_active.return_value = True
        mock_db.get.side_effect = [
            mock_db.get.return_value,
            mock_run,
            mock_run,
            mock_run,
        ]

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec)

        # Verify purge was called with empty string
        mock_purge_tasks.assert_called_once_with("", True)

        # Verify result reflects empty queue list
        assert result["queues_purged"] == []

    def test_cancel_with_no_runs(
        self,
        mock_db: MagicMock,
        mock_spec: MagicMock,
        mock_stop_workers: MagicMock,
        mock_purge_tasks: MagicMock,
    ):
        """
        Test that cancel handles study with no runs gracefully.

        Args:
            mock_db: Mocked MerlinDatabase instance.
            mock_spec: Mocked MerlinSpec instance.
            mock_stop_workers: Mocked stop_celery_workers function.
            mock_purge_tasks: Mocked purge_celery_tasks function.
        """
        mock_study = MagicMock()
        mock_study.get_runs.return_value = []
        mock_db.get.return_value = mock_study

        manager = StudyManager(merlin_db=mock_db)
        result = manager.cancel(mock_spec)

        # Verify workers and queues were still processed
        mock_stop_workers.assert_called_once()
        mock_purge_tasks.assert_called_once()

        # Verify result shows no runs cancelled
        assert result["runs_cancelled"] == 0
