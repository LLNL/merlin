##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for the `merlin/db_scripts/garbage_collector.py` module.
"""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.db_scripts.garbage_collector import DatabaseGarbageCollector
from merlin.exceptions import RunNotFoundError


@pytest.fixture
def mock_db(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that patches the MerlinDatabase constructor.

    This prevents the DatabaseGarbageCollector from trying to connect
    to a real database during tests.

    Args:
        mocker: Pytest mocker fixture.

    Returns:
        A mocked MerlinDatabase instance.
    """
    return mocker.patch("merlin.db_scripts.garbage_collector.MerlinDatabase")


@pytest.fixture
def gc(mock_db: MagicMock) -> DatabaseGarbageCollector:
    """
    Create a DatabaseGarbageCollector instance with mocked database.

    Args:
        mock_db: Mocked MerlinDatabase instance.

    Returns:
        A DatabaseGarbageCollector instance.
    """
    return DatabaseGarbageCollector(merlin_db=mock_db)


@pytest.fixture
def mock_run() -> MagicMock:
    """
    Create a mock run entity.

    Returns:
        A mocked run instance.
    """
    run = MagicMock()
    run.get_id.return_value = "run-123"
    run.get_workspace.return_value = "/path/to/workspace"
    return run


@pytest.fixture
def mock_logical_worker() -> MagicMock:
    """
    Create a mock logical worker entity.

    Returns:
        A mocked logical worker instance.
    """
    worker = MagicMock()
    worker.get_id.return_value = "logical-worker-123"
    worker.get_name.return_value = "test-worker"
    worker.get_queues.return_value = ["queue1", "queue2"]
    worker.get_runs.return_value = ["run-123"]
    return worker


@pytest.fixture
def mock_physical_worker() -> MagicMock:
    """
    Create a mock physical worker entity.

    Returns:
        A mocked physical worker instance.
    """
    worker = MagicMock()
    worker.get_id.return_value = "physical-worker-123"
    worker.get_name.return_value = "celery@test-worker.hostname"
    worker.get_host.return_value = "hostname"
    worker.get_logical_worker_id.return_value = "logical-worker-123"
    return worker


@pytest.fixture
def mock_study() -> MagicMock:
    """
    Create a mock study entity.

    Returns:
        A mocked study instance.
    """
    study = MagicMock()
    study.get_id.return_value = "study-123"
    study.get_name.return_value = "test-study"
    study.get_runs.return_value = ["run-123"]
    return study


class TestDatabaseGarbageCollectorInit:
    """Tests for DatabaseGarbageCollector initialization."""

    def test_init_with_provided_db(self, mock_db: MagicMock):
        """
        Test initialization with a provided database instance.

        Args:
            mock_db: Mocked MerlinDatabase instance.
        """
        gc = DatabaseGarbageCollector(merlin_db=mock_db)
        assert gc.merlin_db is mock_db
        assert gc._issues == {"run": [], "logical_worker": [], "physical_worker": [], "study": []}

    def test_init_without_provided_db(self, mock_db: MagicMock):
        """
        Test initialization without a provided database instance.

        Args:
            mock_db: Mocked MerlinDatabase instance.
        """
        gc = DatabaseGarbageCollector()
        mock_db.assert_called_once()
        assert gc.merlin_db is not None
        assert gc._issues == {"run": [], "logical_worker": [], "physical_worker": [], "study": []}


class TestPromptForConfirmation:
    """Tests for the _prompt_for_confirmation method."""

    @pytest.mark.parametrize("input_value, expected_result", [("yes", True), ("y", True), ("no", False), ("n", False)])
    def test_prompt_for_confirmation_valid_input(
        self, input_value: str, expected_result: bool, mocker: MockerFixture, gc: DatabaseGarbageCollector
    ):
        """
        Test _prompt_for_confirmation with various valid inputs.

        Args:
            input_value: Simulated user input.
            expected_result: Expected boolean result.
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_input = mocker.patch("builtins.input", return_value=input_value)
        assert gc._prompt_for_confirmation() is expected_result
        mock_input.assert_called_once()

    def test_prompt_for_confirmation_invalid_then_valid(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test confirmation with invalid input followed by valid input.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_input = mocker.patch("builtins.input", side_effect=["invalid", "yes"])
        assert gc._prompt_for_confirmation() is True
        assert mock_input.call_count == 2


class TestCheckRunWorkspaces:
    """Tests for the check_run_workspaces method."""

    def test_check_with_valid_workspace(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock
    ):
        """
        Test checking runs when workspace exists.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        mock_run.get_workspace.return_value = "/tmp"
        mock_db.runs.get_all.return_value = [mock_run]

        with mocker.patch("os.path.exists", return_value=True):
            gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 0

    def test_check_with_invalid_workspace(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock
    ):
        """
        Test checking runs when workspace doesn't exist.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        mock_run.get_workspace.return_value = "/nonexistent/path"
        mock_db.runs.get_all.return_value = [mock_run]

        with mocker.patch("os.path.exists", return_value=False):
            gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 1
        assert gc._issues["run"][0] == mock_run

    def test_check_with_multiple_runs(self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_db: MagicMock):
        """
        Test checking multiple runs with mixed validity.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
        """
        valid_run = MagicMock()
        valid_run.get_workspace.return_value = "/valid/path"

        invalid_run = MagicMock()
        invalid_run.get_workspace.return_value = "/invalid/path"

        mock_db.runs.get_all.return_value = [valid_run, invalid_run]

        def path_exists_side_effect(path: str):
            return path == "/valid/path"

        with mocker.patch("os.path.exists", side_effect=path_exists_side_effect):
            gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 1
        assert gc._issues["run"][0] == invalid_run


class TestCheckOrphanedLogicalWorkers:
    """Tests for the check_orphaned_logical_workers method."""

    def test_worker_with_valid_runs(self, gc: DatabaseGarbageCollector, mock_db: MagicMock):
        """
        Test logical worker with valid runs is not orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
        """
        valid_run = MagicMock()
        valid_run.get_id.return_value = "run-123"

        mock_db.runs.get_all.return_value = [valid_run]
        mock_db.logical_workers.get_all.return_value = []

        gc._issues["run"] = []
        gc.check_orphaned_logical_workers()

        assert len(gc._issues["logical_worker"]) == 0

    def test_worker_with_no_runs(self, gc: DatabaseGarbageCollector, mock_logical_worker: MagicMock, mock_db: MagicMock):
        """
        Test logical worker with no runs is orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_logical_worker: Mocked logical worker instance.
            mock_db: Mocked database instance.
        """
        mock_logical_worker.get_runs.return_value = []

        mock_db.runs.get_all.return_value = []
        mock_db.logical_workers.get_all.return_value = [mock_logical_worker]

        gc.check_orphaned_logical_workers()

        assert len(gc._issues["logical_worker"]) == 1
        assert gc._issues["logical_worker"][0] == mock_logical_worker

    def test_worker_with_invalid_runs(self, gc: DatabaseGarbageCollector, mock_logical_worker: MagicMock, mock_db: MagicMock):
        """
        Test logical worker whose runs are all invalid is orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_logical_worker: Mocked logical worker instance.
            mock_db: Mocked database instance.
        """
        invalid_run = MagicMock()
        invalid_run.get_id.return_value = "run-123"

        mock_logical_worker.get_runs.return_value = ["run-123"]
        gc._issues["run"] = [invalid_run]

        mock_db.runs.get_all.return_value = [invalid_run]
        mock_db.logical_workers.get_all.return_value = [mock_logical_worker]

        gc.check_orphaned_logical_workers()

        assert len(gc._issues["logical_worker"]) == 1

    def test_worker_with_nonexistent_runs(
        self, gc: DatabaseGarbageCollector, mock_logical_worker: MagicMock, mock_db: MagicMock
    ):
        """
        Test logical worker with runs that don't exist in DB is orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_logical_worker: Mocked logical worker instance.
            mock_db: Mocked database instance.
        """
        mock_logical_worker.get_runs.return_value = ["run-999"]

        valid_run = MagicMock()
        valid_run.get_id.return_value = "run-123"

        mock_db.runs.get_all.return_value = [valid_run]
        mock_db.logical_workers.get_all.return_value = [mock_logical_worker]

        gc.check_orphaned_logical_workers()

        assert len(gc._issues["logical_worker"]) == 1


class TestCheckOrphanedPhysicalWorkers:
    """Tests for the check_orphaned_physical_workers method."""

    def test_worker_with_valid_logical_worker(
        self, gc: DatabaseGarbageCollector, mock_physical_worker: MagicMock, mock_db: MagicMock
    ):
        """
        Test physical worker with valid logical worker is not orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_physical_worker: Mocked physical worker instance.
            mock_db: Mocked database instance.
        """
        valid_logical = MagicMock()
        valid_logical.get_id.return_value = "logical-worker-123"

        mock_db.physical_workers.get_all.return_value = [mock_physical_worker]
        mock_db.logical_workers.get_all.return_value = [valid_logical]

        gc._issues["logical_worker"] = []
        gc.check_orphaned_physical_workers()

        assert len(gc._issues["physical_worker"]) == 0

    def test_worker_with_orphaned_logical_worker(
        self, gc: DatabaseGarbageCollector, mock_physical_worker: MagicMock, mock_db: MagicMock
    ):
        """
        Test physical worker whose logical worker is orphaned is also orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_physical_worker: Mocked physical worker instance.
            mock_db: Mocked database instance.
        """
        orphaned_logical = MagicMock()
        orphaned_logical.get_id.return_value = "logical-worker-123"

        gc._issues["logical_worker"] = [orphaned_logical]

        mock_db.physical_workers.get_all.return_value = [mock_physical_worker]
        mock_db.logical_workers.get_all.return_value = [orphaned_logical]

        gc.check_orphaned_physical_workers()

        assert len(gc._issues["physical_worker"]) == 1

    def test_worker_with_nonexistent_logical_worker(
        self, gc: DatabaseGarbageCollector, mock_physical_worker: MagicMock, mock_db: MagicMock
    ):
        """
        Test physical worker whose logical worker doesn't exist is orphaned.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_physical_worker: Mocked physical worker instance.
            mock_db: Mocked database instance.
        """
        mock_physical_worker.get_logical_worker_id.return_value = "logical-999"

        valid_logical = MagicMock()
        valid_logical.get_id.return_value = "logical-worker-123"

        mock_db.physical_workers.get_all.return_value = [mock_physical_worker]
        mock_db.logical_workers.get_all.return_value = [valid_logical]

        gc.check_orphaned_physical_workers()

        assert len(gc._issues["physical_worker"]) == 1


class TestCheckEmptyStudies:
    """Tests for the check_empty_studies method."""

    def test_study_with_valid_runs(self, gc: DatabaseGarbageCollector, mock_study: MagicMock, mock_db: MagicMock):
        """
        Test study with valid runs is not empty.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_study: Mocked study instance.
            mock_db: Mocked database instance.
        """
        valid_run = MagicMock()
        valid_run.get_id.return_value = "run-123"

        mock_db.runs.get_all.return_value = [valid_run]
        mock_db.studies.get_all.return_value = [mock_study]

        gc.check_empty_studies()

        assert len(gc._issues["study"]) == 0

    def test_study_with_no_runs(self, gc: DatabaseGarbageCollector, mock_study: MagicMock, mock_db: MagicMock):
        """
        Test study with no runs is empty.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_study: Mocked study instance.
            mock_db: Mocked database instance.
        """
        mock_study.get_runs.return_value = []

        mock_db.runs.get_all.return_value = []
        mock_db.studies.get_all.return_value = [mock_study]

        gc.check_empty_studies()

        assert len(gc._issues["study"]) == 1

    def test_study_with_invalid_runs(self, gc: DatabaseGarbageCollector, mock_study: MagicMock, mock_db: MagicMock):
        """
        Test study with only invalid runs is empty.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_study: Mocked study instance.
            mock_db: Mocked database instance.
        """
        invalid_run = MagicMock()
        invalid_run.get_id.return_value = "run-123"

        mock_study.get_runs.return_value = ["run-123"]
        gc._issues["run"] = [invalid_run]

        mock_db.runs.get_all.return_value = [invalid_run]
        mock_db.studies.get_all.return_value = [mock_study]

        gc.check_empty_studies()

        assert len(gc._issues["study"]) == 1

    def test_study_with_nonexistent_runs(self, gc: DatabaseGarbageCollector, mock_study: MagicMock, mock_db: MagicMock):
        """
        Test study with runs that don't exist in DB is empty.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_study: Mocked study instance.
            mock_db: Mocked database instance.
        """
        mock_study.get_runs.return_value = ["run-999"]

        valid_run = MagicMock()
        valid_run.get_id.return_value = "run-123"

        mock_db.runs.get_all.return_value = [valid_run]
        mock_db.studies.get_all.return_value = [mock_study]

        gc.check_empty_studies()

        assert len(gc._issues["study"]) == 1


class TestCleanupEntity:
    """Tests for the _cleanup_entity method."""

    def test_cleanup_with_no_issues(self, gc: DatabaseGarbageCollector, mock_db: MagicMock):
        """
        Test cleanup when no issues are found.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
        """
        gc._cleanup_entity("run")
        mock_db.delete.assert_not_called()

    def test_cleanup_runs(self, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock):
        """
        Test cleanup of runs.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        gc._issues["run"] = [mock_run]
        gc._cleanup_entity("run")

        mock_db.delete.assert_called_once_with("run", "run-123")

    def test_cleanup_logical_workers(self, gc: DatabaseGarbageCollector, mock_logical_worker: MagicMock, mock_db: MagicMock):
        """
        Test cleanup of logical workers.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_logical_worker: Mocked logical worker instance.
            mock_db: Mocked database instance.
        """
        gc._issues["logical_worker"] = [mock_logical_worker]
        gc._cleanup_entity("logical_worker")

        mock_db.delete.assert_called_once_with("logical_worker", "logical-worker-123")

    def test_cleanup_physical_workers(self, gc: DatabaseGarbageCollector, mock_physical_worker: MagicMock, mock_db: MagicMock):
        """
        Test cleanup of physical workers.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_physical_worker: Mocked physical worker instance.
            mock_db: Mocked database instance.
        """
        gc._issues["physical_worker"] = [mock_physical_worker]
        gc._cleanup_entity("physical_worker")

        mock_db.delete.assert_called_once_with("physical_worker", "physical-worker-123")

    def test_cleanup_studies(self, gc: DatabaseGarbageCollector, mock_study: MagicMock, mock_db: MagicMock):
        """
        Test cleanup of studies.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_study: Mocked study instance.
            mock_db: Mocked database instance.
        """
        gc._issues["study"] = [mock_study]
        gc._cleanup_entity("study")

        mock_db.delete.assert_called_once_with("study", "study-123", remove_associated_runs=False)

    def test_cleanup_handles_deletion_errors(self, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock):
        """
        Test that cleanup handles deletion errors gracefully.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        gc._issues["run"] = [mock_run]
        mock_db.delete.side_effect = RunNotFoundError("Deletion failed")

        # Should not raise exception
        gc._cleanup_entity("run")
        mock_db.delete.assert_called_once()


class TestCleanupMethods:
    """
    Tests for cleanup_runs, cleanup_logical_workers, cleanup_physical_workers, and cleanup_studies.
    """

    def test_cleanup_runs_calls_cleanup_entity(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that cleanup_runs calls _cleanup_entity.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_cleanup = mocker.patch.object(gc, "_cleanup_entity")
        gc.cleanup_runs()
        mock_cleanup.assert_called_once_with("run")

    def test_cleanup_logical_workers_calls_cleanup_entity(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that cleanup_logical_workers calls _cleanup_entity.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_cleanup = mocker.patch.object(gc, "_cleanup_entity")
        gc.cleanup_logical_workers()
        mock_cleanup.assert_called_once_with("logical_worker")

    def test_cleanup_physical_workers_calls_cleanup_entity(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that cleanup_physical_workers calls _cleanup_entity.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_cleanup = mocker.patch.object(gc, "_cleanup_entity")
        gc.cleanup_physical_workers()
        mock_cleanup.assert_called_once_with("physical_worker")

    def test_cleanup_studies_calls_cleanup_entity(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that cleanup_studies calls _cleanup_entity.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_cleanup = mocker.patch.object(gc, "_cleanup_entity")
        gc.cleanup_studies()
        mock_cleanup.assert_called_once_with("study")


class TestGenerateReport:
    """Tests for the generate_report method."""

    def test_generate_report_with_no_issues(self, gc: DatabaseGarbageCollector):
        """
        Test report generation when no issues are found.

        Args:
            gc: DatabaseGarbageCollector instance.
        """
        report = gc.generate_report()

        assert "Invalid Runs: 0" in report
        assert "Orphaned Logical Workers: 0" in report
        assert "Orphaned Physical Workers: 0" in report
        assert "Empty Studies: 0" in report

    def test_generate_report_with_issues(
        self,
        gc: DatabaseGarbageCollector,
        mock_run: MagicMock,
        mock_logical_worker: MagicMock,
        mock_physical_worker: MagicMock,
        mock_study: MagicMock,
    ):
        """
        Test report generation with all types of issues.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_run: MagicMock instance representing a run.
            mock_logical_worker: MagicMock instance representing a logical worker.
            mock_physical_worker: MagicMock instance representing a physical worker.
            mock_study: MagicMock instance representing a study.
        """
        gc._issues["run"] = [mock_run]
        gc._issues["logical_worker"] = [mock_logical_worker]
        gc._issues["physical_worker"] = [mock_physical_worker]
        gc._issues["study"] = [mock_study]

        report = gc.generate_report()

        assert "Invalid Runs: 1" in report
        assert "/path/to/workspace" in report
        assert "Orphaned Logical Workers: 1" in report
        assert "test-worker" in report
        assert "queue1, queue2" in report
        assert "Orphaned Physical Workers: 1" in report
        assert "celery@test-worker.hostname" in report
        assert "hostname" in report
        assert "Empty Studies: 1" in report
        assert "test-study" in report


class TestScan:
    """Tests for the scan method."""

    def test_scan_all_checks(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test scan with all checks enabled.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_runs = mocker.patch.object(gc, "check_run_workspaces")
        mock_logical_workers = mocker.patch.object(gc, "check_orphaned_logical_workers")
        mock_physical_workers = mocker.patch.object(gc, "check_orphaned_physical_workers")
        mock_studies = mocker.patch.object(gc, "check_empty_studies")
        mocker.patch.object(gc, "generate_report", return_value="Report")

        gc.scan()

        mock_runs.assert_called_once()
        mock_logical_workers.assert_called_once()
        mock_physical_workers.assert_called_once()
        mock_studies.assert_called_once()

    def test_scan_selective_checks(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test scan with selective checks.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_runs = mocker.patch.object(gc, "check_run_workspaces")
        mock_logical_workers = mocker.patch.object(gc, "check_orphaned_logical_workers")
        mock_physical_workers = mocker.patch.object(gc, "check_orphaned_physical_workers")
        mock_studies = mocker.patch.object(gc, "check_empty_studies")
        mocker.patch.object(gc, "generate_report", return_value="Report")

        gc.scan(check_runs=True, check_logical_workers=False, check_physical_workers=False, check_studies=False)

        mock_runs.assert_called_once()
        mock_logical_workers.assert_not_called()
        mock_physical_workers.assert_not_called()
        mock_studies.assert_not_called()


class TestClean:
    """Tests for the clean method."""

    def test_clean_with_no_issues(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test clean when no issues are found.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_cleanup_runs = mocker.patch.object(gc, "cleanup_runs")
        mock_cleanup_logical_workers = mocker.patch.object(gc, "cleanup_logical_workers")
        mock_cleanup_physical_workers = mocker.patch.object(gc, "cleanup_physical_workers")
        mock_cleanup_studies = mocker.patch.object(gc, "cleanup_studies")

        gc.clean()

        mock_cleanup_runs.assert_not_called()
        mock_cleanup_logical_workers.assert_not_called()
        mock_cleanup_physical_workers.assert_not_called()
        mock_cleanup_studies.assert_not_called()

    def test_clean_with_force(self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock):
        """
        Test clean with force flag skips confirmation.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
        """
        gc._issues["run"] = [mock_run]

        mock_prompt = mocker.patch.object(gc, "_prompt_for_confirmation")
        mock_cleanup = mocker.patch.object(gc, "cleanup_runs")

        gc.clean(force=True)

        mock_prompt.assert_not_called()
        mock_cleanup.assert_called_once()

    def test_clean_with_confirmation_yes(self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock):
        """
        Test clean with user confirmation.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
        """
        gc._issues["run"] = [mock_run]

        mocker.patch.object(gc, "_prompt_for_confirmation", return_value=True)
        mock_cleanup = mocker.patch.object(gc, "cleanup_runs")

        gc.clean()

        mock_cleanup.assert_called_once()

    def test_clean_with_confirmation_no(self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock):
        """
        Test clean when user declines confirmation.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
        """
        gc._issues["run"] = [mock_run]

        mocker.patch.object(gc, "_prompt_for_confirmation", return_value=False)
        mock_cleanup = mocker.patch.object(gc, "cleanup_runs")

        gc.clean()

        mock_cleanup.assert_not_called()

    def test_clean_selective_cleanup(
        self,
        mocker: MockerFixture,
        gc: DatabaseGarbageCollector,
        mock_run: MagicMock,
        mock_logical_worker: MagicMock,
    ):
        """
        Test clean with selective cleanup options.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_logical_worker: Mocked logical worker instance.
        """
        gc._issues["run"] = [mock_run]
        gc._issues["logical_worker"] = [mock_logical_worker]

        mock_runs = mocker.patch.object(gc, "cleanup_runs")
        mock_logical_workers = mocker.patch.object(gc, "cleanup_logical_workers")
        mock_physical_workers = mocker.patch.object(gc, "cleanup_physical_workers")
        mock_studies = mocker.patch.object(gc, "cleanup_studies")

        gc.clean(
            check_runs=True,
            check_logical_workers=False,
            check_physical_workers=False,
            check_studies=False,
            force=True,
        )

        mock_runs.assert_called_once()
        mock_logical_workers.assert_not_called()
        mock_physical_workers.assert_not_called()
        mock_studies.assert_not_called()


class TestScanAndClean:
    """Tests for the scan_and_clean method."""

    def test_scan_and_clean_calls_both(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that scan_and_clean calls both scan and clean.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        mock_scan = mocker.patch.object(gc, "scan")
        mock_clean = mocker.patch.object(gc, "clean")

        gc.scan_and_clean(
            check_runs=True,
            check_logical_workers=False,
            check_physical_workers=False,
            check_studies=True,
            force=True,
        )

        mock_scan.assert_called_once_with(
            check_runs=True,
            check_logical_workers=False,
            check_physical_workers=False,
            check_studies=True,
        )
        mock_clean.assert_called_once_with(
            check_runs=True,
            check_logical_workers=False,
            check_physical_workers=False,
            check_studies=True,
            force=True,
        )


# TODO should we move this to the integration test suite?
# - uses the same fixtures as unit tests, if we move this test we should also move the fixtures
class TestIntegration:
    """Integration tests for complete garbage collection workflows."""

    def test_full_garbage_collection_workflow(self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_db: MagicMock):
        """
        Test complete workflow from scan to clean.

        Args:
            mocker: Pytest Mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
        """
        # Set up mock entities
        invalid_run = MagicMock()
        invalid_run.get_id.return_value = "run-invalid"
        invalid_run.get_workspace.return_value = "/invalid/workspace"

        orphaned_logical = MagicMock()
        orphaned_logical.get_id.return_value = "logical-orphaned"
        orphaned_logical.get_name.return_value = "orphaned-worker"
        orphaned_logical.get_queues.return_value = ["queue"]
        orphaned_logical.get_runs.return_value = ["run-invalid"]

        orphaned_physical = MagicMock()
        orphaned_physical.get_id.return_value = "physical-orphaned"
        orphaned_physical.get_name.return_value = "celery@orphaned"
        orphaned_physical.get_host.return_value = "host"
        orphaned_physical.get_logical_worker_id.return_value = "logical-orphaned"

        empty_study = MagicMock()
        empty_study.get_id.return_value = "study-empty"
        empty_study.get_name.return_value = "empty-study"
        empty_study.get_runs.return_value = ["run-invalid"]

        # Configure mock database
        mock_db.runs.get_all.return_value = [invalid_run]
        mock_db.logical_workers.get_all.return_value = [orphaned_logical]
        mock_db.physical_workers.get_all.return_value = [orphaned_physical]
        mock_db.studies.get_all.return_value = [empty_study]

        # Run garbage collection
        mocker.patch("os.path.exists", return_value=False)
        gc.scan_and_clean(force=True)

        # Verify deletions occurred in correct order
        assert mock_db.delete.call_count == 4

        # Verify the deletion calls
        calls = [call[0] for call in mock_db.delete.call_args_list]
        assert ("run", "run-invalid") in calls
        assert ("physical_worker", "physical-orphaned") in calls
        assert ("logical_worker", "logical-orphaned") in calls
        assert ("study", "study-empty") in calls
