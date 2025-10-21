##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for the `merlin/db_scripts/garbage_collector.py` module.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock import MockerFixture

from merlin.db_scripts.garbage_collector import DatabaseGarbageCollector
from merlin.exceptions import RunNotFoundError
from tests.fixture_types import FixtureCallable, FixtureStr


@pytest.fixture(scope="session")
def garbage_collection_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    garbage collection workflow.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for garbage collection tests.
    """
    return create_testing_dir(temp_output_dir, "garbage_collection_testing")


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
        assert gc._issues == {
            "run": [],
            "logical_worker": [],
            "physical_worker": [],
            "study": [],
            "inaccessible_runs": [],
        }

    def test_init_without_provided_db(self, mock_db: MagicMock):
        """
        Test initialization without a provided database instance.

        Args:
            mock_db: Mocked MerlinDatabase instance.
        """
        gc = DatabaseGarbageCollector()
        mock_db.assert_called_once()
        assert gc.merlin_db is not None
        assert gc._issues == {
            "run": [],
            "logical_worker": [],
            "physical_worker": [],
            "study": [],
            "inaccessible_runs": [],
        }


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


class TestIsWorkspaceOnAccessibleMount:
    """Tests for the _is_workspace_on_accessible_mount method."""

    def test_workspace_on_specific_mount(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test workspace on a specific non-root mount point.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/mnt/shared/workspace"

        mocker.patch(
            "merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/mnt/shared"), Path("/home")}
        )

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True

    # TODO is there a way to check which mount point is matched?
    def test_workspace_on_longest_matching_mount(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that most specific (longest) mount point is matched.

        If workspace is /p/lustre3/data/workspace and both /p and /p/lustre3
        are mounted, should match /p/lustre3.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/p/lustre3/data/workspace"

        mocker.patch(
            "merlin.db_scripts.garbage_collector.get_accessible_mounts",
            return_value={Path("/p"), Path("/p/lustre3"), Path("/home")},
        )

        result = gc._is_workspace_on_accessible_mount(workspace)

        # Should match, and internally should prefer /p/lustre3 over /p
        assert result is True

    def test_workspace_not_on_any_mount(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test workspace that is not on any accessible non-root mount.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/p/lustre3/workspace"

        # Only /home is accessible, not /p/lustre3
        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/home")})

        mock_log = mocker.patch("merlin.db_scripts.garbage_collector.LOG")

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is False
        mock_log.warning.assert_called_once()
        warning_msg = str(mock_log.warning.call_args)
        assert "root filesystem" in warning_msg and "mounted file system" in warning_msg

    def test_workspace_on_root_filesystem(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test workspace on root filesystem (e.g., /tmp, /var).

        Since get_accessible_mounts excludes root, this should return False.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/tmp/workspace"

        # No specific mount for /tmp
        mocker.patch(
            "merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/home"), Path("/mnt/data")}
        )

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is False

    def test_workspace_with_path_object(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that Path objects are handled correctly.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = Path("/mnt/shared/workspace")

        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/mnt/shared")})

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True

    def test_workspace_with_symlink(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, garbage_collection_testing_dir: FixtureStr
    ):
        """
        Test that symlinks are resolved before checking mount points.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            garbage_collection_testing_dir: The path to the temporary output directory where garbage collection
                tests will store their results.
        """
        gc_testing_dir = Path(garbage_collection_testing_dir)

        # Create a real directory and symlink for testing
        real_dir = gc_testing_dir / "real" / "workspace"
        real_dir.mkdir(parents=True)

        symlink = gc_testing_dir / "link"
        symlink.symlink_to(real_dir.parent)

        workspace = symlink / "workspace"

        # Mock the mount to match the real path
        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={gc_testing_dir / "real"})

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True

    def test_workspace_with_relative_path(
        self,
        mocker: MockerFixture,
        gc: DatabaseGarbageCollector,
        garbage_collection_testing_dir: FixtureStr,
        monkeypatch: MonkeyPatch,
    ):
        """
        Test that relative paths are resolved to absolute paths.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            garbage_collection_testing_dir: The path to the temporary output directory where garbage collection
                tests will store their results.
            monkeypatch: Pytest monkeypatch fixture.
        """
        gc_testing_dir = Path(garbage_collection_testing_dir)

        # Create a test directory structure
        test_dir = gc_testing_dir / "mnt" / "shared"
        test_dir.mkdir(parents=True, exist_ok=True)

        # Change to the workspace parent directory
        workspace_dir = test_dir / "workspace"
        workspace_dir.mkdir()

        monkeypatch.chdir(test_dir)

        # Use relative path
        workspace = "./workspace"

        mocker.patch(
            "merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={gc_testing_dir / "mnt" / "shared"}
        )

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True

    def test_no_accessible_mounts(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test behavior when there are no accessible non-root mounts.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/any/workspace"

        # No mounts at all (empty set)
        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value=set())

        mock_log = mocker.patch("merlin.db_scripts.garbage_collector.LOG")

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is False
        mock_log.warning.assert_called_once()

    def test_workspace_at_mount_root(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test workspace located directly at mount point root.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/mnt/shared"

        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/mnt/shared")})

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True

    def test_workspace_similar_but_not_matching_mount(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test workspace with path similar to mount but not under it.

        E.g., /p/lustre2 should not match mount /p/lustre1

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/p/lustre2/workspace"

        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/p/lustre1")})

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is False

    def test_get_accessible_mounts_called_with_exclude_root(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test that get_accessible_mounts is called with exclude_root=True.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/mnt/shared/workspace"

        mock_get_mounts = mocker.patch(
            "merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/mnt/shared")}
        )

        gc._is_workspace_on_accessible_mount(workspace)

        mock_get_mounts.assert_called_once_with(exclude_root=True)

    def test_workspace_with_trailing_slash(self, mocker: MockerFixture, gc: DatabaseGarbageCollector):
        """
        Test workspace path with trailing slash is handled correctly.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
        """
        workspace = "/mnt/shared/workspace/"  # Note trailing slash

        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={Path("/mnt/shared")})

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True

    def test_workspace_with_dot_segments(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, garbage_collection_testing_dir: FixtureStr
    ):
        """
        Test workspace path with .. segment is resolved correctly.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            garbage_collection_testing_dir: The path to the temporary output directory where garbage collection
                tests will store their results.
        """
        gc_testing_dir = Path(garbage_collection_testing_dir)

        # Create test structure
        mount_dir = gc_testing_dir / "mnt" / "shared"
        mount_dir.mkdir(parents=True, exist_ok=True)

        # Path with .. segments that resolves to mount_dir
        workspace = gc_testing_dir / "mnt" / "other" / ".." / "shared" / "workspace"

        mocker.patch("merlin.db_scripts.garbage_collector.get_accessible_mounts", return_value={mount_dir})

        result = gc._is_workspace_on_accessible_mount(workspace)

        assert result is True


class TestCheckRunWorkspaces:
    """Tests for the check_run_workspaces method."""

    def test_check_with_invalid_workspace_on_accessible_mount(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock
    ):
        """
        Test checking runs when workspace is on an accessible mount but doesn't exist.

        Case 1: is_accessible_mount=True but workspace_exists=False
        Expected: Workspace flagged as invalid.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        workspace = "/mnt/shared/missing_workspace"
        mock_run.get_workspace.return_value = workspace
        mock_run.get_id.return_value = "run-123"
        mock_db.runs.get_all.return_value = [mock_run]

        mocker.patch.object(gc, "_is_workspace_on_accessible_mount", return_value=True)
        mocker.patch("os.path.exists", return_value=False)

        gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 1
        assert gc._issues["run"][0] == mock_run

        assert len(gc._issues["inaccessible_runs"]) == 0

    def test_check_with_workspace_on_inaccessible_mount(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock
    ):
        """
        Test checking runs when workspace is on an inaccessible mount.

        Case 2: is_accessible_mount=False and workspace_exists=False
        Expected: Counted as inaccessible, not flagged as invalid.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        workspace = "/p/lustre3/workspace"
        mock_run.get_workspace.return_value = workspace
        mock_run.get_id.return_value = "run-123"
        mock_db.runs.get_all.return_value = [mock_run]

        mocker.patch.object(gc, "_is_workspace_on_accessible_mount", return_value=False)
        mocker.patch("os.path.exists", return_value=False)

        mock_log = mocker.patch("merlin.db_scripts.garbage_collector.LOG")

        gc.check_run_workspaces()

        # Should NOT be in issues list for run entries
        assert len(gc._issues["run"]) == 0

        # Should be in issues list for inaccessible runs
        assert len(gc._issues["inaccessible_runs"]) == 1
        assert gc._issues["inaccessible_runs"][0] == mock_run

        # Should log warning about inaccessible workspaces
        mock_log.warning.assert_called()
        warning_calls = [str(call) for call in list(mock_log.warning.call_args_list + mock_log.debug.call_args_list)]
        assert any("inaccessible" in str(call).lower() for call in warning_calls)

    def test_check_with_valid_workspace_on_root_filesystem(
        self,
        mocker: MockerFixture,
        gc: DatabaseGarbageCollector,
        mock_run: MagicMock,
        mock_db: MagicMock,
        garbage_collection_testing_dir: FixtureStr,
    ):
        """
        Test checking runs when workspace exists on root filesystem (e.g., /tmp).

        Case 3: is_accessible_mount=False but workspace_exists=True
        Expected: No issues flagged (valid local workspace).

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
            garbage_collection_testing_dir: The path to the temporary output directory where garbage collection
                tests will store files associated with this test.
        """
        workspace = os.path.join(garbage_collection_testing_dir, "local_workspace")
        os.makedirs(workspace, exist_ok=True)
        mock_run.get_workspace.return_value = workspace
        mock_run.get_id.return_value = "run-123"
        mock_db.runs.get_all.return_value = [mock_run]

        gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 0
        assert len(gc._issues["inaccessible_runs"]) == 0

    def test_check_with_valid_workspace_on_accessible_mount(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_run: MagicMock, mock_db: MagicMock
    ):
        """
        Test checking runs when workspace exists on an accessible non-root mount.

        Case 4: is_accessible_mount=True and workspace_exists=True
        Expected: No issues flagged.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_run: Mocked run instance.
            mock_db: Mocked database instance.
        """
        workspace = "/mnt/shared/workspace"
        mock_run.get_workspace.return_value = workspace
        mock_run.get_id.return_value = "run-123"
        mock_db.runs.get_all.return_value = [mock_run]

        mocker.patch.object(gc, "_is_workspace_on_accessible_mount", return_value=True)
        mocker.patch("os.path.exists", return_value=True)

        gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 0
        assert len(gc._issues["inaccessible_runs"]) == 0

    def test_check_with_multiple_runs_mixed_cases(
        self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_db: MagicMock, garbage_collection_testing_dir: FixtureStr
    ):
        """
        Test checking multiple runs covering all four cases.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
            garbage_collection_testing_dir: The path to the temporary output directory where garbage collection
                tests will store files associated with this test.
        """
        # Create real directories for valid workspaces
        valid_local_path = os.path.join(garbage_collection_testing_dir, "valid_local")
        valid_mount_path = os.path.join(garbage_collection_testing_dir, "mnt", "shared", "valid")
        os.makedirs(valid_local_path, exist_ok=True)
        os.makedirs(valid_mount_path, exist_ok=True)
        
        # Case 1: Invalid workspace on accessible mount (should be flagged)
        invalid_accessible = MagicMock()
        invalid_accessible.get_workspace.return_value = os.path.join(garbage_collection_testing_dir, "mnt", "shared", "invalid")
        invalid_accessible.get_id.return_value = "run-invalid-accessible"

        # Case 2: Workspace on inaccessible mount (should not be flagged)
        inaccessible = MagicMock()
        inaccessible.get_workspace.return_value = "/p/lustre3/workspace"
        inaccessible.get_id.return_value = "run-inaccessible"

        # Case 3: Valid local workspace on root filesystem
        valid_local = MagicMock()
        valid_local.get_workspace.return_value = valid_local_path
        valid_local.get_id.return_value = "run-valid-local"

        # Case 4: Valid workspace on accessible mount
        valid_accessible = MagicMock()
        valid_accessible.get_workspace.return_value = valid_mount_path
        valid_accessible.get_id.return_value = "run-valid-accessible"

        mock_db.runs.get_all.return_value = [invalid_accessible, inaccessible, valid_local, valid_accessible]

        # Mock _is_workspace_on_accessible_mount based on workspace path
        def is_accessible_side_effect(workspace):
            # Workspaces under garbage_collection_testing_dir/mnt/shared are accessible
            # /p/lustre3 is inaccessible (real shared mount)
            workspace_str = str(workspace)
            return (
                workspace_str.startswith(os.path.join(garbage_collection_testing_dir, "mnt", "shared"))
                and not workspace_str.startswith("/p/lustre3")
            )

        mocker.patch.object(gc, "_is_workspace_on_accessible_mount", side_effect=is_accessible_side_effect)

        gc.check_run_workspaces()

        # Only invalid_accessible should be in 'run' issues (Case 1)
        assert len(gc._issues["run"]) == 1
        assert gc._issues["run"][0] == invalid_accessible

        # Only inaccessible should be in 'inaccessible_runs' issues (Case 2)
        assert len(gc._issues["inaccessible_runs"]) == 1
        assert gc._issues["inaccessible_runs"][0] == inaccessible

    def test_check_logs_inaccessible_count(self, mocker: MockerFixture, gc: DatabaseGarbageCollector, mock_db: MagicMock):
        """
        Test that inaccessible workspace count is logged correctly.

        Args:
            mocker: Pytest mocker fixture.
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
        """
        run1 = MagicMock()
        run1.get_workspace.return_value = "/p/lustre3/ws1"
        run1.get_id.return_value = "run-1"

        run2 = MagicMock()
        run2.get_workspace.return_value = "/p/lustre3/ws2"
        run2.get_id.return_value = "run-2"

        run3 = MagicMock()
        run3.get_workspace.return_value = "/mnt/shared/ws3"
        run3.get_id.return_value = "run-3"

        mock_db.runs.get_all.return_value = [run1, run2, run3]

        mocker.patch.object(gc, "_is_workspace_on_accessible_mount", return_value=False)
        mocker.patch("os.path.exists", return_value=False)

        mock_log = mocker.patch("merlin.db_scripts.garbage_collector.LOG")

        gc.check_run_workspaces()

        # All 3 should be counted as inaccessible but not invalid (Case 2)
        assert len(gc._issues["run"]) == 0
        assert len(gc._issues["inaccessible_runs"]) == 3
        assert gc._issues["inaccessible_runs"][0] == run1
        assert gc._issues["inaccessible_runs"][1] == run2
        assert gc._issues["inaccessible_runs"][2] == run3

        # Check that warning about 3 inaccessible workspaces was logged
        log_calls = mock_log.warning.call_args_list
        assert len(log_calls) > 0

        # Find the summary warning (should be the last one)
        summary_warning = str(log_calls[-1])
        assert "3" in summary_warning

    def test_check_with_no_runs(self, gc: DatabaseGarbageCollector, mock_db: MagicMock):
        """
        Test checking when there are no runs in the database.

        Args:
            gc: DatabaseGarbageCollector instance.
            mock_db: Mocked database instance.
        """
        mock_db.runs.get_all.return_value = []

        gc.check_run_workspaces()

        assert len(gc._issues["run"]) == 0
        assert len(gc._issues["inaccessible_runs"]) == 0


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
        assert "Inaccessible Runs: 0" in report

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
        mock_inaccessible_run = mock_run.copy()
        mock_inaccessible_run.get_workspace.return_value = "/path/to/inaccessible/workspace"
        gc._issues["inaccessible_runs"] = [mock_inaccessible_run]

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
        assert "Inaccessible Runs: 1" in report
        assert "/path/to/inaccessible/workspace" in report


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
        mocker.patch.object(gc, "_is_workspace_on_accessible_mount", return_value=True)
        gc.scan_and_clean(force=True)

        # Verify deletions occurred in correct order
        assert mock_db.delete.call_count == 4

        # Verify the deletion calls
        calls = [call[0] for call in mock_db.delete.call_args_list]
        assert ("run", "run-invalid") in calls
        assert ("physical_worker", "physical-orphaned") in calls
        assert ("logical_worker", "logical-orphaned") in calls
        assert ("study", "study-empty") in calls
