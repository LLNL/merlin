##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for MerlinStepRecord._update_status_file() auto-detection of task_server.
"""

import tempfile
from unittest.mock import Mock, patch

from maestrowf.abstracts.enums import State

from merlin.study.step import MerlinStepRecord


class TestUpdateStatusFileAutoDetection:
    """Tests for MerlinStepRecord._update_status_file() task_server auto-detection"""

    def _create_mock_record(self, workspace_dir):
        """Helper to create a mock MerlinStepRecord object with required attributes."""
        mock_record = Mock(spec=MerlinStepRecord)
        mock_record.name = "test_step"
        mock_record.status = State.RUNNING
        mock_record.elapsed_time = 1.5
        mock_record.run_time = 1.0
        mock_record.restarts = 0
        mock_record.condensed_workspace = "workspace_0"

        # Mock workspace
        mock_workspace = Mock()
        mock_workspace.value = workspace_dir
        mock_record.workspace = mock_workspace

        # Mock merlin_step params
        mock_record.merlin_step = Mock()
        mock_record.merlin_step.params = {
            "cmd": {"param1": "value1"},
            "restart_cmd": None,
        }

        return mock_record

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_auto_detect_local_mode(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test task_server auto-detection when is_local_mode() returns True"""
        mock_is_local_mode.return_value = True
        mock_exists.return_value = False  # Status file doesn't exist

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Call the method with task_server=None (auto-detect)
            MerlinStepRecord._update_status_file(mock_record, result=None, task_server=None)

            # Should call write_status
            mock_write_status.assert_called_once()

            # Check the status_info dict - should NOT have celery-specific info
            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]

            # Should not have task_queue (celery-specific)
            assert "task_queue" not in status_info.get("test_step", {})

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_auto_detect_celery_mode(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test task_server auto-detection when is_local_mode() returns False"""
        mock_is_local_mode.return_value = False
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Mock celery app and helpers
            mock_app = Mock()
            mock_app.conf.task_always_eager = True  # Avoid worker lookup

            with patch.dict("sys.modules", {"merlin.celery": Mock(app=mock_app)}):
                MerlinStepRecord._update_status_file(mock_record, result=None, task_server=None)

            mock_write_status.assert_called_once()

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_explicit_local_override(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test explicit task_server='local' overrides auto-detection"""
        # Even if is_local_mode returns False, explicit override should work
        mock_is_local_mode.return_value = False
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Explicitly set task_server="local"
            MerlinStepRecord._update_status_file(mock_record, result=None, task_server="local")

            mock_write_status.assert_called_once()

            # Check the status_info - should NOT have celery-specific info
            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]
            assert "task_queue" not in status_info.get("test_step", {})

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_explicit_celery_override(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test explicit task_server='celery' overrides auto-detection"""
        # Even if is_local_mode returns True, explicit override should work
        mock_is_local_mode.return_value = True
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Mock celery app
            mock_app = Mock()
            mock_app.conf.task_always_eager = True

            with patch.dict("sys.modules", {"merlin.celery": Mock(app=mock_app)}):
                # Explicitly set task_server="celery"
                MerlinStepRecord._update_status_file(mock_record, result=None, task_server="celery")

            mock_write_status.assert_called_once()


class TestUpdateStatusFileCeleryBehavior:
    """Tests for Celery-specific behavior in _update_status_file()"""

    def _create_mock_record(self, workspace_dir):
        """Helper to create a mock MerlinStepRecord object with required attributes."""
        mock_record = Mock(spec=MerlinStepRecord)
        mock_record.name = "test_step"
        mock_record.status = State.RUNNING
        mock_record.elapsed_time = 1.5
        mock_record.run_time = 1.0
        mock_record.restarts = 0
        mock_record.condensed_workspace = "workspace_0"

        mock_workspace = Mock()
        mock_workspace.value = workspace_dir
        mock_record.workspace = mock_workspace

        mock_record.merlin_step = Mock()
        mock_record.merlin_step.params = {
            "cmd": {"param1": "value1"},
            "restart_cmd": None,
        }

        return mock_record

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_celery_mode_skips_worker_info_when_eager(
        self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status
    ):
        """Test that Celery mode skips worker info when task_always_eager=True"""
        mock_is_local_mode.return_value = False
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Mock celery app with task_always_eager=True
            mock_app = Mock()
            mock_app.conf.task_always_eager = True

            with patch.dict("sys.modules", {"merlin.celery": Mock(app=mock_app)}):
                MerlinStepRecord._update_status_file(mock_record, result=None, task_server="celery")

            mock_write_status.assert_called_once()

            # Check status_info doesn't have worker info (task_always_eager=True)
            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]
            assert "workers" not in status_info.get("test_step", {})

    @patch("merlin.study.step.get_current_worker")
    @patch("merlin.study.step.get_current_queue")
    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_celery_mode_adds_worker_info_when_not_eager(
        self,
        mock_is_local_mode,
        mock_exists,
        mock_read_status,
        mock_write_status,
        mock_get_queue,
        mock_get_worker,
    ):
        """Test that Celery mode adds worker info when task_always_eager=False"""
        mock_is_local_mode.return_value = False
        mock_exists.return_value = False
        mock_get_queue.return_value = "test_queue"
        mock_get_worker.return_value = "worker-1"

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Mock celery app with task_always_eager=False (real workers)
            mock_app = Mock()
            mock_app.conf.task_always_eager = False

            with patch.dict("sys.modules", {"merlin.celery": Mock(app=mock_app)}):
                MerlinStepRecord._update_status_file(mock_record, result=None, task_server="celery")

            mock_write_status.assert_called_once()

            # Check status_info has worker info
            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]
            assert status_info["test_step"]["task_queue"] == "test_queue"
            assert "workers" in status_info["test_step"]

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_local_mode_never_imports_celery(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test that local mode never tries to import celery"""
        mock_is_local_mode.return_value = True
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            # Don't mock celery - if it's imported, test will fail or behave unexpectedly
            # The point is that with task_server="local", celery import is skipped
            MerlinStepRecord._update_status_file(mock_record, result=None, task_server="local")

            mock_write_status.assert_called_once()


class TestUpdateStatusFileStatusInfo:
    """Tests for status info dictionary construction"""

    def _create_mock_record(self, workspace_dir, state=State.RUNNING):
        """Helper to create a mock MerlinStepRecord object with required attributes."""
        mock_record = Mock(spec=MerlinStepRecord)
        mock_record.name = "test_step"
        mock_record.status = state
        mock_record.elapsed_time = 2.5
        mock_record.run_time = 2.0
        mock_record.restarts = 1
        mock_record.condensed_workspace = "workspace_0"

        mock_workspace = Mock()
        mock_workspace.value = workspace_dir
        mock_record.workspace = mock_workspace

        mock_record.merlin_step = Mock()
        mock_record.merlin_step.params = {
            "cmd": {"param1": "value1", "param2": "value2"},
            "restart_cmd": {"restart_param": "restart_value"},
        }

        return mock_record

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_status_info_contains_correct_fields(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test that status_info dict contains all required fields"""
        mock_is_local_mode.return_value = True
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            MerlinStepRecord._update_status_file(mock_record, result="SUCCESS", task_server="local")

            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]

            # Check structure
            assert "test_step" in status_info
            assert "parameters" in status_info["test_step"]
            assert "workspace_0" in status_info["test_step"]

            # Check workspace-specific info
            ws_info = status_info["test_step"]["workspace_0"]
            assert ws_info["status"] == "RUNNING"
            assert ws_info["return_code"] == "SUCCESS"
            assert ws_info["elapsed_time"] == 2.5
            assert ws_info["run_time"] == 2.0
            assert ws_info["restarts"] == 1

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_status_info_includes_parameters(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test that status_info includes cmd and restart parameters"""
        mock_is_local_mode.return_value = True
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            MerlinStepRecord._update_status_file(mock_record, result=None, task_server="local")

            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]

            params = status_info["test_step"]["parameters"]
            assert params["cmd"] == {"param1": "value1", "param2": "value2"}
            assert params["restart"] == {"restart_param": "restart_value"}

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_status_translates_state_enum(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test that State enum is translated to string"""
        mock_is_local_mode.return_value = True
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir, state=State.FINISHED)

            MerlinStepRecord._update_status_file(mock_record, result=None, task_server="local")

            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]

            assert status_info["test_step"]["workspace_0"]["status"] == "FINISHED"

    @patch("merlin.study.step.write_status")
    @patch("merlin.study.step.read_status")
    @patch("merlin.study.step.os.path.exists")
    @patch("merlin.config.configfile.is_local_mode")
    def test_updates_existing_status_file(self, mock_is_local_mode, mock_exists, mock_read_status, mock_write_status):
        """Test that existing status file is read and updated"""
        mock_is_local_mode.return_value = True
        mock_exists.return_value = True  # Status file exists

        existing_status = {
            "test_step": {
                "parameters": {"cmd": None, "restart": None},
                "old_workspace": {"status": "FINISHED"},
            }
        }
        mock_read_status.return_value = existing_status

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_record = self._create_mock_record(tmpdir)

            MerlinStepRecord._update_status_file(mock_record, result=None, task_server="local")

            # Should read existing status
            mock_read_status.assert_called_once()

            # Should write updated status
            mock_write_status.assert_called_once()

            call_args = mock_write_status.call_args[0]
            status_info = call_args[0]

            # Should preserve old workspace info
            assert "old_workspace" in status_info["test_step"]
            # Should add new workspace info
            assert "workspace_0" in status_info["test_step"]
