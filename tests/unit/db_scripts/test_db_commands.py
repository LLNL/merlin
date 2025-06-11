##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `db_commands.py` module.
"""

import logging
from argparse import Namespace
from unittest.mock import call

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

# Import the functions being tested
from merlin.db_scripts.db_commands import database_delete, database_get, database_info


class TestDatabaseInfo:
    """Tests for the database_info function."""

    def test_database_info(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test that database_info prints the correct information.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its methods
        mock_db = mocker.MagicMock()
        mock_db.get_db_type.return_value = "SQLite"
        mock_db.get_db_version.return_value = "1.0.0"
        mock_db.get_connection_string.return_value = "sqlite:///merlin.db"
        mock_db.get_all.side_effect = [
            ["study1", "study2"],  # studies
            ["run1", "run2", "run3"],  # runs
            ["worker1"],  # logical workers
            ["worker1", "worker2"],  # physical workers
        ]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Call the function
        database_info()

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Merlin Database Information" in captured.out
        assert "Database Type: SQLite" in captured.out
        assert "Database Version: 1.0.0" in captured.out
        assert "Connection String: sqlite:///merlin.db" in captured.out
        assert "Studies:" in captured.out
        assert "Total: 2" in captured.out
        assert "Runs:" in captured.out
        assert "Total: 3" in captured.out
        assert "Logical Workers:" in captured.out
        assert "Total: 1" in captured.out
        assert "Physical Workers:" in captured.out
        assert "Total: 2" in captured.out

        # Verify get_all was called with the correct entity types
        mock_db.get_all.assert_has_calls([call("study"), call("run"), call("logical_worker"), call("physical_worker")])


class TestDatabaseGet:
    """Tests for the database_get function."""

    def test_get_study(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting studies by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get method
        mock_db = mocker.MagicMock()
        mock_study1 = mocker.MagicMock()
        mock_study1.__str__.return_value = "Study 1"
        mock_study2 = mocker.MagicMock()
        mock_study2.__str__.return_value = "Study 2"
        mock_db.get.side_effect = [mock_study1, mock_study2]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with study IDs
        args = Namespace(get_type="study", study=["study1", "study2"])

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Study 1" in captured.out
        assert "Study 2" in captured.out

        # Verify the get method was called with the correct arguments
        mock_db.get.assert_has_calls([call("study", "study1"), call("study", "study2")])

    def test_get_run(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting runs by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get method
        mock_db = mocker.MagicMock()
        mock_run = mocker.MagicMock()
        mock_run.__str__.return_value = "Run 1"
        mock_db.get.return_value = mock_run

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with run IDs
        args = Namespace(get_type="run", run=["run1"])

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Run 1" in captured.out

        # Verify the get method was called with the correct arguments
        mock_db.get.assert_called_once_with("run", "run1")

    def test_get_logical_worker(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting logical workers by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get method
        mock_db = mocker.MagicMock()
        mock_worker = mocker.MagicMock()
        mock_worker.__str__.return_value = "Logical Worker 1"
        mock_db.get.return_value = mock_worker

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with worker IDs
        args = Namespace(get_type="logical-worker", worker=["worker1"])

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Logical Worker 1" in captured.out

        # Verify the get method was called with the correct arguments
        mock_db.get.assert_called_once_with("logical_worker", "worker1")

    def test_get_physical_worker(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting physical workers by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get method
        mock_db = mocker.MagicMock()
        mock_worker = mocker.MagicMock()
        mock_worker.__str__.return_value = "Physical Worker 1"
        mock_db.get.return_value = mock_worker

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with worker IDs
        args = Namespace(get_type="physical-worker", worker=["worker1"])

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Physical Worker 1" in captured.out

        # Verify the get method was called with the correct arguments
        mock_db.get.assert_called_once_with("physical_worker", "worker1")

    def test_get_all_studies(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting all studies.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get_all method
        mock_db = mocker.MagicMock()
        mock_study1 = mocker.MagicMock()
        mock_study1.__str__.return_value = "Study 1"
        mock_study2 = mocker.MagicMock()
        mock_study2.__str__.return_value = "Study 2"
        mock_db.get_all.return_value = [mock_study1, mock_study2]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(get_type="all-studies")

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Study 1" in captured.out
        assert "Study 2" in captured.out

        # Verify the get_all method was called with the correct entity type
        mock_db.get_all.assert_called_once_with("study")

    def test_get_all_runs(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting all runs.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get_all method
        mock_db = mocker.MagicMock()
        mock_run1 = mocker.MagicMock()
        mock_run1.__str__.return_value = "Run 1"
        mock_run2 = mocker.MagicMock()
        mock_run2.__str__.return_value = "Run 2"
        mock_db.get_all.return_value = [mock_run1, mock_run2]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(get_type="all-runs")

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Run 1" in captured.out
        assert "Run 2" in captured.out

        # Verify the get_all method was called with the correct entity type
        mock_db.get_all.assert_called_once_with("run")

    def test_get_all_logical_workers(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting all logical workers.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get_all method
        mock_db = mocker.MagicMock()
        mock_worker = mocker.MagicMock()
        mock_worker.__str__.return_value = "Logical Worker 1"
        mock_db.get_all.return_value = [mock_worker]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(get_type="all-logical-workers")

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Logical Worker 1" in captured.out

        # Verify the get_all method was called with the correct entity type
        mock_db.get_all.assert_called_once_with("logical_worker")

    def test_get_all_physical_workers(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting all physical workers.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get_all method
        mock_db = mocker.MagicMock()
        mock_worker = mocker.MagicMock()
        mock_worker.__str__.return_value = "Physical Worker 1"
        mock_db.get_all.return_value = [mock_worker]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(get_type="all-physical-workers")

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Physical Worker 1" in captured.out

        # Verify the get_all method was called with the correct entity type
        mock_db.get_all.assert_called_once_with("physical_worker")

    def test_get_everything(self, mocker: MockerFixture, capsys: CaptureFixture):
        """
        Test getting everything from the database.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            capsys: A built-in fixture from the pytest library to capture stdout and stderr.
        """
        # Mock MerlinDatabase and its get_everything method
        mock_db = mocker.MagicMock()
        mock_entity = mocker.MagicMock()
        mock_entity.__str__.return_value = "Database Entity"
        mock_db.get_everything.return_value = [mock_entity]

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(get_type="everything")

        # Call the function
        database_get(args)

        # Capture the printed output
        captured = capsys.readouterr()

        # Verify the output contains the expected information
        assert "Database Entity" in captured.out

        # Verify the get_everything method was called
        mock_db.get_everything.assert_called_once()

    def test_get_empty_studies(self, mocker: MockerFixture, caplog: CaptureFixture):
        """
        Test getting studies when none are found.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        caplog.set_level(logging.INFO)

        # Mock MerlinDatabase and its get_all method
        mock_db = mocker.MagicMock()
        mock_db.get_all.return_value = []

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(get_type="all-studies")

        # Call the function
        database_get(args)

        # Verify LOG was called with the correct message
        assert "No studies found in the database." in caplog.text

    def test_get_invalid_option(self, mocker: MockerFixture, caplog: CaptureFixture):
        """
        Test providing an invalid get option.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        # Mock MerlinDatabase
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with invalid get_type
        args = Namespace(get_type="invalid-option")

        # Call the function
        database_get(args)

        # Verify LOG was called with the correct message
        assert "No valid get option provided." in caplog.text


class TestDatabaseDelete:
    """Tests for the database_delete function."""

    def test_delete_study(self, mocker: MockerFixture):
        """
        Test deleting studies by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with study IDs
        args = Namespace(delete_type="study", study=["study1", "study2"], keep_associated_runs=False)

        # Call the function
        database_delete(args)

        # Verify the delete method was called with the correct arguments
        mock_db.delete.assert_has_calls(
            [call("study", "study1", remove_associated_runs=True), call("study", "study2", remove_associated_runs=True)]
        )

    def test_delete_study_keep_runs(self, mocker: MockerFixture):
        """
        Test deleting studies by ID while keeping associated runs.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with study IDs and keep_associated_runs=True
        args = Namespace(delete_type="study", study=["study1"], keep_associated_runs=True)

        # Call the function
        database_delete(args)

        # Verify the delete method was called with the correct arguments
        mock_db.delete.assert_called_once_with("study", "study1", remove_associated_runs=False)

    def test_delete_run(self, mocker: MockerFixture):
        """
        Test deleting runs by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with run IDs
        args = Namespace(delete_type="run", run=["run1", "run2"])

        # Call the function
        database_delete(args)

        # Verify the delete method was called with the correct arguments
        mock_db.delete.assert_has_calls([call("run", "run1"), call("run", "run2")])

    def test_delete_logical_worker(self, mocker: MockerFixture):
        """
        Test deleting logical workers by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with worker IDs
        args = Namespace(delete_type="logical-worker", worker=["worker1"])

        # Call the function
        database_delete(args)

        # Verify the delete method was called with the correct arguments
        mock_db.delete.assert_called_once_with("logical_worker", "worker1")

    def test_delete_physical_worker(self, mocker: MockerFixture):
        """
        Test deleting physical workers by ID.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with worker IDs
        args = Namespace(delete_type="physical-worker", worker=["worker1"])

        # Call the function
        database_delete(args)

        # Verify the delete method was called with the correct arguments
        mock_db.delete.assert_called_once_with("physical_worker", "worker1")

    def test_delete_all_studies(self, mocker: MockerFixture):
        """
        Test deleting all studies.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete_all method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(delete_type="all-studies", keep_associated_runs=False)

        # Call the function
        database_delete(args)

        # Verify the delete_all method was called with the correct arguments
        mock_db.delete_all.assert_called_once_with("study", remove_associated_runs=True)

    def test_delete_all_runs(self, mocker: MockerFixture):
        """
        Test deleting all runs.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete_all method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(delete_type="all-runs")

        # Call the function
        database_delete(args)

        # Verify the delete_all method was called with the correct entity type
        mock_db.delete_all.assert_called_once_with("run")

    def test_delete_all_logical_workers(self, mocker: MockerFixture):
        """
        Test deleting all logical workers.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete_all method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(delete_type="all-logical-workers")

        # Call the function
        database_delete(args)

        # Verify the delete_all method was called with the correct entity type
        mock_db.delete_all.assert_called_once_with("logical_worker")

    def test_delete_all_physical_workers(self, mocker: MockerFixture):
        """
        Test deleting all physical workers.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete_all method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(delete_type="all-physical-workers")

        # Call the function
        database_delete(args)

        # Verify the delete_all method was called with the correct entity type
        mock_db.delete_all.assert_called_once_with("physical_worker")

    def test_delete_everything(self, mocker: MockerFixture):
        """
        Test deleting everything from the database.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
        """
        # Mock MerlinDatabase and its delete_everything method
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args
        args = Namespace(delete_type="everything", force=True)

        # Call the function
        database_delete(args)

        # Verify the delete_everything method was called with the correct arguments
        mock_db.delete_everything.assert_called_once_with(force=True)

    def test_delete_invalid_option(self, mocker: MockerFixture, caplog: CaptureFixture):
        """
        Test providing an invalid delete option.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        # Mock MerlinDatabase
        mock_db = mocker.MagicMock()

        # Patch the MerlinDatabase class
        mocker.patch("merlin.db_scripts.db_commands.MerlinDatabase", return_value=mock_db)

        # Create args with invalid delete_type
        args = Namespace(delete_type="invalid-option")

        # Call the function
        database_delete(args)

        # Verify LOG.error was called with the correct message
        assert "No valid delete option provided." in caplog.text
