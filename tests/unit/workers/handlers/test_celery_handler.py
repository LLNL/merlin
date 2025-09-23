##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/handlers/celery_handler.py` module.
"""

from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.workers.celery_worker import CeleryWorker
from merlin.workers.handlers import CeleryWorkerHandler


class DummyCeleryWorker(CeleryWorker):
    def __init__(self, name: str, config: Dict = None, env: Dict = None):
        super().__init__(name, config or {}, env or {})
        self.launched_with = None
        self.launch_command = f"celery --worker-name={name}"

    def get_launch_command(self, override_args: str = "", disable_logs: bool = False) -> str:
        parts = [self.launch_command]
        if override_args:
            parts.append(override_args)
        if disable_logs:
            parts.append("--no-logs")
        return " ".join(parts)

    def start(self, override_args: str = "", disable_logs: bool = False):
        self.launched_with = (override_args, disable_logs)
        return f"Launching {self.name} with {override_args} and logs {'off' if disable_logs else 'on'}"


class TestCeleryWorkerHandler:
    """
    Unit tests for the CeleryWorkerHandler class.
    """

    @pytest.fixture
    def handler(self, mocker: MockerFixture) -> CeleryWorkerHandler:
        """
        Create a CeleryWorkerHandler instance with mocked database.

        Args:
            mocker: Pytest mocker fixture.

        Returns:
            CeleryWorkerHandler instance with mocked dependencies.
        """
        mocker.patch("merlin.workers.handlers.celery_handler.MerlinDatabase")
        return CeleryWorkerHandler()

    @pytest.fixture
    def mock_db(self, mocker: MockerFixture) -> MagicMock:
        """
        Mock the MerlinDatabase used in CeleryWorker constructor.

        Args:
            mocker: Pytest mocker fixture.

        Returns:
            A mocked MerlinDatabase instance.
        """
        return mocker.patch("merlin.workers.celery_worker.MerlinDatabase")

    @pytest.fixture
    def workers(self, mock_db: MagicMock) -> List[DummyCeleryWorker]:
        """
        Create a list of dummy CeleryWorker instances for testing.

        Args:
            mock_db: Mocked MerlinDatabase instance.

        Returns:
            List of DummyCeleryWorker instances.
        """
        return [
            DummyCeleryWorker("worker1"),
            DummyCeleryWorker("worker2"),
        ]
    
    @pytest.fixture
    def mock_logical_workers(self) -> List[MagicMock]:
        """
        Create mock logical worker entities for testing query_workers.

        Returns:
            List of mock logical worker entities.
        """
        worker1 = MagicMock()
        worker1.get_name.return_value = "logical_worker1"
        worker1.get_queues.return_value = ["[merlin]_queue1", "[merlin]_queue2"]
        
        worker2 = MagicMock()
        worker2.get_name.return_value = "logical_worker2"
        worker2.get_queues.return_value = ["[merlin]_queue3"]
        
        return [worker1, worker2]

    @pytest.fixture
    def mock_formatter(self, mocker: MockerFixture) -> MagicMock:
        """
        Mock the worker formatter factory and formatter.

        Args:
            mocker: Pytest mocker fixture.

        Returns:
            Mock formatter instance.
        """
        mock_formatter = MagicMock()
        mocker.patch(
            "merlin.workers.handlers.celery_handler.worker_formatter_factory.create",
            return_value=mock_formatter
        )
        return mock_formatter

    def test_echo_only_prints_commands(
        self, handler: CeleryWorkerHandler, workers: List[DummyCeleryWorker], capsys: pytest.CaptureFixture
    ):
        """
        Test that `start_workers` prints launch commands when `echo_only=True`.

        Args:
            handler: CeleryWorkerHandler instance.
            workers: DummyCeleryWorker instances.
            capsys: Pytest fixture to capture stdout.
        """
        handler.start_workers(workers, echo_only=True, override_args="--debug", disable_logs=True)
        output = capsys.readouterr().out

        for worker in workers:
            expected = worker.get_launch_command(override_args="--debug", disable_logs=True)
            assert expected in output

    def test_launch_workers_calls_worker_launch(self, handler: CeleryWorkerHandler, workers: List[DummyCeleryWorker]):
        """
        Test that `start_workers` invokes `start()` on each worker when `echo_only=False`.

        Args:
            handler: CeleryWorkerHandler instance.
            workers: DummyCeleryWorker instances.
        """
        handler.start_workers(workers, echo_only=False, override_args="--custom", disable_logs=True)

        for worker in workers:
            assert worker.launched_with == ("--custom", True)

    def test_default_kwargs_are_used(self, handler: CeleryWorkerHandler, workers: List[DummyCeleryWorker]):
        """
        Test that `start_workers` uses defaults when optional kwargs are omitted.

        Args:
            handler: CeleryWorkerHandler instance.
            workers: DummyCeleryWorker instances.
        """
        handler.start_workers(workers)

        for worker in workers:
            assert worker.launched_with == ("", False)

    def test_build_filters_with_queues_and_workers(self, handler: CeleryWorkerHandler):
        """
        Test that `_build_filters` correctly constructs filters dictionary.

        Args:
            handler: CeleryWorkerHandler instance.
        """
        queues = ["queue1", "queue2"]
        workers = ["worker1", "worker2"]
        
        filters = handler._build_filters(queues, workers)
        
        assert filters == {
            "queues": ["queue1", "queue2"],
            "name": ["worker1", "worker2"]
        }

    def test_build_filters_with_only_queues(self, handler: CeleryWorkerHandler):
        """
        Test that `_build_filters` handles only queues parameter.

        Args:
            handler: CeleryWorkerHandler instance.
        """
        queues = ["queue1"]
        
        filters = handler._build_filters(queues, None)
        
        assert filters == {"queues": ["queue1"]}

    def test_build_filters_with_only_workers(self, handler: CeleryWorkerHandler):
        """
        Test that `_build_filters` handles only workers parameter.

        Args:
            handler: CeleryWorkerHandler instance.
        """
        workers = ["worker1"]
        
        filters = handler._build_filters(None, workers)
        
        assert filters == {"name": ["worker1"]}

    def test_build_filters_with_no_parameters(self, handler: CeleryWorkerHandler):
        """
        Test that `_build_filters` returns empty dict when no parameters provided.

        Args:
            handler: CeleryWorkerHandler instance.
        """
        filters = handler._build_filters(None, None)
        
        assert filters == {}

    def test_query_workers_calls_database_and_formatter(
        self, 
        handler: CeleryWorkerHandler, 
        mock_logical_workers: List[MagicMock],
        mock_formatter: MagicMock
    ):
        """
        Test that `query_workers` retrieves data from database and calls formatter.

        Args:
            handler: CeleryWorkerHandler instance.
            mock_logical_workers: Mock logical worker entities.
            mock_formatter: Mock formatter instance.
        """
        # Mock the database get_all method
        handler.merlin_db.get_all.return_value = mock_logical_workers
        
        handler.query_workers("rich", queues=["queue1"], workers=["worker1"])
        
        # Verify database was called with correct filters
        expected_filters = {"queues": ["queue1"], "name": ["worker1"]}
        handler.merlin_db.get_all.assert_called_once_with("logical_worker", filters=expected_filters)
        
        # Verify formatter was created and called
        mock_formatter.format_and_display.assert_called_once_with(
            mock_logical_workers, expected_filters, handler.merlin_db
        )

    def test_query_workers_with_no_filters(
        self,
        handler: CeleryWorkerHandler,
        mock_logical_workers: List[MagicMock],
        mock_formatter: MagicMock
    ):
        """
        Test that `query_workers` works correctly when no filters are provided.

        Args:
            handler: CeleryWorkerHandler instance.
            mock_logical_workers: Mock logical worker entities.
            mock_formatter: Mock formatter instance.
        """
        handler.merlin_db.get_all.return_value = mock_logical_workers
        
        handler.query_workers("json")
        
        # Verify database was called with empty filters
        handler.merlin_db.get_all.assert_called_once_with("logical_worker", filters={})
        
        # Verify formatter was called correctly
        mock_formatter.format_and_display.assert_called_once_with(
            mock_logical_workers, {}, handler.merlin_db
        )

    def test_query_workers_uses_correct_formatter(
        self,
        handler: CeleryWorkerHandler,
        mock_logical_workers: List[MagicMock],
        mocker: MockerFixture
    ):
        """
        Test that `query_workers` uses the correct formatter type.

        Args:
            handler: CeleryWorkerHandler instance.
            mock_logical_workers: Mock logical worker entities.
            mocker: Pytest mocker fixture.
        """
        handler.merlin_db.get_all.return_value = mock_logical_workers
        
        mock_factory = mocker.patch("merlin.workers.handlers.celery_handler.worker_formatter_factory")
        mock_formatter = MagicMock()
        mock_factory.create.return_value = mock_formatter
        
        handler.query_workers("json", queues=["test_queue"])
        
        # Verify the correct formatter type was requested
        mock_factory.create.assert_called_once_with("json")
        mock_formatter.format_and_display.assert_called_once()

    def test_query_workers_handles_empty_results(
        self,
        handler: CeleryWorkerHandler,
        mock_formatter: MagicMock
    ):
        """
        Test that `query_workers` handles empty database results gracefully.

        Args:
            handler: CeleryWorkerHandler instance.
            mock_formatter: Mock formatter instance.
        """
        handler.merlin_db.get_all.return_value = []
        
        handler.query_workers("rich")
        
        # Verify formatter is still called with empty list
        mock_formatter.format_and_display.assert_called_once_with([], {}, handler.merlin_db)

    def test_query_workers_passes_all_parameters_to_formatter(
        self,
        handler: CeleryWorkerHandler,
        mock_logical_workers: List[MagicMock],
        mock_formatter: MagicMock
    ):
        """
        Test that `query_workers` passes all necessary parameters to formatter.

        Args:
            handler: CeleryWorkerHandler instance.
            mock_logical_workers: Mock logical worker entities.
            mock_formatter: Mock formatter instance.
        """
        handler.merlin_db.get_all.return_value = mock_logical_workers
        
        queues = ["queue1", "queue2"]
        workers = ["worker1"]
        
        handler.query_workers("rich", queues=queues, workers=workers)
        
        expected_filters = {"queues": queues, "name": workers}
        
        # Verify all parameters are passed correctly
        mock_formatter.format_and_display.assert_called_once_with(
            mock_logical_workers,
            expected_filters,
            handler.merlin_db
        )
