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

from merlin.study.configurations import WorkerConfig
from merlin.workers.celery_worker import CeleryWorker
from merlin.workers.handlers import CeleryWorkerHandler


class DummyCeleryWorker(CeleryWorker):
    def __init__(self, worker_config: WorkerConfig):
        super().__init__(worker_config)
        self.launched_with = None
        self.launch_command = f"celery --worker-name={self.worker_config.name}"

    def get_launch_command(self, override_args: str = "", disable_logs: bool = False) -> str:
        parts = [self.launch_command]
        if override_args:
            parts.append(override_args)
        if disable_logs:
            parts.append("--no-logs")
        return " ".join(parts)

    def start(self, override_args: str = "", disable_logs: bool = False):
        self.launched_with = (override_args, disable_logs)
        return f"Launching {self.worker_config.name} with {override_args} and logs {'off' if disable_logs else 'on'}"


class TestCeleryWorkerHandler:
    """
    Unit tests for the CeleryWorkerHandler class.
    """

    @pytest.fixture
    def handler(self) -> CeleryWorkerHandler:
        return CeleryWorkerHandler()

    @pytest.fixture
    def mock_db(self, mocker: MockerFixture) -> MagicMock:
        return mocker.patch("merlin.workers.celery_worker.MerlinDatabase")

    @pytest.fixture
    def workers(self, mock_db: MagicMock) -> List[DummyCeleryWorker]:
        worker_1_config = WorkerConfig(name="worker1")
        worker_2_config = WorkerConfig(name="worker2")
        return [
            DummyCeleryWorker(worker_1_config),
            DummyCeleryWorker(worker_2_config),
        ]

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
