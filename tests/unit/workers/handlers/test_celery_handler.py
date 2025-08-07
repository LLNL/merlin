##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/handlers/celery_handler.py` module.
"""

import pytest
from typing import Dict, List

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

    def launch_worker(self, override_args: str = "", disable_logs: bool = False):
        self.launched_with = (override_args, disable_logs)
        return f"Launching {self.name} with {override_args} and logs {'off' if disable_logs else 'on'}"


class TestCeleryWorkerHandler:
    """
    Unit tests for the CeleryWorkerHandler class.
    """

    @pytest.fixture
    def handler(self) -> CeleryWorkerHandler:
        return CeleryWorkerHandler()

    @pytest.fixture
    def workers(self) -> List[DummyCeleryWorker]:
        return [
            DummyCeleryWorker("worker1"),
            DummyCeleryWorker("worker2"),
        ]

    def test_echo_only_prints_commands(self, handler: CeleryWorkerHandler, workers: List[DummyCeleryWorker], capsys: pytest.CaptureFixture):
        """
        Test that `launch_workers` prints launch commands when `echo_only=True`.

        Args:
            handler: CeleryWorkerHandler instance.
            workers: DummyCeleryWorker instances.
            capsys: Pytest fixture to capture stdout.
        """
        handler.launch_workers(workers, echo_only=True, override_args="--debug", disable_logs=True)
        output = capsys.readouterr().out

        for worker in workers:
            expected = worker.get_launch_command(override_args="--debug", disable_logs=True)
            assert expected in output

    def test_launch_workers_calls_worker_launch(self, handler: CeleryWorkerHandler, workers: List[DummyCeleryWorker]):
        """
        Test that `launch_workers` invokes `launch_worker()` on each worker when `echo_only=False`.

        Args:
            handler: CeleryWorkerHandler instance.
            workers: DummyCeleryWorker instances.
        """
        handler.launch_workers(workers, echo_only=False, override_args="--custom", disable_logs=True)

        for worker in workers:
            assert worker.launched_with == ("--custom", True)

    def test_default_kwargs_are_used(self, handler: CeleryWorkerHandler, workers: List[DummyCeleryWorker]):
        """
        Test that `launch_workers` uses defaults when optional kwargs are omitted.

        Args:
            handler: CeleryWorkerHandler instance.
            workers: DummyCeleryWorker instances.
        """
        handler.launch_workers(workers)

        for worker in workers:
            assert worker.launched_with == ("", False)
