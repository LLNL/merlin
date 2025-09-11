##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/handlers/worker_handler.py` module.
"""

from typing import Any, Dict, List

import pytest

from merlin.study.configurations import WorkerConfig
from merlin.workers.handlers.worker_handler import MerlinWorkerHandler
from merlin.workers.worker import MerlinWorker


class DummyWorker(MerlinWorker):
    def get_launch_command(self, override_args: str = "") -> str:
        return "launch"

    def start(self) -> str:
        return "launched"

    def get_metadata(self) -> Dict:
        return {}


class DummyWorkerHandler(MerlinWorkerHandler):
    def __init__(self):
        super().__init__()
        self.started = False
        self.stopped = False
        self.queried = False

    def start_workers(self, workers: List[MerlinWorker], **kwargs):
        self.started = True
        self.last_workers = workers
        return [worker.start() for worker in workers]

    def stop_workers(self):
        self.stopped = True
        return "Stopped all workers"

    def query_workers(self) -> Any:
        self.queried = True
        return {"status": "ok", "workers": len(getattr(self, "last_workers", []))}


def test_abstract_handler_cannot_be_instantiated():
    """
    Test that attempting to instantiate the abstract base class raises a TypeError.
    """
    with pytest.raises(TypeError):
        MerlinWorkerHandler()


def test_unimplemented_methods_raise_not_implemented():
    """
    Test that calling abstract methods on a subclass without implementation raises NotImplementedError.
    """

    class IncompleteHandler(MerlinWorkerHandler):
        pass

    # Should raise TypeError due to unimplemented abstract methods
    with pytest.raises(TypeError):
        IncompleteHandler()


def test_launch_workers_calls_worker_launch():
    """
    Test that `start_workers` calls each worker's `start` method.
    """
    handler = DummyWorkerHandler()
    workers = [DummyWorker(WorkerConfig(name="w1")), DummyWorker(WorkerConfig(name="w2"))]

    result = handler.start_workers(workers)

    assert handler.started
    assert result == ["launched", "launched"]


def test_stop_workers_sets_flag():
    """
    Test that `stop_workers` sets the internal state and returns expected value.
    """
    handler = DummyWorkerHandler()
    response = handler.stop_workers()

    assert handler.stopped
    assert response == "Stopped all workers"


def test_query_workers_returns_summary():
    """
    Test that `query_workers` returns a valid summary of current worker state.
    """
    handler = DummyWorkerHandler()
    workers = [DummyWorker(WorkerConfig(name="a")), DummyWorker(WorkerConfig(name="b"))]
    handler.start_workers(workers)

    summary = handler.query_workers()

    assert handler.queried
    assert summary == {"status": "ok", "workers": 2}
