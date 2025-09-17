##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/worker.py` module.
"""

import os

from pytest_mock import MockerFixture

from merlin.study.configurations import WorkerConfig
from merlin.workers.worker import MerlinWorker


class DummyMerlinWorker(MerlinWorker):
    def get_launch_command(self, override_args: str = "") -> str:
        return f"run_worker --name {self.worker_config.name} {override_args}"

    def start(self):
        return f"Launching {self.worker_config.name}"


def test_init_sets_attributes():
    """
    Test that the constructor sets name, config, and env correctly.
    """
    worker_config = WorkerConfig(name="test_worker", queues={"queue1", "queue2"}, env={"TEST_ENV": "123"})

    worker = DummyMerlinWorker(worker_config)

    assert worker.worker_config.name == worker_config.name
    assert worker.worker_config.queues == worker_config.queues
    assert worker.worker_config.env == worker_config.env


def test_init_uses_os_environ_when_env_none(mocker: MockerFixture):
    """
    Test that os.environ is copied when no env is provided.

    Args:
        mocker: Pytest mocker fixture.
    """
    mock_environ = {"MY_VAR": "xyz"}
    mocker.patch.dict("os.environ", mock_environ, clear=True)

    worker_config = WorkerConfig(name="test_worker")

    worker = DummyMerlinWorker(worker_config)

    assert "MY_VAR" in worker.worker_config.env
    assert worker.worker_config.env["MY_VAR"] == "xyz"
    assert worker.worker_config.env is not os.environ  # ensure it's a copy


def test_get_launch_command_returns_expected_string():
    """
    Test that get_launch_command builds the correct shell string.
    """
    worker_name = "test_worker"
    worker_config = WorkerConfig(name=worker_name)
    worker = DummyMerlinWorker(worker_config)
    cmd = worker.get_launch_command("--debug")

    assert "--debug" in cmd
    assert worker_name in cmd


def test_launch_worker_returns_expected_string():
    """
    Test that start returns a string indicating launch.
    """
    worker_name = "test_worker"
    worker_config = WorkerConfig(name=worker_name)
    worker = DummyMerlinWorker(worker_config)
    result = worker.start()
    assert result == f"Launching {worker_name}"


def test_get_metadata_returns_expected_dict():
    """
    Test that get_metadata returns the correct metadata dictionary.
    """
    worker_config = WorkerConfig(
        name="test_worker",
        args="-l INFO --concurrency 3",
        queues={"queue1"},
        nodes=10,
    )
    worker = DummyMerlinWorker(worker_config)
    meta = worker.get_metadata()

    assert WorkerConfig.from_dict(meta) == worker_config
