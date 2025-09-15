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

from merlin.workers.worker import MerlinWorker


class DummyMerlinWorker(MerlinWorker):
    def get_launch_command(self, override_args: str = "") -> str:
        return f"run_worker --name {self.name} {override_args}"

    def launch_worker(self):
        return f"Launching {self.name}"

    def get_metadata(self) -> dict:
        return {"name": self.name, "config": self.config}


def test_init_sets_attributes():
    """
    Test that the constructor sets name, config, and env correctly.
    """
    name = "test_worker"
    config = {"foo": "bar"}
    env = {"TEST_ENV": "123"}

    worker = DummyMerlinWorker(name, config, env)

    assert worker.name == name
    assert worker.config == config
    assert worker.env == env


def test_init_uses_os_environ_when_env_none(mocker: MockerFixture):
    """
    Test that os.environ is copied when no env is provided.

    Args:
        mocker: Pytest mocker fixture.
    """
    mock_environ = {"MY_VAR": "xyz"}
    mocker.patch.dict("os.environ", mock_environ, clear=True)

    worker = DummyMerlinWorker("w", {}, None)

    assert "MY_VAR" in worker.env
    assert worker.env["MY_VAR"] == "xyz"
    assert worker.env is not os.environ  # ensure it's a copy


def test_get_launch_command_returns_expected_string():
    """
    Test that get_launch_command builds the correct shell string.
    """
    worker = DummyMerlinWorker("dummy", {}, {})
    cmd = worker.get_launch_command("--debug")

    assert "--debug" in cmd
    assert "dummy" in cmd


def test_launch_worker_returns_expected_string():
    """
    Test that launch_worker returns a string indicating launch.
    """
    worker = DummyMerlinWorker("dummy", {}, {})
    result = worker.launch_worker()
    assert result == "Launching dummy"


def test_get_metadata_returns_expected_dict():
    """
    Test that get_metadata returns the correct metadata dictionary.
    """
    config = {"foo": "bar"}
    worker = DummyMerlinWorker("dummy", config, {})
    meta = worker.get_metadata()

    assert meta == {"name": "dummy", "config": config}
