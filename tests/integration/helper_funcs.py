##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains helper functions for the integration
test suite.
"""

import os
import re
import shutil
import subprocess
from time import sleep
from typing import Dict, List

from merlin.spec.expansion import get_spec_with_expansion
from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_types import FixtureRedis
from tests.integration.conditions import Condition


def load_workers_from_spec(spec_filepath: str) -> dict:
    """
    Load worker specifications from a YAML file.

    This function reads a YAML file containing study specifications and
    extracts the worker information under the "merlin" section. It
    constructs a dictionary in the form that
    [`CeleryWorkersManager.launch_workers`][context_managers.celery_workers_manager.CeleryWorkersManager.launch_workers]
    requires.

    Args:
        spec_filepath: The file path to the YAML specification file.

    Returns:
        A dictionary containing the worker specifications from the
            "merlin" section of the YAML file.
    """
    worker_info = {}
    spec = get_spec_with_expansion(spec_filepath)
    steps_and_queues = spec.get_task_queues(omit_tag=True)

    for worker_name, worker_settings in spec.merlin["resources"]["workers"].items():
        match = re.search(r"--concurrency\s+(\d+)", worker_settings["args"])
        concurrency = int(match.group(1)) if match else 1
        worker_info[worker_name] = {"concurrency": concurrency}
        if worker_settings["steps"] == ["all"]:
            worker_info[worker_name]["queues"] = list(steps_and_queues.values())
        else:
            worker_info[worker_name]["queues"] = [steps_and_queues[step] for step in worker_settings["steps"]]

    return worker_info


def copy_app_yaml_to_cwd(merlin_server_dir: str):
    """
    Copy the app.yaml file from the directory provided to the current working
    directory.

    Grab the app.yaml file from `merlin_server_dir` and copy it to the current
    working directory so that Merlin will read this in as the server configuration
    for whatever test is calling this.

    Args:
        merlin_server_dir: The path to the `merlin_server` directory that should be created by the
            [`redis_server`][conftest.redis_server] fixture.
    """
    copied_app_yaml = os.path.join(os.getcwd(), "app.yaml")
    if not os.path.exists(copied_app_yaml):
        server_app_yaml = os.path.join(merlin_server_dir, "app.yaml")
        shutil.copy(server_app_yaml, copied_app_yaml)


def check_test_conditions(conditions: List[Condition], info: Dict[str, str]):
    """
    Ensure all specified test conditions are satisfied based on the output
    from a subprocess.

    This function iterates through a list of [`Condition`][integration.conditions.Condition]
    instances, ingests the provided information (stdout, stderr, and return
    code) for each condition, and checks if each condition passes. If any
    condition fails, an AssertionError is raised with a detailed message that
    includes the condition that failed, along with the captured output and
    return code.

    Args:
        conditions: A list of Condition instances that define the expectations for the test.
        info: A dictionary containing the output from the subprocess, which should
            include the following keys:\n
            - 'stdout': The standard output captured from the subprocess.
            - 'stderr': The standard error output captured from the subprocess.
            - 'return_code': The return code of the subprocess, indicating success
              or failure of the command executed.

    Raises:
        AssertionError: If any of the conditions do not pass, an AssertionError is raised with
            a detailed message including the failed condition and the subprocess
            output.
    """
    for condition in conditions:
        condition.ingest_info(info)
        try:
            assert condition.passes
        except AssertionError as exc:
            error_message = (
                f"Condition failed: {condition}\n"
                f"Captured stdout: {info['stdout']}\n"
                f"Captured stderr: {info['stderr']}\n"
                f"Return code: {info['return_code']}\n"
            )
            raise AssertionError(error_message) from exc


def run_workflow(redis_client: FixtureRedis, workflow_path: str, vars_to_substitute: List[str]) -> subprocess.CompletedProcess:
    """
    Run a Merlin workflow using the `merlin run` and `merlin run-workers` commands.

    This function executes a Merlin workflow using a specified path to a study and variables to
    configure the study with. It utilizes context managers to safely send tasks to the server
    and start up workers. The tasks are given 15 seconds to be sent to the server. Once tasks
    exist on the server, the workflow is given 30 seconds to run to completion, which should be
    plenty of time.

    Args:
        redis_client: A fixture that connects us to a redis client that we can interact with.
        workflow_path: The path to the study that we're going to run here
        vars_to_substitute: A list of variables in the form ["VAR_NAME=var_value"] to be modified
            in the workflow.

    Returns:
        The completed process object containing information about the execution of the workflow, including
            return code, stdout, and stderr.
    """
    from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

    run_workers_proc = None

    with CeleryTaskManager(celery_app, redis_client):
        # Send the tasks to the server
        try:
            subprocess.run(
                f"merlin run {workflow_path} --vars {' '.join(vars_to_substitute)}",
                shell=True,
                capture_output=True,
                text=True,
                timeout=15,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("Could not send tasks to the server within the allotted time.") from exc

        # We use a context manager to start workers so that they'll safely stop even if this test fails
        with CeleryWorkersManager(celery_app) as celery_worker_manager:
            # Start the workers then add them to the context manager so they can be stopped safely later
            run_workers_proc = subprocess.Popen(  # pylint: disable=consider-using-with
                f"merlin run-workers {workflow_path}".split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            celery_worker_manager.add_run_workers_process(run_workers_proc.pid)

            # Let the workflow try to run for 30 seconds
            sleep(30)

    return run_workers_proc
