"""
This module contains helper functions for the integration
test suite.
"""

import os
import re
import shutil
from typing import Dict, List

import yaml

from merlin.spec.expansion import get_spec_with_expansion
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
