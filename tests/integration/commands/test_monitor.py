"""
This module will contain the testing logic for the `merlin monitor` command.
"""

import subprocess
from time import sleep

from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_data_classes import MonitorSetup, RedisBrokerAndBackend
from tests.fixture_types import FixtureStr
from tests.integration.conditions import HasRegex, StepFileExists
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd


class TestMonitor:
    """
    Tests for the `merlin monitor` command.
    """

    # TODO this is huge, can we split it up somehow?
    def test_auto_restart(
        self, monitor_setup: MonitorSetup, redis_broker_and_backend_class: RedisBrokerAndBackend, merlin_server_dir: FixtureStr
    ):
        """
        Test that the monitor automatically restarts the workflow when:
        1. There are no tasks in the queues
        2. There are no workers processing tasks
        3. The workflow has not yet finished

        This test is accomplished by:
        1. Sending tasks to the queues
        2. Starting workers so that they begin processing the workflow
        3. Starting the monitor so that it begins monitoring the workflow
        4. Stopping the workers so that the tasks disappear
        5. Restarting the workers who should now have nothing to work on, plus the workflow is
           not yet complete.

        The result of this process will produce the necessary conditions for the monitor to
        restart the workflow.

        Args:
            monitor_setup: A fixture that returns a
                [`MonitorSetup`][fixture_data_classes.MonitorSetup] instance.
            redis_broker_and_backend_class: Fixture for setting up Redis broker and
                backend for class-scoped tests.
            merlin_server_dir: A fixture to provide the path to the merlin_server directory that will be
                created by the [`redis_server`][conftest.redis_server] fixture.
        """
        from merlin.celery import app as celery_app  # pylint: disable=import-outside-toplevel

        # Need to copy app.yaml to cwd so we can connect to redis server
        copy_app_yaml_to_cwd(merlin_server_dir)

        run_workers_proc = restart_workers_proc = monitor_stdout = monitor_stderr = None
        with CeleryTaskManager(celery_app, redis_broker_and_backend_class.client):
            # Send the tasks to the server
            try:
                subprocess.run(
                    f"merlin run {monitor_setup.auto_restart_yaml} --vars OUTPUT_PATH={monitor_setup.testing_dir}",
                    shell=True,
                    text=True,
                    timeout=15,
                )
            except subprocess.TimeoutExpired as exc:
                raise TimeoutError("Could not send tasks to the server within the allotted time.") from exc

            # We use a context manager to start workers so that they'll safely stop even if this test fails
            with CeleryWorkersManager(celery_app) as celery_worker_manager:
                # Start the workers then add them to the context manager so they can be stopped safely later
                # This worker will start processing the workflow but we don't want it to finish processing it
                run_workers_proc = subprocess.Popen(  # pylint: disable=consider-using-with
                    f"merlin run-workers {monitor_setup.auto_restart_yaml}".split(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
                )
                celery_worker_manager.add_run_workers_process(run_workers_proc.pid)
                sleep(5)

                # Start the monitor and give it a 3 second sleep interval
                monitor_proc = subprocess.Popen(  # pylint: disable=consider-using-with
                    f"merlin monitor {monitor_setup.auto_restart_yaml} --sleep 3".split(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
                )

                # Give the monitor 10 seconds to get going, then stop the workers
                sleep(10)
                celery_worker_manager.stop_all_workers()

                # Restart workers; these should have no tasks to process until monitor restarts workflow
                restart_workers_proc = subprocess.Popen(  # pylint: disable=consider-using-with
                    f"merlin run-workers {monitor_setup.auto_restart_yaml}".split(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    start_new_session=True,
                )
                celery_worker_manager.add_run_workers_process(restart_workers_proc.pid)

                monitor_stdout, monitor_stderr = monitor_proc.communicate()

        # Define our test conditions
        study_name = "monitor_auto_restart_test"
        conditions = [
            HasRegex("Monitor: Restarting workflow for run with workspace"),
            HasRegex("Monitor: Workflow restarted successfully:"),
            HasRegex("Monitor: Failed to restart workflow:", negate=True),
            StepFileExists("step_1", "MERLIN_FINISHED", study_name, monitor_setup.testing_dir),
        ]

        # Check our test conditions
        workers_stdout = run_workers_proc.stdout.read() + restart_workers_proc.stdout.read()
        workers_stderr = run_workers_proc.stderr.read() + restart_workers_proc.stderr.read()
        info = {
            "return_code": monitor_proc.returncode,
            "stdout": monitor_stdout + workers_stdout,
            "stderr": monitor_stderr + workers_stderr,
        }
        check_test_conditions(conditions, info)
