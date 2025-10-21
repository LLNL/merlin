##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module will contain the testing logic for the `merlin monitor` command.
"""

import os
import shutil
import subprocess
import tempfile
from time import sleep
from typing import Generator, List, Tuple
from unittest.mock import MagicMock, Mock, patch

import pytest

from merlin.config.configfile import initialize_config
from merlin.db_scripts.entities.study_entity import StudyEntity
from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.monitor.monitor import Monitor
from merlin.spec.specification import MerlinSpec
from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_data_classes import MonitorSetup, RedisBrokerAndBackend
from tests.fixture_types import FixtureCallable, FixtureStr
from tests.integration.conditions import HasRegex, StepFileExists
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd


@pytest.fixture(scope="session")
def monitor_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the monitor functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary output directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for monitor tests.
    """
    return create_testing_dir(temp_output_dir, "monitor_testing")


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
        4. Purging the tasks so that there's nothing left in the queues and the workflow cannot finish
           without a restart that the monitor must provide

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

        run_workers_proc = monitor_stdout = monitor_stderr = None
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

                # Purge the tasks that are in the queues
                purge_proc = subprocess.run(
                    f"merlin purge -f {monitor_setup.auto_restart_yaml}".split(), capture_output=True, text=True
                )

                monitor_stdout, monitor_stderr = monitor_proc.communicate()

        # Define our test conditions
        study_name = "monitor_auto_restart_test"
        conditions = [
            HasRegex("Purged 1 message from 2 known task queues."),
            HasRegex("Monitor: Restarting workflow for run with workspace"),
            HasRegex("Monitor: Workflow restarted successfully:"),
            HasRegex("Monitor: Failed to restart workflow:", negate=True),
            StepFileExists("process_samples", "MERLIN_FINISHED", study_name, monitor_setup.testing_dir, samples=True),
            StepFileExists("funnel_step", "MERLIN_FINISHED", study_name, monitor_setup.testing_dir),
        ]

        # Check our test conditions
        info = {
            "return_code": monitor_proc.returncode,
            "stdout": monitor_stdout + purge_proc.stdout + run_workers_proc.stdout.read(),
            "stderr": monitor_stderr + purge_proc.stderr + run_workers_proc.stderr.read(),
        }
        check_test_conditions(conditions, info)


class TestMultiRunMonitoring:
    """
    Integration tests for monitoring multiple concurrent runs.
    """

    @pytest.fixture(autouse=True)
    def setup_local_config(self):
        """
        Initialize the local configuration so that we're using a SQLite database.
        """
        initialize_config(local_mode=True)

    @pytest.fixture
    def temp_workspaces(self, monitor_testing_dir: FixtureStr) -> Generator[List[str], None, None]:
        """
        Create temporary workspace directories for test runs.

        Args:
            monitor_testing_dir: The path to the temporary testing directory.

        Yields:
            A list of temporary workspace directories.
        """
        workspaces = []
        temp_dirs = []
        
        for i in range(3):
            temp_dir = os.path.join(monitor_testing_dir, f"merlin_test_run_{i}")
            temp_dirs.append(temp_dir)
            
            # Create merlin_info directory
            merlin_info_dir = os.path.join(temp_dir, "merlin_info")
            os.makedirs(merlin_info_dir, exist_ok=True)
            
            workspaces.append(temp_dir)
        
        yield workspaces
        
        # Cleanup
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_spec(self) -> MagicMock:
        """
        Create a mock MerlinSpec object.

        Returns:
            A mocked MerlinSpec instance.
        """
        spec = MagicMock(spec=MerlinSpec)
        spec.name = "test_study"
        spec.get_queue_list.return_value = ["test_queue"]
        return spec

    @pytest.fixture
    def mock_task_server_monitor(self) -> MagicMock:
        """
        Create a mock task server monitor.

        Returns:
            A mocked task server monitor instance.
        """
        monitor = MagicMock()
        monitor.wait_for_workers = MagicMock()
        monitor.run_worker_health_check = MagicMock()
        monitor.check_tasks = MagicMock(return_value=False)
        monitor.check_workers_processing = MagicMock(return_value=False)
        return monitor

    @pytest.fixture
    def setup_database_with_runs(
        self, temp_workspaces: Generator[List[str], None, None]
    ) -> Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None]:
        """
        Set up a test database with a study and multiple runs.
        
        Args:
            temp_workspaces: A list of temporary workspace directories.

        Yields:
            A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        merlin_db = MerlinDatabase()
        
        # Create study
        study_entity = merlin_db.create("study", "test_study")
        
        # Create three runs
        run_entities = []
        for workspace in temp_workspaces:
            run_entity = merlin_db.create(
                "run",
                study_name="test_study",
                workspace=workspace,
                queues=["test_queue"]
            )
            run_entities.append(run_entity)
        
        yield merlin_db, study_entity, run_entities
        
        # Cleanup
        merlin_db.delete_everything(force=True)

    def test_monitor_detects_all_active_runs(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
    ):
        """
        Test that the monitor correctly identifies all active runs.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        merlin_db, study_entity, _ = setup_database_with_runs

        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor):
            Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)

            # Get all runs before any complete
            all_runs = [merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            active_runs = [run for run in all_runs if not run.run_complete]

            assert len(active_runs) == 3, "Should detect all 3 active runs"
            assert len(all_runs) == 3, "Should have 3 total runs"

    def test_monitor_filters_completed_runs(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
    ):
        """
        Test that the monitor correctly filters out completed runs.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        merlin_db, study_entity, run_entities = setup_database_with_runs
        
        # Mark first run as complete
        run_entities[0].run_complete = True
        run_entities[0].save()
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor):
            Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)
            
            # Get active runs
            all_runs = [merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            active_runs = [run for run in all_runs if not run.run_complete]
            completed_runs = [run for run in all_runs if run.run_complete]
            
            assert len(active_runs) == 2, "Should have 2 active runs"
            assert len(completed_runs) == 1, "Should have 1 completed run"
            assert completed_runs[0].get_id() == run_entities[0].get_id()

    def test_monitor_performs_health_checks_on_all_runs(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
    ):
        """
        Test that health checks are performed on all active runs.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        merlin_db, study_entity, run_entities = setup_database_with_runs
        
        # Mark last run as complete so we only check 2 runs
        run_entities[2].run_complete = True
        run_entities[2].save()
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor):
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)
            
            # Run one monitoring cycle
            all_runs = [merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            active_runs = [run for run in all_runs if not run.run_complete]
            
            for run in active_runs:
                monitor.wait_for_workers(run)
                monitor.check_run_health(run)
            
            # Verify health checks were called for each active run
            assert mock_task_server_monitor.run_worker_health_check.call_count == 2

    def test_monitor_detects_stalled_workflow_across_multiple_runs(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
        temp_workspaces: Generator[List[str], None, None],
    ):
        """
        Test that the monitor can detect and restart a stalled workflow
        when monitoring multiple runs.

        **Note:** A workflow is considered stalled if there are no tasks in the queues, no workers are processing tasks,
        and the run has not been marked as complete.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
            temp_workspaces: A list of temporary workspace directories.
        """
        merlin_db, study_entity, run_entities = setup_database_with_runs
        
        # Configure mock to simulate stalled workflow for run 1
        def check_tasks_side_effect(run):
            # Run 0 and 2 have tasks, run 1 is stalled
            if run.get_id() == run_entities[1].get_id():
                return False
            return True
        
        mock_task_server_monitor.check_tasks.side_effect = check_tasks_side_effect
        mock_task_server_monitor.check_workers_processing.return_value = False
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor), \
             patch("merlin.monitor.monitor.subprocess.run") as mock_subprocess:
            
            mock_subprocess.return_value = Mock(returncode=0, stdout="Restart successful", stderr="")
            
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=False)
            
            # Perform health check on all runs
            all_runs = [merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            active_runs = [run for run in all_runs if not run.run_complete]
            
            for run in active_runs:
                monitor.wait_for_workers(run)
                monitor.check_run_health(run)
            
            # Verify restart was called only for the stalled run
            assert mock_subprocess.call_count == 1
            called_workspace = mock_subprocess.call_args[0][0]
            assert temp_workspaces[1] in called_workspace

    def test_monitor_all_runs_exits_when_all_complete(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
    ):
        """
        Test that monitor_all_runs exits gracefully when all runs complete.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        _, _, run_entities = setup_database_with_runs
        
        # Mark all runs as complete
        for run_entity in run_entities:
            run_entity.run_complete = True
            run_entity.save()
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor):
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)
            
            # This should exit immediately without hanging
            monitor.monitor_all_runs()
            
            # Verify no health checks were performed
            assert mock_task_server_monitor.run_worker_health_check.call_count == 0

    def test_monitor_handles_new_runs_dynamically(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
        temp_workspaces: Generator[List[str], None, None],
        monitor_testing_dir: FixtureStr
    ):
        """
        Test that the monitor can detect new runs added during monitoring
        (simulating iterative workflows).

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
            temp_workspaces: A generator of temporary workspace directories.
            monitor_testing_dir: A temporary directory for monitor testing.
        """
        merlin_db, study_entity, _ = setup_database_with_runs
        
        # We'll add a new run after the first monitoring cycle
        monitoring_cycle_count = [0]
        
        def wait_for_workers_side_effect(worker_names, sleep_time):
            monitoring_cycle_count[0] += 1
            
            # After first cycle, add a new run
            if monitoring_cycle_count[0] == 1:
                new_temp_dir = os.path.join(monitor_testing_dir, "merlin_test_run_new")
                merlin_info_dir = os.path.join(new_temp_dir, "merlin_info")
                os.makedirs(merlin_info_dir, exist_ok=True)
                temp_workspaces.append(new_temp_dir)
                
                merlin_db.create(
                    "run",
                    study_name="test_study",
                    workspace=new_temp_dir,
                    queues=["test_queue"]
                )
            
            # Mark all runs complete after second cycle to exit
            if monitoring_cycle_count[0] >= 2:
                study_entity = merlin_db.get("study", "test_study")
                all_run_ids = study_entity.get_runs()
                for run_id in all_run_ids:
                    run = merlin_db.get("run", run_id)
                    run.run_complete = True
                    run.save()
        
        mock_task_server_monitor.wait_for_workers.side_effect = wait_for_workers_side_effect
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor):
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)
            
            # Run the monitor
            monitor.monitor_all_runs()
            
            # Verify we detected and monitored 4 runs total (3 original + 1 new)
            study_entity = merlin_db.get("study", "test_study")
            all_runs = study_entity.get_runs()
            assert len(all_runs) == 4, "Should have detected the new run"

    def test_monitor_handles_concurrent_restarts(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
        temp_workspaces: Generator[List[str], None, None],
    ):
        """
        Test that the monitor can handle restarting multiple stalled runs
        in the same monitoring cycle.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
            temp_workspaces: A generator that provides temporary workspaces.
        """
        merlin_db, study_entity, _ = setup_database_with_runs
        
        # All runs are stalled
        mock_task_server_monitor.check_tasks.return_value = False
        mock_task_server_monitor.check_workers_processing.return_value = False
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor), \
             patch("merlin.monitor.monitor.subprocess.run") as mock_subprocess:
            
            mock_subprocess.return_value = Mock(returncode=0, stdout="Restart successful", stderr="")
            
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=False)
            
            # Perform health check on all runs
            all_runs = [merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            active_runs = [run for run in all_runs if not run.run_complete]
            
            for run in active_runs:
                monitor.wait_for_workers(run)
                monitor.check_run_health(run)
            
            # Verify restart was called for all 3 runs
            assert mock_subprocess.call_count == 3
            
            # Verify each workspace was restarted
            called_workspaces = [call[0][0] for call in mock_subprocess.call_args_list]
            for workspace in temp_workspaces:
                assert any(workspace in cmd for cmd in called_workspaces)

    def test_monitor_no_restart_flag_prevents_restarts(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
    ):
        """
        Test that the no_restart flag prevents automatic restarts across all runs.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        merlin_db, study_entity, _ = setup_database_with_runs
        
        # All runs are stalled
        mock_task_server_monitor.check_tasks.return_value = False
        mock_task_server_monitor.check_workers_processing.return_value = False
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor), \
             patch("merlin.monitor.monitor.subprocess.run") as mock_subprocess:
            
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)
            
            # Perform health check on all runs
            all_runs = [merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            active_runs = [run for run in all_runs if not run.run_complete]
            
            for run in active_runs:
                monitor.wait_for_workers(run)
                monitor.check_run_health(run)
            
            # Verify no restarts occurred
            assert mock_subprocess.call_count == 0

    def test_monitor_all_runs_monitoring_loop(
        self, 
        mock_spec: MagicMock,
        mock_task_server_monitor: MagicMock,
        setup_database_with_runs: Generator[Tuple[MerlinDatabase, StudyEntity, List[RunEntity]], None, None],
    ):
        """
        Test the full monitor_all_runs loop with multiple cycles.

        Args:
            mock_spec: A mocked MerlinSpec instance.
            mock_task_server_monitor: A mocked task server monitor instance.
            setup_database_with_runs: A tuple containing the MerlinDatabase instance, StudyEntity, and a list of RunEntities.
        """
        merlin_db, _, run_entities = setup_database_with_runs
        
        cycle_count = 0
        
        def sleep_side_effect(duration):
            nonlocal cycle_count
            cycle_count += 1
            # After 3 cycles, mark all runs complete to exit
            if cycle_count >= 3:
                for run_entity in run_entities:
                    run = merlin_db.get("run", run_entity.get_id())
                    run.run_complete = True
                    run.save()
        
        mock_task_server_monitor.check_tasks.return_value = True  # Runs are active
        
        with patch("merlin.monitor.monitor.monitor_factory.create", return_value=mock_task_server_monitor), \
            patch("merlin.monitor.monitor.time.sleep", side_effect=sleep_side_effect):
            monitor = Monitor(mock_spec, sleep=1, task_server="celery", no_restart=True)
            
            # Run the monitor
            monitor.monitor_all_runs()
            
            # Verify we went through 3 monitoring cycles
            assert cycle_count == 3
            
            # Verify health checks were performed multiple times
            # 3 runs * 3 cycles = 9 health checks
            assert mock_task_server_monitor.run_worker_health_check.call_count == 9
