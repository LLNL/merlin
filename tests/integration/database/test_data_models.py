"""
Integration tests to test interactions between models.
"""

from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel


class TestModelInteractions:
    """Tests for interactions between models."""

    def test_study_run_relationship(self):
        """Test the relationship between StudyModel and RunModel."""
        # Create a study
        study = StudyModel(id="study-123", name="Test Study")

        # Create runs associated with the study
        run1 = RunModel(id="run-1", study_id=study.id)
        run2 = RunModel(id="run-2", study_id=study.id)

        # Update study with run IDs
        study.update_fields({"runs": [run1.id, run2.id]})

        # Verify the relationship
        assert run1.id in study.runs
        assert run2.id in study.runs
        assert run1.study_id == study.id
        assert run2.study_id == study.id

    def test_logical_physical_worker_relationship(self):
        """Test the relationship between LogicalWorkerModel and PhysicalWorkerModel."""
        # Create a logical worker
        logical_worker = LogicalWorkerModel(name="worker1", queues={"queue1"})

        # Create physical workers associated with the logical worker
        physical_worker1 = PhysicalWorkerModel(id="pw-1", logical_worker_id=logical_worker.id, name="celery@worker1.host1")

        physical_worker2 = PhysicalWorkerModel(id="pw-2", logical_worker_id=logical_worker.id, name="celery@worker1.host2")

        # Update logical worker with physical worker IDs
        logical_worker.update_fields({"physical_workers": [physical_worker1.id, physical_worker2.id]})

        # Verify the relationship
        assert physical_worker1.id in logical_worker.physical_workers
        assert physical_worker2.id in logical_worker.physical_workers
        assert physical_worker1.logical_worker_id == logical_worker.id
        assert physical_worker2.logical_worker_id == logical_worker.id

    def test_run_worker_relationship(self):
        """Test the relationship between RunModel and LogicalWorkerModel."""
        # Create a run
        run = RunModel(id="run-test", study_id="study-test")

        # Create logical workers for the run
        worker1 = LogicalWorkerModel(name="worker1", queues={"queue1"})
        worker2 = LogicalWorkerModel(name="worker2", queues={"queue2"})

        # Update run with worker IDs
        run.update_fields({"workers": [worker1.id, worker2.id]})

        # Update workers with run ID
        worker1.update_fields({"runs": [run.id]})
        worker2.update_fields({"runs": [run.id]})

        # Verify the relationship
        assert worker1.id in run.workers
        assert worker2.id in run.workers
        assert run.id in worker1.runs
        assert run.id in worker2.runs
