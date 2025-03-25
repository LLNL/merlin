"""
"""
import logging
from datetime import datetime
from typing import Dict, List

from merlin.backends.results_backend import ResultsBackend
from merlin.common.abstracts.enums import WorkerStatus
from merlin.db_scripts.data_models import WorkerModel
from merlin.exceptions import WorkerNotFoundError

LOG = logging.getLogger("merlin")

class DatabaseWorker:
    """
    """

    def __init__(self, worker_info: WorkerModel, backend: ResultsBackend):
        self.worker_info: WorkerModel = worker_info
        self.backend: ResultsBackend = backend

    def __repr__(self) -> str:
        """
        Provide a string representation of the `DatabaseWorker` instance.

        Returns:
            A human-readable string representation of the `DatabaseWorker` instance.
        """
        return (
            f"DatabaseWorker("
            f"id={self.get_id()}, "
            f"name={self.get_name()}, "
            f"launch_cmd={self.get_launch_cmd()}, "
            f"queues={self.get_queues()}, "
            f"args={self.get_args()}, "
            f"pid={self.get_pid()}, "
            f"status={self.get_status()}, "
            f"heartbeat_timestamp={self.get_heartbeat_timestamp()}, "
            f"latest_start_time={self.get_latest_start_time()}, "
            f"host={self.get_host()}, "
            f"restart_count={self.get_restart_count()}, "
            f"additional_data={self.get_additional_data()}, "
            f"backend={self.backend.get_name()})"
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the `DatabaseWorker` instance.

        Returns:
            A human-readable string representation of the `DatabaseWorker` instance.
        """
        worker_id = self.get_id()
        return (
            f"Worker with ID {worker_id}\n"
            f"------------{'-' * len(worker_id)}\n"
            f"Name: {self.get_name()}\n"
            f"Launch Command: {self.get_launch_cmd()}\n"
            f"Queues: {self.get_queues()}\n"
            f"Args: {self.get_args()}\n"
            f"Process ID: {self.get_pid()}\n"
            f"Status: {self.get_status()}\n"
            f"Last Heartbeat: {self.get_heartbeat_timestamp()}\n"
            f"Last Spinup: {self.get_latest_start_time()}\n"
            f"Host: {self.get_host()}\n"
            f"Restart Count: {self.get_restart_count()}\n"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )
    
    def reload_data(self):
        """
        Reload the latest data for this run from the database and update the
        [`RunModel`][merlin.db_scripts.db_formats.RunModel] object.
        """
        worker_id = self.get_id()
        updated_worker_info = self.backend.retrieve_worker(worker_id)
        if not updated_worker_info:
            raise WorkerNotFoundError(f"Worker with ID {worker_id} not found in the database.")
        self.worker_info = updated_worker_info
    
    def get_id(self) -> str:
        """
        Get the ID for this worker.

        Returns:
            The ID for this worker.
        """
        return self.worker_info.id

    def get_name(self) -> str:
        """
        Get the name of this worker.

        Returns:
            The name for this worker.
        """
        return self.worker_info.name
    
    def get_launch_cmd(self) -> str:
        """
        Get the command used to launch this worker.

        Returns:
            The command used to launch this worker.
        """
        return self.worker_info.launch_cmd
    
    def set_launch_cmd(self, launch_cmd: str):
        """
        Set the launch command used to start this worker.

        Args:
            launch_cmd: The launch command used to start this worker.
        """
        self.worker_info.launch_cmd = launch_cmd
        self.save()
    
    def get_queues(self) -> List[str]:
        """
        Get the task queues that were assigned to this worker.

        Returns:
            A list of strings representing the queues that were assigned to this worker.
        """
        return self.worker_info.queues
    
    def set_queues(self, queues: List[str]):
        """
        Set the queues that this worker is watching.

        Args:
            queues: The queues that this worker is watching.
        """
        self.worker_info.queues = queues
        self.save()
    
    def get_args(self) -> Dict:
        """
        Get the arguments for this worker.

        Returns:
            A dictionary of arguments for this worker.
        """
        return self.worker_info.args
    
    def set_args(self, args: str):
        """
        Set the arguments used by this worker.

        Args:
            args: The arguments used by this worker.
        """
        self.worker_info.args = args
        self.save()
    
    def get_pid(self) -> str:
        """
        Get the process ID for this worker.

        Returns:
            The process ID for this worker.
        """
        self.reload_data()
        return self.worker_info.pid
    
    def set_pid(self, pid: str):
        """
        Set the PID of this worker.

        Args:
            pid: The new PID of this worker.
        """
        self.worker_info.pid = pid
        self.save()

    def get_status(self) -> WorkerStatus:
        """
        Get the status of this worker.

        Returns:
            A [`WorkerStatus`][common.abstracts.enums.WorkerStatus] enum representing
                the status of this worker.
        """
        self.reload_data()
        return self.worker_info.status
    
    def set_status(self, status: WorkerStatus):
        """
        Set the status of this worker.

        Args:
            status: A [`WorkerStatus`][common.abstracts.enums.WorkerStatus] enum representing
                the new status of the worker.
        """
        self.worker_info.status = status
        self.save()
    
    def get_heartbeat_timestamp(self) -> str:
        """
        Get the last heartbeat timestamp of this worker.

        Returns:
            The last heartbeat timestamp we received from this worker
        """
        self.reload_data()
        return self.worker_info.heartbeat_timestamp
    
    def set_heartbeat_timestamp(self, heartbeat_timestamp: datetime):
        """
        Set the latest heartbeat timestamp of this worker.

        Args:
            heartbeat_timestamp: The latest heartbeat timestamp of this worker.
        """
        self.worker_info.heartbeat_timestamp = heartbeat_timestamp
        self.save()
    
    def get_latest_start_time(self) -> datetime:
        """
        Get the time that this worker was last started.

        Returns:
            A datetime object representing the last time this worker was started.
        """
        self.reload_data()
        return self.worker_info.latest_start_time
    
    def set_latest_start_time(self, latest_start_time: datetime):
        """
        Set the latest start time of this worker. This will be set on worker
        startup followed by any time the worker is restarted.

        Args:
            latest_start_time: The latest start time of this worker.
        """
        self.worker_info.latest_start_time = latest_start_time
        self.save()
    
    def get_host(self) -> str:
        """
        Get the hostname where this worker is running.

        Returns:
            The name of the host that this worker is running on.
        """
        self.reload_data()
        return self.worker_info.host
    
    def set_host(self, host: str):
        """
        Set the host of this worker.

        Args:
            host: The name of the host that this worker is now running on
        """
        self.worker_info.host = host
        self.save()

    def get_restart_count(self) -> int:
        """
        Get the number of times that this worker has been restarted.

        Returns:
            The number of times that this worker has been restarted.
        """
        self.reload_data()
        return self.worker_info.restart_count
    
    def increment_restart_count(self):
        """
        Add another restart to the restart count.
        """
        self.worker_info.restart_count = self.get_restart_count() + 1
        self.save()
    
    def get_additional_data(self) -> Dict:
        """
        Get any additional data saved to this study.

        Returns:
            Additional data saved to this study.
        """
        self.reload_data()
        return self.worker_info.additional_data
    
    def save(self):
        """
        Save the current state of this worker to the database.
        """
        self.backend.save_worker(self.worker_info)

    @classmethod
    def load(cls, worker_id: str, backend: ResultsBackend) -> "DatabaseWorker":
        """
        Load a worker from the database by ID.

        Args:
            worker_id: The ID of the worker to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseWorker` instance.
        """
        worker_info = backend.retrieve_worker(worker_id)
        if not worker_info:
            raise WorkerNotFoundError(f"Worker with ID {worker_id} not found in the database.")

        return cls(worker_info, backend)
    
    @classmethod
    def load_by_name(cls, worker_name: str, backend: ResultsBackend) -> "DatabaseWorker":
        """
        Load a worker from the database by its name.

        Args:
            worker_name: The name of the worker to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseWorker` instance.

        Raises:
            WorkerNotFoundError: If no worker with the given name is found in the database.
        """
        worker_info = backend.retrieve_worker_by_name(worker_name)
        if not worker_info:
            raise WorkerNotFoundError(f"Worker with name '{worker_name}' not found in the database.")

        return cls(worker_info, backend)
    
    @classmethod
    def delete(cls, worker_id: str, backend: ResultsBackend):
        """
        Delete a worker from the database.

        Args:
            worker_id: The ID of the worker to delete.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.
        """
        LOG.info(f"Deleting worker with id '{worker_id}' from the database...")
        backend.delete_worker(worker_id)
        LOG.info(f"Worker with id '{worker_id}' has been successfully deleted.")
