"""
"""
import logging
from typing import List

from redis import Redis

from merlin.db_scripts.data_models import LogicalWorkerModel

LOG = logging.getLogger("merlin")


class RedisLogicalWorkerStore:
    """
    """

    def __init__(self, client: Redis):
        self.client: Redis = client

    def save(self, worker: LogicalWorkerModel):
        """
        Given a [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] object, enter
        all of it's information to the backend database.

        Args:
            worker: A [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] instance.
        """

    def retrieve(self, worker_id: str) -> LogicalWorkerModel:
        """
        Given a worker's id, retrieve it from the backend database.

        Args:
            worker_id: The ID of the worker to retrieve.

        Returns:
            A [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] instance.
        """

    def retrieve_all(self) -> List[LogicalWorkerModel]:
        """
        Query the backend database for every logical worker that's currently stored.

        Returns:
            A list of [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] objects.
        """

    def delete(self, worker_id: str):
        """
        Given a worker id, find it in the database and remove that entry.

        Args:
            worker_id: The id of the worker to delete.
        """