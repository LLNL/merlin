##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module to define functionality for sending tasks to the server
and ensuring they're cleared from the server when the test finishes.
"""

from types import TracebackType
from typing import List, Type

from celery import Celery
from celery.result import AsyncResult
from redis import Redis


class CeleryTaskManager:
    """
    A context manager for managing Celery tasks.

    This class provides a way to send tasks to a Celery server and clean up
    any tasks that were sent during its lifetime. It is designed to be used
    as a context manager, ensuring that tasks are properly removed from the
    server when the context is exited.

    Attributes:
        celery_app: The Celery application instance.
        redis_server: The Redis server connection string.
    """

    def __init__(self, app: Celery, redis_client: Redis):
        self.celery_app: Celery = app
        self.redis_client = redis_client

    def __enter__(self) -> "CeleryTaskManager":
        """
        Enters the runtime context related to this object.

        Returns:
            The current instance of the manager.
        """
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, traceback: TracebackType):
        """
        Exits the runtime context and performs cleanup.

        This method removes any tasks currently in the server.

        Args:
            exc_type: The exception type raised, if any.
            exc_value: The exception instance raised, if any.
            traceback: The traceback object, if an exception was raised.
        """
        self.remove_tasks()

    def send_task(self, task_name: str, *args, **kwargs) -> AsyncResult:
        """
        Sends a task to the Celery server.

        This method will be used for tests that don't call
        `merlin run`, allowing for isolated test functionality.

        Args:
            task_name: The name of the task to send to the server.
            *args: Additional positional arguments to pass to the task.
            **kwargs: Additional keyword arguments to pass to the task.

        Returns:
            A Celery AsyncResult object containing information about the
                task that was sent to the server.
        """
        valid_kwargs = [
            "add_to_parent",
            "chain",
            "chord",
            "compression",
            "connection",
            "countdown",
            "eta",
            "exchange",
            "expires",
            "group_id",
            "group_index",
            "headers",
            "ignore_result",
            "link",
            "link_error",
            "parent_id",
            "priority",
            "producer",
            "publisher",
            "queue",
            "replaced_task_nesting",
            "reply_to",
            "result_cls",
            "retries",
            "retry",
            "retry_policy",
            "root_id",
            "route_name",
            "router",
            "routing_key",
            "serializer",
            "shadow",
            "soft_time_limit",
            "task_id",
            "task_type",
            "time_limit",
        ]
        send_task_kwargs = {key: kwargs.pop(key) for key in valid_kwargs if key in kwargs}

        return self.celery_app.send_task(task_name, args=args, kwargs=kwargs, **send_task_kwargs)

    def remove_tasks(self):
        """
        Removes tasks from the Celery server.

        Tasks are removed in two ways:
        1. By purging the Celery app queues, which will only purge tasks
           sent with `send_task`.
        2. By deleting the remaining queues in the Redis server, which will
           purge any tasks that weren't sent with `send_task` (e.g., tasks
           sent with `merlin run`).
        """
        # Purge the tasks
        self.celery_app.control.purge()

        # Purge any remaining tasks directly through redis that may have been missed
        queues = self.get_queue_list()
        for queue in queues:
            self.redis_client.delete(queue)

    def get_queue_list(self) -> List[str]:
        """
        Builds a list of Celery queues that exist on the Redis server.

        Queries the Redis server for its keys and returns the keys
        that represent the Celery queues.

        Returns:
            A list of Celery queue names.
        """
        cursor = 0
        queues = []
        while True:
            # Get the 'merlin' queue if it exists
            cursor, matching_queues = self.redis_client.scan(cursor=cursor, match="merlin")
            queues.extend(matching_queues)

            # Get any queues that start with '[merlin]'
            cursor, matching_queues = self.redis_client.scan(cursor=cursor, match="\\[merlin\\]*")
            queues.extend(matching_queues)

            if cursor == 0:
                break

        return queues
