##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""

"""

import logging
from typing import Dict, List

from merlin.workers.handlers import BaseWorkerHandler, CeleryWorkerHandler


LOG = logging.getLogger("merlin")


class WorkerHandlerFactory:
    """
    
    """

    # def __init__(self):
    #     """
        
    #     """
    #     # Map canonical handler names to their classes
    #     self._handlers: Dict[str, BaseWorkerHandler] = {}

    #     # Map aliases to canonical handler names
    #     self._handler_aliases: Dict[str, str] = {}

    #     # Register built-in handlers
    #     self._register_builtin_handlers()

    # def _register_builtin_handlers(self):
    #     """Register built-in worker handler implementations."""
    #     try:
    #         self.register("celery", CeleryWorkerHandler)
    #         LOG.debug("Registered CeleryWorkerHandler")
    #     except ImportError as e:
    #         LOG.warning(f"Could not register CeleryWorkerHandler: {e}")

    # def list_available(self) -> List[str]:
    #     """
    #     Get a list of the supported task servers in Merlin.
        
    #     Returns:
    #         A list of names representing the supported task servers in Merlin.
    #     """
    #     self._discover_plugins()
    #     return list(self._task_servers.keys())

    # def register(self, name: str, handler_class: BaseWorkerHandler, aliases: List[str] = None):
    #     """
    #     Register a new worker handler implementation.

    #     Args:
    #         name: The canonical name for the handler.
    #         handler_class: The class implementing BaseWorkerHandler.
    #         aliases: Optional list of alternative names for this handler.

    #     Raises:
    #         TypeError: If the handler_class does not implement BaseWorkerHandler.
    #     """
    #     if not issubclass(handler_class, BaseWorkerHandler):
    #         raise TypeError(f"{handler_class} must implement BaseWorkerHandler")

    #     self._handlers[name] = handler_class
    #     LOG.debug(f"Registered handler: {name}")

    #     if aliases:
    #         for alias in aliases:
    #             self._handler_aliases[alias] = name
    #             LOG.debug(f"Registered alias '{alias}' for handler '{name}'")

    # def create(self, server_type: str, config: Dict = None) -> TaskServerInterface:
    #     """
    #     Create and return a task server instance for the specified type.
    #     Args:
    #         server_type: The name of the task server to create.
    #         config: Optional configuration dictionary for task server initialization.
    #     Returns:
    #         An instantiation of a TaskServerInterface object.
    #     Raises:
    #         MerlinInvalidTaskServerError: If the requested task server is not supported.
    #     """
    #     # Resolve alias to canonical task server name
    #     server_type = self._task_server_aliases.get(server_type, server_type)

    #     # Discover plugins if server not found
    #     if server_type not in self._task_servers:
    #         self._discover_plugins()

    #     # Get correct task server class
    #     task_server_class = self._task_servers.get(server_type)

    #     if task_server_class is None:
    #         available = ", ".join(self.list_available())
    #         raise MerlinInvalidTaskServerError(
    #             f"Task server '{server_type}' is not supported by Merlin. "
    #             f"Available task servers: {available}"
    #         )

    #     # Create instance
    #     try:
    #         instance = task_server_class()
    #         LOG.info(f"Created {server_type} task server")
    #         return instance
    #     except Exception as e:
    #         raise MerlinInvalidTaskServerError(
    #             f"Failed to create {server_type} task server: {e}"
    #         ) from e