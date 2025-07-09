##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Task server factory for selecting and instantiating task servers in Merlin.

This module defines the `TaskServerFactory` class, which serves as an abstraction
layer for managing available task server implementations. It supports dynamic selection
and instantiation of task server handlers such as Celery or (TODO) Kafka, based on user input
or system configuration.

The factory maintains mappings of task server names and aliases, and raises a clear error
if an unsupported task server is requested.
"""

import logging
from typing import Dict, List

from merlin.task_servers.task_server_interface import TaskServerInterface
from merlin.exceptions import MerlinInvalidTaskServerError


LOG = logging.getLogger(__name__)


class TaskServerFactory:
    """
    Factory class for managing and instantiating supported Merlin task servers.

    This class maintains a registry of available task server implementations (e.g., Celery, Kafka)
    and provides a unified interface for creating task server instances. Supports plugin discovery
    and server introspection for both built-in and external task server implementations.

    Attributes:
        _task_servers (Dict[str, TaskServerInterface]): Mapping of task server names to their classes.
        _task_server_aliases (Dict[str, str]): Optional aliases for resolving canonical task server names.

    Methods:
        create: Create and initialize a task server instance by name.
        register: Register a new task server implementation with optional aliases.
        list_available: Get list of all available task server names.
        get_server_info: Get detailed information about a specific task server type.
        _discover_plugins: Discover and load task server plugins from entry points.
        
    Legacy Methods (for backward compatibility):
        get_task_server: Use create() instead.
        register_task_server: Use register() instead.
        get_supported_task_servers: Use list_available() instead.
    """

    def __init__(self):
        """Initialize the task server factory."""
        # Map canonical task server names to their classes
        self._task_servers: Dict[str, TaskServerInterface] = {}
        # Map aliases to canonical task server names
        self._task_server_aliases: Dict[str, str] = {
            "redis": "celery",  # Legacy alias
            "rabbitmq": "celery",  # Legacy alias
        }
        
        # Register built-in task servers
        self._register_builtin_servers()

    def _register_builtin_servers(self):
        """Register built-in task server implementations."""
        try:
            from merlin.task_servers.implementations.celery_server import CeleryTaskServer
            self.register("celery", CeleryTaskServer)
            LOG.debug("Registered CeleryTaskServer")
        except ImportError as e:
            LOG.warning(f"Could not register CeleryTaskServer: {e}")
        
        # Register other built-in servers as they become available (TODO)
        try:
            from merlin.task_servers.implementations.kafka_server import KafkaTaskServer
            self.register("kafka", KafkaTaskServer)
            LOG.debug("Registered KafkaTaskServer")
        except ImportError:
            LOG.debug("KafkaTaskServer not available")

    def list_available(self) -> List[str]:
        """
        Get a list of the supported task servers in Merlin.

        Returns:
            A list of names representing the supported task servers in Merlin.
        """
        self._discover_plugins()
        return list(self._task_servers.keys())

    def create(self, server_type: str, config: Dict = None) -> TaskServerInterface:
        """
        Create and return a task server instance for the specified type.

        Args:
            server_type: The name of the task server to create.
            config: Optional configuration dictionary for task server initialization.

        Returns:
            An instantiation of a TaskServerInterface object.

        Raises:
            MerlinInvalidTaskServerError: If the requested task server is not supported.
        """
        # Resolve alias to canonical task server name
        server_type = self._task_server_aliases.get(server_type, server_type)

        # Discover plugins if server not found
        if server_type not in self._task_servers:
            self._discover_plugins()

        # Get correct task server class
        task_server_class = self._task_servers.get(server_type)

        if task_server_class is None:
            available = ", ".join(self.list_available())
            raise MerlinInvalidTaskServerError(
                f"Task server '{server_type}' is not supported by Merlin. "
                f"Available task servers: {available}"
            )

        # Create instance
        try:
            instance = task_server_class()
            LOG.info(f"Created {server_type} task server")
            return instance
        except Exception as e:
            raise MerlinInvalidTaskServerError(
                f"Failed to create {server_type} task server: {e}"
            ) from e

    def register(self, name: str, server_class: TaskServerInterface, 
                aliases: List[str] = None) -> None:
        """
        Register a new task server implementation.

        Args:
            name: The canonical name for the task server.
            server_class: The class implementing TaskServerInterface.
            aliases: Optional list of alternative names for this task server.

        Raises:
            TypeError: If the server_class does not implement TaskServerInterface.
        """
        if not issubclass(server_class, TaskServerInterface):
            raise TypeError(f"{server_class} must implement TaskServerInterface")

        self._task_servers[name] = server_class
        LOG.debug(f"Registered task server: {name}")

        if aliases:
            for alias in aliases:
                self._task_server_aliases[alias] = name
                LOG.debug(f"Registered alias '{alias}' for task server '{name}'")

    def get_server_info(self, server_type: str) -> Dict:
        """
        Get information about a specific task server type.

        Args:
            server_type: The name of the task server to get info for.

        Returns:
            Dictionary containing server information and capabilities.

        Raises:
            MerlinInvalidTaskServerError: If the requested task server is not supported.
        """
        # Resolve alias to canonical task server name
        server_type = self._task_server_aliases.get(server_type, server_type)

        if server_type not in self._task_servers:
            self._discover_plugins()

        if server_type not in self._task_servers:
            available = ", ".join(self.list_available())
            raise MerlinInvalidTaskServerError(
                f"Task server '{server_type}' is not supported by Merlin. "
                f"Available task servers: {available}"
            )

        server_class = self._task_servers[server_type]
        return {
            "name": server_type,
            "class": server_class.__name__,
            "module": server_class.__module__,
            "description": server_class.__doc__ or "No description available",
        }

    def _discover_plugins(self) -> None:
        """
        Discover and load task server plugins from entry points and modules.
        
        This method attempts to find additional task server implementations
        through Python entry points and module scanning.
        """
        # METHOD 1: Entry points (for pip-installable plugins)
        try:
            import pkg_resources
            for entry_point in pkg_resources.iter_entry_points('merlin.task_servers'):
                try:
                    plugin_class = entry_point.load()
                    self.register(entry_point.name, plugin_class)
                    LOG.info(f"Loaded task server plugin: {entry_point.name}")
                except Exception as e:
                    LOG.warning(f"Failed to load plugin {entry_point.name}: {e}")
        except ImportError:
            LOG.debug("pkg_resources not available for plugin discovery")

        # METHOD 2: Built-in implementations directory scanning
        try:
            import importlib
            import pkgutil
            from merlin.task_servers import implementations
            
            for _, module_name, _ in pkgutil.iter_modules(implementations.__path__):
                if module_name.endswith('_server'):
                    try:
                        module = importlib.import_module(
                            f"merlin.task_servers.implementations.{module_name}"
                        )
                        # Look for classes ending with "TaskServer"
                        for attr_name in dir(module):
                            if attr_name.endswith("TaskServer"):
                                attr = getattr(module, attr_name)
                                if (isinstance(attr, type) and 
                                    issubclass(attr, TaskServerInterface) and
                                    attr != TaskServerInterface):
                                    # Extract server type from class name
                                    server_type = attr_name.replace("TaskServer", "").lower()
                                    if server_type not in self._task_servers:
                                        self.register(server_type, attr)
                                        LOG.debug(f"Auto-discovered task server: {server_type}")
                    except Exception as e:
                        LOG.debug(f"Failed to load implementation {module_name}: {e}")
        except Exception as e:
            LOG.debug(f"Failed to discover built-in implementations: {e}")

    # Legacy methods for backward compatibility
    def get_supported_task_servers(self) -> List[str]:
        """Legacy method - use list_available() instead."""
        return self.list_available()

    def get_task_server(self, task_server: str, config: Dict = None) -> TaskServerInterface:
        """Legacy method - use create() instead.""" 
        return self.create(task_server, config)

    def register_task_server(self, name: str, task_server_class: TaskServerInterface,
                           aliases: List[str] = None) -> None:
        """Legacy method - use register() instead."""
        return self.register(name, task_server_class, aliases)


# Global factory instance
task_server_factory = TaskServerFactory()
