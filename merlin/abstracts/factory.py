##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Base factory class for managing pluggable components in Merlin.

This module defines an abstract `MerlinBaseFactory` class that provides a reusable
infrastructure for registering, discovering, and instantiating pluggable components.
It supports alias resolution, entry-point-based plugin discovery, and runtime
introspection of registered components.

Subclasses must define how to register built-in components, validate component classes,
and identify the appropriate entry point group for plugin discovery.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Type

import pkg_resources


LOG = logging.getLogger("merlin")


class MerlinBaseFactory(ABC):
    """
    Abstract base factory for managing and instantiating pluggable components.

    This class provides the infrastructure for:
    - Registering components and their aliases
    - Discovering plugins via Python entry points
    - Creating instances of registered components
    - Listing and introspecting available components

    Subclasses are required to:
        - Implement `_register_builtins()` to register default implementations
        - Implement `_validate_component()` to enforce interface/type constraints
        - Define `_entry_point_group()` to identify the entry point namespace for discovery

    Attributes:
        _registry (Dict[str, Any]): Maps canonical component names to their classes.
        _aliases (Dict[str, str]): Maps alias names to canonical component names.

    Methods:
        register: Register a new component and its optional aliases.
        list_available: Return a list of all registered component names.
        create: Instantiate a registered component by name or alias.
        get_component_info: Return introspection metadata for a registered component.
        _discover_plugins: Discover and register plugin components using entry points.
        _register_builtins: Abstract method for registering built-in/default components.
        _validate_component: Abstract method for enforcing type/interface constraints.
        _entry_point_group: Abstract method for returning the entry point namespace.
    """

    def __init__(self):
        """
        Initialize the base factory.

        This base class provides common functionality for managing
        a registry of available implementations and any aliases for them.
        Subclasses can extend this to register built-in or default items.
        """
        # Map canonical names to implementation classes or instances
        self._registry: Dict[str, Any] = {}

        # Map aliases to canonical names (e.g., legacy names or shorthand)
        self._aliases: Dict[str, str] = {}

        # Register built-in implementations, if any
        self._register_builtins()

    @abstractmethod
    def _register_builtins(self):
        """
        Register built-in components.

        Subclasses must implement this to register relevant components.
        """
        raise NotImplementedError("Subclasses of `MerlinBaseFactory` must implement a `_register_builtins` method.")

    @abstractmethod
    def _validate_component(self, component_class: Any):
        """
        Validate the component class before registration.

        Subclasses must implement this to enforce type or interface constraints.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If `component_class` is not valid.
        """
        raise NotImplementedError("Subclasses of `MerlinBaseFactory` must implement a `_validate_component` method.")

    @abstractmethod
    def _entry_point_group(self) -> str:
        """
        Return the entry point group used for plugin discovery.

        Subclasses must override this.

        Returns:
            The entry point group used for plugin discovery.
        """
        raise NotImplementedError("Subclasses must define an entry point group.")

    def _discover_plugins_via_entry_points(self):
        """
        Discover and register plugins via Python entry points.
        """
        try:
            for entry_point in pkg_resources.iter_entry_points(self._entry_point_group()):
                try:
                    plugin_class = entry_point.load()
                    self.register(entry_point.name, plugin_class)
                    LOG.info(f"Loaded plugin via entry point: {entry_point.name}")
                except Exception as e:  # pylint: disable=broad-exception-caught
                    LOG.warning(f"Failed to load plugin '{entry_point.name}': {e}")
        except ImportError:
            LOG.debug("pkg_resources not available for plugin discovery")

    def _discover_builtin_modules(self):
        """
        Optional hook to discover built-in components by scanning local modules.

        Default implementation does nothing.

        Subclasses can override this method to implement package/module scanning.
        """
        LOG.warning(
            f"Class {self.__class__.__name__} did not override _discover_builtin_modules(). "
            "Built-in module discovery will be skipped."
        )

    def _discover_plugins(self):
        """
        Discover and register plugin components via entry points.

        Subclasses can override this to support more discovery mechanisms.
        """
        self._discover_plugins_via_entry_points()
        self._discover_builtin_modules()

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        """
        Raise an appropriate exception when an invalid component is requested.

        Subclasses should override this to raise more specific exceptions.

        Args:
            msg: The message to add to the error being raised.

        Raises:
            A subclass of Exception (e.g., ValueError by default).
        """
        raise ValueError(msg)

    def register(self, name: str, component_class: Any, aliases: List[str] = None) -> None:
        """
        Register a new component implementation.

        Args:
            name: Canonical name for the component.
            component_class: The class or implementation to register.
            aliases: Optional alternative names for this component.

        Raises:
            TypeError: If the component_class fails validation.
        """
        self._validate_component(component_class)

        self._registry[name] = component_class
        LOG.debug(f"Registered component: {name}")

        if aliases:
            for alias in aliases:
                self._aliases[alias] = name
                LOG.debug(f"Registered alias '{alias}' for component '{name}'")

    def list_available(self) -> List[str]:
        """
        Return a list of supported component names.

        This includes both built-in and dynamically discovered components.

        Returns:
            A list of canonical names for all available components.
        """
        self._discover_plugins()
        return list(self._registry.keys())

    def _get_component_class(self, canonical_name: str, component_type: str) -> Any:
        """
        Retrieve a registered component class by its canonical name.

        This method ensures that all plugin discovery mechanisms have been invoked
        before attempting to look up the component. If the requested component is
        not found in the registry, it raises a descriptive error with a list of
        available components.

        Args:
            canonical_name: The canonical name of the component (resolved from alias).
            component_type: The original name or alias provided by the user (used in error messages).

        Returns:
            The class object corresponding to the requested component.

        Raises:
            Exception: Raises the result of `_raise_component_error_class` if the component is not registered.
        """
        # Discover plugins if needed
        if canonical_name not in self._registry:
            self._discover_plugins()

        # Grab the component class from the registry and ensure it's supported
        component_class = self._registry.get(canonical_name)
        if component_class is None:
            available = ", ".join(self.list_available())
            self._raise_component_error_class(
                f"Component '{component_type}' is not supported. " f"Available components: {available}"
            )

        return component_class

    def create(self, component_type: str, config: Dict = None) -> Any:
        """
        Instantiate and return a component of the specified type.

        Args:
            component_type: The name or alias of the component to create.
            config: Optional configuration for initializing the component.

        Returns:
            An instance of the requested component.

        Raises:
            Exception: If the component is not registered or instantiation fails.
        """
        # Resolve alias
        canonical_name = self._aliases.get(component_type, component_type)

        # Get the class associated with the name
        component_class = self._get_component_class(canonical_name, component_type)

        # Create and return an instance of the component_class
        try:
            instance = component_class() if config is None else component_class(**config)
            LOG.info(f"Created component '{canonical_name}'")
            return instance
        except Exception as e:
            raise ValueError(f"Failed to create component '{canonical_name}': {e}") from e

    def get_component_info(self, component_type: str) -> Dict:
        """
        Get introspection information about a registered component.

        Args:
            component_type: The name or alias of the component.

        Returns:
            Dictionary containing metadata such as name, class, module, and docstring.

        Raises:
            Exception: If the component is not registered.
        """
        canonical_name = self._aliases.get(component_type, component_type)

        component_class = self._get_component_class(canonical_name, component_type)

        return {
            "name": canonical_name,
            "class": component_class.__name__,
            "module": component_class.__module__,
            "description": component_class.__doc__ or "No description available",
        }
