##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `factory.py` module of the `abstracts/` directory.
"""

from typing import Any, Type

import pytest
from pytest_mock import MockerFixture

from merlin.abstracts import MerlinBaseFactory


# --- Dummy Components ---
class DummyComponent:
    """A testable dummy component."""


class DummyComponentWithInit:
    def __init__(self, foo=None, bar=None):
        self.foo = foo
        self.bar = bar


# --- Concrete Subclass for Testing ---
class TestableFactory(MerlinBaseFactory):
    def _register_builtins(self) -> None:
        self.register("dummy", DummyComponent, aliases=["alias_dummy"])

    def _validate_component(self, component_class: Any) -> None:
        if not isinstance(component_class, type):
            raise TypeError("Component must be a class")

    def _entry_point_group(self) -> str:
        return "merlin.test_plugins"

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        raise RuntimeError(msg)  # Use a distinct error type for test verification


class TestMerlinBaseFactory:
    """
    Unit test suite for the `MerlinBaseFactory` abstract base class.

    This suite verifies the expected behavior of the factory's core logic through a
    concrete subclass (`TestableFactory`) that defines the required abstract methods.
    The tests ensure that the factory:

    - Registers components and their aliases correctly
    - Validates component classes during registration
    - Creates instances with and without initialization arguments
    - Resolves aliases when creating components
    - Raises appropriate errors for unknown components
    - Provides accurate component metadata through introspection
    - Invokes plugin discovery hooks properly

    The tests do not rely on external entry points or plugins, and use simple
    dummy component classes to isolate and validate base functionality.
    """

    @pytest.fixture
    def factory(self) -> TestableFactory:
        """
        An instance of the dummy `TestableFactory` class. Resets on each test.

        Returns:
            An instance of the dummy `TestableFactory` class for testing.
        """
        return TestableFactory()

    def test_register_and_list(self, factory: TestableFactory):
        """
        Test that components are registered and listed properly.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        assert "dummy" in factory.list_available()
        assert factory._registry["dummy"] is DummyComponent
        assert factory._aliases["alias_dummy"] == "dummy"

    def test_create_component_without_config(self, factory: TestableFactory):
        """
        Test instantiation of a registered component with no config.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        instance = factory.create("dummy")
        assert isinstance(instance, DummyComponent)

    def test_create_component_with_config(self, factory: TestableFactory):
        """
        Test instantiation of a component with constructor args.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        factory.register("with_init", DummyComponentWithInit)
        config = {"foo": "a", "bar": 42}
        instance = factory.create("with_init", config=config)
        assert isinstance(instance, DummyComponentWithInit)
        assert instance.foo == "a"
        assert instance.bar == 42

    def test_create_component_using_alias(self, factory: TestableFactory):
        """
        Test alias resolution in component creation.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        instance = factory.create("alias_dummy")
        assert isinstance(instance, DummyComponent)

    def test_create_unregistered_component_raises(self, factory: TestableFactory):
        """
        Test that creating an unknown component raises the correct error.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        with pytest.raises(RuntimeError, match="not supported"):
            factory.create("unknown")

    def test_register_invalid_component_raises(self, factory: TestableFactory):
        """
        Test that register raises TypeError for non-class input.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        with pytest.raises(TypeError):
            factory.register("bad", object())  # not a class

    def test_get_component_info(self, factory: TestableFactory):
        """
        Test metadata returned from `get_component_info`.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        info = factory.get_component_info("dummy")
        assert info["name"] == "dummy"
        assert info["class"] == "DummyComponent"
        assert info["module"] == DummyComponent.__module__
        assert "description" in info

    def test_get_component_info_for_invalid_component(self, factory: TestableFactory):
        """
        Test that get_component_info raises when the component is unknown.

        Args:
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        with pytest.raises(
            RuntimeError, match="not supported"
        ):  # raises RuntimeError because of `_raise_component_error_class`
            factory.get_component_info("not_registered")

    def test_discover_plugins_calls_both_hooks(self, mocker: MockerFixture, factory: TestableFactory):
        """
        Test that _discover_plugins calls both plugin and module hooks.

        Args:
            mocker: PyTest mocker fixture.
            factory: An instance of the dummy `TestableFactory` class for testing.
        """
        plugin_mock = mocker.patch.object(factory, "_discover_plugins_via_entry_points")
        builtin_mock = mocker.patch.object(factory, "_discover_builtin_modules")

        factory._discover_plugins()
        plugin_mock.assert_called_once()
        builtin_mock.assert_called_once()
