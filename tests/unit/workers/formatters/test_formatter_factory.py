##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/formatters/formatter_factory.py` module.
"""

from typing import Dict, List

import pytest
from pytest_mock import MockerFixture

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import MerlinWorkerFormatterNotSupportedError
from merlin.workers.formatters.formatter_factory import WorkerFormatterFactory
from merlin.workers.formatters.worker_formatter import WorkerFormatter


class DummyJSONFormatter(WorkerFormatter):
    """Dummy JSON formatter implementation for testing."""
    
    def __init__(self, *args, **kwargs):
        pass

    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase):
        return f"JSON formatted {len(logical_workers)} workers"


class DummyRichFormatter(WorkerFormatter):
    """Dummy Rich formatter implementation for testing."""
    
    def __init__(self, *args, **kwargs):
        pass

    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase):
        return f"Rich formatted {len(logical_workers)} workers"


class DummyCSVFormatter(WorkerFormatter):
    """Dummy CSV formatter implementation for testing."""
    
    def __init__(self, *args, **kwargs):
        pass

    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase):
        return f"CSV formatted {len(logical_workers)} workers"


class TestWorkerFormatterFactory:
    """
    Test suite for the `WorkerFormatterFactory`.

    This class verifies that the factory properly registers, validates, instantiates,
    and handles Merlin worker formatters. It mocks built-ins for test isolation.
    """

    @pytest.fixture
    def formatter_factory(self, mocker: MockerFixture) -> WorkerFormatterFactory:
        """
        A fixture that returns a fresh instance of `WorkerFormatterFactory` with built-in formatters patched.

        Args:
            mocker: PyTest mocker fixture.

        Returns:
            A factory instance with mocked formatter classes.
        """
        mocker.patch("merlin.workers.formatters.formatter_factory.JSONWorkerFormatter", DummyJSONFormatter)
        mocker.patch("merlin.workers.formatters.formatter_factory.RichWorkerFormatter", DummyRichFormatter)
        return WorkerFormatterFactory()

    def test_list_available_formatters(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `list_available` returns the expected built-in formatter names.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        available = formatter_factory.list_available()
        assert set(available) == {"json", "rich"}

    def test_create_valid_formatter(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `create` returns a valid formatter instance for a registered name.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        json_instance = formatter_factory.create("json")
        assert isinstance(json_instance, DummyJSONFormatter)
        
        rich_instance = formatter_factory.create("rich")
        assert isinstance(rich_instance, DummyRichFormatter)

    def test_create_valid_formatter_with_alias(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that aliases are resolved to canonical formatter names.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        formatter_factory.register("csv", DummyCSVFormatter, aliases=["comma", "spreadsheet"])
        
        instance_by_name = formatter_factory.create("csv")
        instance_by_alias = formatter_factory.create("comma")
        instance_by_alias2 = formatter_factory.create("spreadsheet")
        
        assert isinstance(instance_by_name, DummyCSVFormatter)
        assert isinstance(instance_by_alias, DummyCSVFormatter)
        assert isinstance(instance_by_alias2, DummyCSVFormatter)

    def test_create_invalid_formatter_raises(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `create` raises `MerlinWorkerFormatterNotSupportedError` for unknown formatter types.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        with pytest.raises(MerlinWorkerFormatterNotSupportedError, match="unknown_formatter"):
            formatter_factory.create("unknown_formatter")

    def test_invalid_registration_type_error(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that trying to register a non-WorkerFormatter raises TypeError.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        
        class NotAFormatter:
            pass

        with pytest.raises(TypeError, match="must inherit from WorkerFormatter"):
            formatter_factory.register("fake_formatter", NotAFormatter)

    def test_register_overwrites_existing_formatter(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that registering a formatter with an existing name overwrites it.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        # Initially json should be DummyJSONFormatter
        instance1 = formatter_factory.create("json")
        assert isinstance(instance1, DummyJSONFormatter)
        
        # Register a different formatter with the same name
        formatter_factory.register("json", DummyCSVFormatter)
        
        # Should now return the new formatter type
        instance2 = formatter_factory.create("json")
        assert isinstance(instance2, DummyCSVFormatter)

    def test_list_available_includes_registered_formatters(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `list_available` includes dynamically registered formatters.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        # Initial built-in formatters
        initial_available = set(formatter_factory.list_available())
        assert initial_available == {"json", "rich"}
        
        # Register additional formatter
        formatter_factory.register("csv", DummyCSVFormatter)
        
        # Should now include the new formatter
        updated_available = set(formatter_factory.list_available())
        assert updated_available == {"json", "rich", "csv"}

    # TODO should the factory list aliases as well?
    def test_list_available_excludes_aliases(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `list_available` returns canonical names, not aliases.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        formatter_factory.register("csv", DummyCSVFormatter, aliases=["comma", "spreadsheet"])
        
        available = set(formatter_factory.list_available())
        # Should contain canonical name but not aliases
        assert "csv" in available
        assert "comma" not in available
        assert "spreadsheet" not in available

    # TODO if we change 'config' to 'kwargs' in MerlinBaseFactory class create method, uncomment this
    # def test_create_with_constructor_arguments(self, formatter_factory: WorkerFormatterFactory):
    #     """
    #     Test that `create` can pass arguments to formatter constructors.

    #     Args:
    #         formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
    #     """
    #     class ParameterizedFormatter(WorkerFormatter):
    #         def __init__(self, param1=None, param2=None):
    #             self.param1 = param1
    #             self.param2 = param2
            
    #         def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase):
    #             return f"Formatted with {self.param1} and {self.param2}"
        
    #     formatter_factory.register("parameterized", ParameterizedFormatter)
        
    #     # Test creating with arguments
    #     instance = formatter_factory.create("parameterized", param1="test", param2=42)
    #     assert isinstance(instance, ParameterizedFormatter)
    #     assert instance.param1 == "test"
    #     assert instance.param2 == 42

    def test_entry_point_group_returns_correct_namespace(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that the factory uses the correct entry point namespace.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        entry_point_group = formatter_factory._entry_point_group()
        assert entry_point_group == "merlin.workers.formatters"

    def test_validate_component_accepts_valid_formatter(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `_validate_component` accepts valid WorkerFormatter subclasses.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        # Should not raise an exception
        formatter_factory._validate_component(DummyJSONFormatter)
        formatter_factory._validate_component(DummyRichFormatter)
        formatter_factory._validate_component(DummyCSVFormatter)

    def test_validate_component_rejects_invalid_formatter(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that `_validate_component` rejects non-WorkerFormatter classes.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        class InvalidFormatter:
            pass
        
        with pytest.raises(TypeError, match="must inherit from WorkerFormatter"):
            formatter_factory._validate_component(InvalidFormatter)

    def test_raise_component_error_class_returns_correct_exception(self, formatter_factory: WorkerFormatterFactory):
        """
        Test that the factory raises the correct exception type for invalid components.

        Args:
            formatter_factory: Instance of the `WorkerFormatterFactory` for testing.
        """
        with pytest.raises(MerlinWorkerFormatterNotSupportedError, match="test message"):
            formatter_factory._raise_component_error_class("test message")

    def test_factory_instance_isolation(self, mocker: MockerFixture):
        """
        Test that different factory instances don't interfere with each other.

        Args:
            mocker: PyTest mocker fixture.
        """
        # Mock the built-ins for both factories
        mocker.patch("merlin.workers.formatters.formatter_factory.JSONWorkerFormatter", DummyJSONFormatter)
        mocker.patch("merlin.workers.formatters.formatter_factory.RichWorkerFormatter", DummyRichFormatter)
        
        factory1 = WorkerFormatterFactory()
        factory2 = WorkerFormatterFactory()
        
        # Register formatter in only one factory
        factory1.register("csv", DummyCSVFormatter)
        
        # factory1 should have the new formatter
        assert "csv" in factory1.list_available()
        csv_instance = factory1.create("csv")
        assert isinstance(csv_instance, DummyCSVFormatter)
        
        # factory2 should not have the new formatter
        assert "csv" not in factory2.list_available()
        with pytest.raises(MerlinWorkerFormatterNotSupportedError):
            factory2.create("csv")