##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `monitor_factory.py` module.
"""

import pytest

from merlin.exceptions import MerlinInvalidTaskServerError
from merlin.monitor.celery_monitor import CeleryMonitor
from merlin.monitor.monitor_factory import MonitorFactory


# @pytest.fixture
# def factory() -> MonitorFactory:
#     """
#     Fixture to provide a `MonitorFactory` instance.

#     Returns:
#         An instance of the `MonitorFactory` object.
#     """
#     return MonitorFactory()


# def test_get_supported_task_servers(factory: MonitorFactory):
#     """
#     Test that the correct list of supported task servers is returned.

#     Args:
#         factory: An instance of the `MonitorFactory` object.
#     """
#     supported = factory.get_supported_task_servers()
#     assert isinstance(supported, list)
#     assert "celery" in supported
#     assert len(supported) == 1


# def test_get_monitor_valid(factory: MonitorFactory):
#     """
#     Test that get_monitor returns the correct monitor for a valid task server.

#     Args:
#         factory: An instance of the `MonitorFactory` object.
#     """
#     monitor = factory.get_monitor("celery")
#     assert isinstance(monitor, CeleryMonitor)


# def test_get_monitor_invalid(factory: MonitorFactory):
#     """
#     Test that get_monitor raises an error for an unsupported task server.

#     Args:
#         factory: An instance of the `MonitorFactory` object.
#     """
#     with pytest.raises(MerlinInvalidTaskServerError) as excinfo:
#         factory.get_monitor("invalid")

#     assert "Task server unsupported by Merlin: invalid" in str(excinfo.value)


class TestMonitorFactory:
    """
    Test suite for the `MonitorFactory`.

    This class validates that the factory properly registers and instantiates
    task server monitor classes (like `CeleryMonitor`), resolves component names,
    and raises appropriate errors for unsupported types. These tests focus
    on the behavior of the generic factory logic applied to monitor components.
    """

    @pytest.fixture
    def monitor_factory(self) -> MonitorFactory:
        """
        Create a fresh `MonitorFactory` instance for each test.

        Returns:
            A new instance of `MonitorFactory`.
        """
        return MonitorFactory()

    def test_list_available_monitors(self, monitor_factory: MonitorFactory):
        """
        Test that `list_available` returns registered task server monitors.

        Args:
            monitor_factory: Instance of `MonitorFactory` for testing.
        """
        available = monitor_factory.list_available()
        assert "celery" in available
        assert len(available) == 1

    def test_create_valid_monitor(self, monitor_factory: MonitorFactory):
        """
        Test that `create` instantiates a monitor for a valid task server.

        Args:
            monitor_factory: Instance of `MonitorFactory` for testing.
        """
        monitor = monitor_factory.create("celery")
        assert isinstance(monitor, CeleryMonitor)

    def test_create_invalid_monitor_raises(self, monitor_factory: MonitorFactory):
        """
        Test that creating a monitor for an unknown task server raises error.

        Args:
            monitor_factory: Instance of `MonitorFactory` for testing.
        """
        with pytest.raises(MerlinInvalidTaskServerError, match="not supported"):
            monitor_factory.create("invalid_task_server")

    def test_register_invalid_component_type(self, monitor_factory: MonitorFactory):
        """
        Test that registering a non-TaskServerMonitor subclass raises TypeError.

        Args:
            monitor_factory: Instance of `MonitorFactory` for testing.
        """

        class NotAMonitor:
            pass

        with pytest.raises(TypeError, match="must inherit from TaskServerMonitor"):
            monitor_factory.register("invalid", NotAMonitor)

    def test_get_component_info_returns_expected_metadata(self, monitor_factory: MonitorFactory):
        """
        Test that `get_component_info` returns metadata about a monitor class.

        Args:
            monitor_factory: Instance of `MonitorFactory` for testing.
        """
        info = monitor_factory.get_component_info("celery")
        assert info["name"] == "celery"
        assert info["class"] == "CeleryMonitor"
        assert "description" in info

    def test_get_component_info_for_unknown_component_raises(self, monitor_factory: MonitorFactory):
        """
        Test that `get_component_info` raises for unknown component types.

        Args:
            monitor_factory: Instance of `MonitorFactory` for testing.
        """
        with pytest.raises(MerlinInvalidTaskServerError, match="not supported"):
            monitor_factory.get_component_info("unknown")
