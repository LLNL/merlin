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


@pytest.fixture
def factory() -> MonitorFactory:
    """
    Fixture to provide a `MonitorFactory` instance.

    Returns:
        An instance of the `MonitorFactory` object.
    """
    return MonitorFactory()


def test_get_supported_task_servers(factory: MonitorFactory):
    """
    Test that the correct list of supported task servers is returned.

    Args:
        factory: An instance of the `MonitorFactory` object.
    """
    supported = factory.get_supported_task_servers()
    assert isinstance(supported, list)
    assert "celery" in supported
    assert len(supported) == 1


def test_get_monitor_valid(factory: MonitorFactory):
    """
    Test that get_monitor returns the correct monitor for a valid task server.

    Args:
        factory: An instance of the `MonitorFactory` object.
    """
    monitor = factory.get_monitor("celery")
    assert isinstance(monitor, CeleryMonitor)


def test_get_monitor_invalid(factory: MonitorFactory):
    """
    Test that get_monitor raises an error for an unsupported task server.

    Args:
        factory: An instance of the `MonitorFactory` object.
    """
    with pytest.raises(MerlinInvalidTaskServerError) as excinfo:
        factory.get_monitor("invalid")

    assert "Task server unsupported by Merlin: invalid" in str(excinfo.value)
