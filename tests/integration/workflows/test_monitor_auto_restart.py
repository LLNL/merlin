"""
This module contains tests for the monitor_auto_restart_test workflow.
"""

from tests.fixture_data_classes import MonitorAutoRestartSetup


class TestMonitorAutoRestart:
    """
    Tests for the chord error workflow.
    """

    # def test_monitor_auto_restarts(self, monitor_auto_restart_setup: MonitorAutoRestartSetup):
    #     """
    #     Test that the monitor automatically restarts the workflow when:
    #     1. There are no tasks in the queues
    #     2. There are no workers processing tasks
    #     3. The workflow has not yet finished

    #     Args:
    #         monitor_auto_restart_setup: A fixture that returns a
    #             [`MonitorAutoRestartSetup`][fixture_data_classes.MonitorAutoRestartSetup] instance.
    #     """
