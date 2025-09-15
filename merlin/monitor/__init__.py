##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `monitor` package provides classes and utilities for monitoring the health and progress
of task servers and workflows in Merlin. It includes abstract interfaces, concrete implementations,
and factory classes to manage task server monitors for supported systems like Celery.

Modules:
    monitor_factory.py: Provides a factory class to manage and retrieve task server monitors for
        supported task servers.
    monitor.py: Defines the [`Monitor`][monitor.monitor.Monitor] class for monitoring the progress
        of Merlin workflows, ensuring workers and tasks are functioning correctly and preventing
        workflow hangs.
    task_server_monitor.py: Defines the [`TaskServerMonitor`][monitor.task_server_monitor.TaskServerMonitor]
        abstract base class, which serves as a common interface for monitoring task servers, including
        methods for worker and task monitoring.
    celery_monitor.py: Implements the [`CeleryMonitor`][monitor.celery_monitor.CeleryMonitor] class,
        a concrete subclass of [`TaskServerMonitor`][monitor.task_server_monitor.TaskServerMonitor]
        for monitoring Celery task servers, including worker health checks and task queue monitoring.
"""
