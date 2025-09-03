##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Worker framework for managing task execution in Merlin.

The `workers` package defines the core abstractions and implementations for launching
and managing task server workers in the Merlin workflow framework. It includes an
extensible system for defining worker behavior, instantiating worker instances, and
handling task server-specific logic (e.g., Celery).

This package supports a plugin-based architecture through factories, allowing new
task server backends to be added seamlessly via Python entry points.

Subpackages:
    - `handlers/`: Defines the interface and implementations for worker handler classes
        responsible for launching and managing groups of workers.

Modules:
    worker.py: Defines the `MerlinWorker` abstract base class, which represents a single
        task server worker and provides a common interface for launching and
        configuring worker instances.
    celery_worker.py: Implements `CeleryWorker`, a concrete subclass of `MerlinWorker` that uses
        Celery to process tasks from configured queues. Supports local and batch launch modes.
    worker_factory.py: Defines the `WorkerFactory`, which manages the registration, validation,
        and instantiation of individual worker implementations such as `CeleryWorker`.
        Supports plugin discovery via entry points.
"""

from merlin.workers.celery_worker import CeleryWorker


__all__ = ["CeleryWorker"]
