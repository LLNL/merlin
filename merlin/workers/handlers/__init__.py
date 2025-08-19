##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Worker handler interface and implementations for Merlin task servers.

The `handlers` package defines the extensible framework for managing task server
workers in Merlin. It includes an abstract base interface, concrete implementations
for Celery and Kafka, and a factory for dynamic registration and instantiation of worker handlers.

This design allows Merlin to support multiple task server backends through a consistent
interface while enabling future integration with additional systems.

Modules:
    handler_factory.py: Factory for registering and instantiating Merlin worker
        handler implementations.
    worker_handler.py: Abstract base class that defines the interface for all Merlin
        worker handlers.
    celery_handler.py: Celery-specific implementation of the worker handler interface.
    kafka_handler.py: Kafka-specific implementation of the worker handler interface.
"""


from merlin.workers.handlers.celery_handler import CeleryWorkerHandler
from merlin.workers.handlers.kafka_handler import KafkaWorkerHandler


__all__ = ["CeleryWorkerHandler", "KafkaWorkerHandler"]
