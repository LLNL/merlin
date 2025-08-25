##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for the Merlin task server abstraction layer.

This package contains tests for:
- TaskServerInterface abstract base class
- TaskServerFactory registration and creation
- Task server implementations (Celery, Kafka, etc.)
- Configuration validation and error handling
""" 