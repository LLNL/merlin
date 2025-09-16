##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `common` package provides shared utilities, classes, and logic used across Merlin.
It includes functionality for managing encryption, handling data sampling, working with
enumerations, and defining Celery tasks.

Subpackages:
    - `security/`: Contains functionality for managing encryption and ensuring secure
        communication. Includes modules for general encryption logic and encrypting backend traffic.

Modules:
    dumper.py: Provides functionality for dumping information to files.
    enums.py: Defines enumerations for interfaces.
    sample_index_factory.py: Houses factory methods for creating
        [`SampleIndex`][common.sample_index.SampleIndex] objects.
    sample_index.py: Implements the logic for managing the sample hierarchy, including
        the [`SampleIndex`][common.sample_index.SampleIndex] class.
    tasks.py: Defines Celery tasks, breaking down the Directed Acyclic Graph ([`DAG`][study.dag.DAG])
        into smaller tasks that Celery can manage.
    util_sampling.py: Contains utility functions for data sampling.
"""
