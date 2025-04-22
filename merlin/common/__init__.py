###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

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
