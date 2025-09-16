##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `spec` package defines the structure, defaults, and functionality for working with
Merlin specification files.

Modules:
    all_keys.py: Defines all the valid keys for each block in a Merlin specification file,
        ensuring consistency and validation.
    defaults.py: Provides the default values for each block in a spec file, enabling workflows
        to execute even when fields are omitted.
    expansion.py: Handles the expansion of variables within a spec file, including user-defined,
        environment, and reserved variables, as well as parameter substitutions.
    override.py: Supports overriding variables in a spec file via the command-line interface,
        with functions for validation and replacement.
    specification.py: Contains the `MerlinSpec` class, which represents the raw data from a
        Merlin specification file and provides methods for interacting with it.
"""
