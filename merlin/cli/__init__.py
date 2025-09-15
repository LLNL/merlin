##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Merlin CLI Package.

This package defines the core components and supporting utilities for the
Merlin command-line interface (CLI). It provides the entry point parser for
the `merlin` CLI tool, a suite of modular subcommands for workflow and
infrastructure management, and shared helper functions used across CLI
handlers.

Subpackages:
    commands: Contains all command implementations for the Merlin CLI, including
        workflow execution, monitoring, database interaction, and more.

Modules:
    argparse_main: Sets up the top-level argument parser and integrates all
        registered CLI subcommands into the `merlin` CLI interface.
    utils: Provides shared utility functions for parsing arguments, loading
        YAML specifications, and handling configuration logic across commands.
"""
