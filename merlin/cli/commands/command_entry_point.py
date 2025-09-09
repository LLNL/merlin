##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Defines the abstract base class for Merlin CLI commands.

This module provides the `CommandEntryPoint` abstract base class that all
Merlin command implementations must inherit from. It standardizes the interface
for adding command-specific argument parsers and processing CLI command logic.
"""

from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace


class CommandEntryPoint(ABC):
    """
    Abstract base class for a Merlin CLI command entry point.

    Methods:
        add_parser: Adds the parser for a specific command to the main `ArgumentParser`.
        process_command: Executes the logic for this CLI command.
    """

    @abstractmethod
    def add_parser(self, subparsers: ArgumentParser):
        """Add the parser for this command to the main `ArgumentParser`."""
        raise NotImplementedError("Subclasses of `CommandEntryPoint` must implement an `add_parser` method.")

    @abstractmethod
    def process_command(self, args: Namespace):
        """Execute the logic for this CLI command."""
        raise NotImplementedError("Subclasses of `CommandEntryPoint` must implement an `process_command` method.")
