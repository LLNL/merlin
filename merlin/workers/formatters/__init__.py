##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Merlin Worker Formatters Package.

This package provides classes and utilities for formatting and displaying
Merlin worker information. Worker formatters can render logical and physical
worker data in multiple formats, including JSON for programmatic consumption
and Rich for interactive terminal visualization. The package also includes
a factory for managing supported formatter implementations.

Modules:
    formatter_factory: WorkerFormatterFactory for managing supported worker
        formatters. Allows creation by name or alias and ensures consistent
        handling of different output formats.
    json_formatter: JSONWorkerFormatter that outputs structured, machine-readable
        JSON data, including detailed logical and physical worker records,
        applied filters, timestamps, and summary statistics.
    rich_formatter: RichWorkerFormatter and related layout classes for formatting
        and displaying worker information in the terminal with responsive layouts,
        summary panels, compact views, and rich styling. Adapts to terminal width
        for optimal readability.
    worker_formatter: WorkerFormatter abstract base class defining the standard
        interface for all worker formatters.
"""
