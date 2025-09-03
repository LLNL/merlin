##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Merlin CLI Commands Package.

This package defines all top-level and subcommand implementations for the Merlin
command-line interface. Each module encapsulates the logic and argument parsing
for a distinct Merlin command, following a consistent structure built around the
`CommandEntryPoint` interface.

Modules:
    command_entry_point: Defines the abstract base class `CommandEntryPoint` for all CLI commands.
    config: Implements the `config` command for managing Merlin configuration files.
    database: Implements the `database` command for interacting with the underlying database (view, delete, inspect).
    example: Implements the `example` command to download and set up example workflows.
    info: Implements the `info` command for displaying configuration and environment diagnostics.
    monitor: Implements the `monitor` command to keep workflow allocations alive.
    purge: Implements the `purge` command for removing tasks from queues.
    query_workers: Implements the `query-workers` command for inspecting active task server workers.
    queue_info: Implements the `queue-info` command for querying task server queue statistics.
    restart: Implements the `restart` command to resume a workflow from a previous state.
    run_workers: Implements the `run-workers` command to launch task-executing workers.
    run: Implements the `run` command to execute Merlin or Maestro workflows.
    server: Implements the `server` command to manage containerized Redis server components.
    status: Implements the `status` and `detailed-status` commands for workflow state inspection.
    stop_workers: Implements the `stop-workers` command for terminating active workers.
"""

from merlin.cli.commands.config import ConfigCommand
from merlin.cli.commands.example import ExampleCommand
from merlin.cli.commands.info import InfoCommand
from merlin.cli.commands.monitor import MonitorCommand
from merlin.cli.commands.purge import PurgeCommand
from merlin.cli.commands.query_workers import QueryWorkersCommand
from merlin.cli.commands.queue_info import QueueInfoCommand
from merlin.cli.commands.restart import RestartCommand
from merlin.cli.commands.run import RunCommand
from merlin.cli.commands.run_workers import RunWorkersCommand
from merlin.cli.commands.server import ServerCommand
from merlin.cli.commands.status import DetailedStatusCommand, StatusCommand
from merlin.cli.commands.stop_workers import StopWorkersCommand


# Keep these in alphabetical order
ALL_COMMANDS = [
    ConfigCommand(),
    DetailedStatusCommand(),
    ExampleCommand(),
    InfoCommand(),
    MonitorCommand(),
    PurgeCommand(),
    QueryWorkersCommand(),
    QueueInfoCommand(),
    RestartCommand(),
    RunCommand(),
    RunWorkersCommand(),
    ServerCommand(),
    StatusCommand(),
    StopWorkersCommand(),
]
