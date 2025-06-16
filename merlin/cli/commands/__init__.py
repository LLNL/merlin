"""

"""

from merlin.cli.commands.config import ConfigCommand
from merlin.cli.commands.database import DatabaseCommand
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
    DatabaseCommand(),
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
