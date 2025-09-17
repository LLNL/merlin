##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Configuration dataclasses for Merlin workers and batch systems.

This module provides strongly-typed configuration objects that replace
dictionary-based configurations.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Union


# pylint: disable=too-many-instance-attributes


@dataclass
class BatchConfig:
    """
    Configuration for batch job submission and execution.

    This dataclass encapsulates all batch-related configuration options
    that can be specified in a Merlin workflow specification.

    Attributes:
        type: The type of batch system to use ('local', 'slurm', 'lsf', 'flux', 'pbs').
        nodes: Number of nodes to request for the batch job.
        shell: Shell to use for command execution.
        bank: Account/bank to charge for the job.
        queue: Queue/partition to submit the job to.
        walltime: Maximum wall time for the job.
        launch_pre: Commands to run before launching the main command.
        launch_args: Additional arguments for the launch command.
        worker_launch: Custom launch command for workers.
        flux_path: Path to the Flux executable directory.
        flux_exec: Custom Flux exec command.
        flux_start_opts: Additional options for flux start command.
        flux_exec_workers: Whether to use flux exec to launch workers on all nodes.
    """

    type: str = "local"
    nodes: Optional[Union[int, str]] = None
    shell: str = "bash"
    bank: str = ""
    queue: str = ""
    walltime: str = ""
    dry_run: bool = False
    launch_pre: str = ""
    launch_args: str = ""
    worker_launch: str = ""
    flux_path: str = ""
    flux_exec: Optional[str] = None
    flux_start_opts: str = ""
    flux_exec_workers: bool = True

    def __post_init__(self):
        """Validate configuration after initialization."""
        valid_types = {"local", "slurm", "lsf", "flux", "pbs"}
        if self.type not in valid_types:
            raise ValueError(f"Invalid batch type '{self.type}'. Must be one of: {valid_types}")

        if self.nodes is not None:
            if isinstance(self.nodes, str) and self.nodes != "all":
                try:
                    self.nodes = int(self.nodes)
                except ValueError as exc:
                    raise ValueError(f"Invalid nodes value '{self.nodes}'. Must be an integer, 'all', or None.") from exc

        # Normalize flux_path
        if self.flux_path and not self.flux_path.endswith("/"):
            self.flux_path += "/"

    @classmethod
    def from_dict(cls, config_dict: Dict) -> "BatchConfig":
        """
        Create a BatchConfig from a dictionary.

        Args:
            config_dict: Dictionary containing batch configuration.

        Returns:
            BatchConfig instance with values from the dictionary.
        """
        return cls(**config_dict)

    def to_dict(self) -> Dict:
        """
        Convert BatchConfig to dictionary for backward compatibility.

        Returns:
            Dictionary representation of the configuration.
        """
        return {
            "type": self.type,
            "nodes": self.nodes,
            "shell": self.shell,
            "bank": self.bank,
            "queue": self.queue,
            "walltime": self.walltime,
            "dry_run": self.dry_run,
            "launch_pre": self.launch_pre,
            "launch_args": self.launch_args,
            "worker_launch": self.worker_launch,
            "flux_path": self.flux_path,
            "flux_exec": self.flux_exec,
            "flux_start_opts": self.flux_start_opts,
            "flux_exec_workers": self.flux_exec_workers,
        }

    def is_parallel(self) -> bool:
        """
        Check if this configuration enables parallel execution.

        Returns:
            True if batch type is not 'local'.
        """
        return self.type != "local"

    def merge(self, other: "BatchConfig") -> "BatchConfig":
        """
        Merge this configuration with another, with other taking precedence.

        Args:
            other: BatchConfig to merge with this one.

        Returns:
            New BatchConfig with merged values.
        """
        merged_dict = self.to_dict()
        other_dict = other.to_dict()

        # Only override non-empty/non-None values
        for key, value in other_dict.items():
            if value not in (None, "", []):
                merged_dict[key] = value

        return BatchConfig.from_dict(merged_dict)


@dataclass
class WorkerConfig:
    """
    Configuration for Merlin workers.

    This dataclass encapsulates all worker-related configuration options
    including queues, machines, batch settings, and launch arguments.

    Attributes:
        name: Name of the worker.
        args: Command-line arguments for the worker process.
        queues: Set of queue names this worker should process.
        batch: Batch configuration for this worker.
        machines: List of machine names where this worker can run.
        nodes: Number of nodes to use (can override batch.nodes).
        overlap: Whether this worker can overlap queues with other workers.
        env: Environment variables for the worker process.
    """

    name: str
    args: str = ""
    queues: Set[str] = field(default_factory=lambda: {"[merlin]_merlin"})
    batch: BatchConfig = field(default_factory=BatchConfig)
    machines: List[str] = field(default_factory=list)
    nodes: Optional[Union[int, str]] = None
    overlap: bool = False
    env: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.name:
            raise ValueError("Worker name cannot be empty")

        if not isinstance(self.queues, set):
            if isinstance(self.queues, (list, tuple)):
                self.queues = set(self.queues)
            else:
                raise ValueError("queues must be a set, list, or tuple")

        if self.nodes is not None:
            if isinstance(self.nodes, str) and self.nodes != "all":
                try:
                    self.nodes = int(self.nodes)
                except ValueError as exc:
                    raise ValueError(f"Invalid nodes value '{self.nodes}'. Must be an integer, 'all', or None.") from exc

        if not self.env:
            self.env = os.environ.copy()

    @classmethod
    # def from_dict(cls, name: str, config_dict: Dict, env: Dict[str, str] = None) -> 'WorkerConfig':
    def from_dict(cls, config_dict: Dict) -> "WorkerConfig":
        """
        Create a WorkerConfig from a dictionary.

        Args:
            config_dict: Dictionary containing worker configuration.

        Returns:
            WorkerConfig instance with values from the dictionary.
        """
        # Extract batch configuration if present
        batch_dict = config_dict.get("batch", {})
        batch_config = BatchConfig.from_dict(batch_dict) if batch_dict else BatchConfig()

        # Convert queues to set if needed
        queues = config_dict.get("queues", {"[merlin]_merlin"})
        if isinstance(queues, (list, tuple)):
            queues = set(queues)
        elif not isinstance(queues, set):
            queues = {queues} if isinstance(queues, str) else {"[merlin]_merlin"}

        return cls(
            name=config_dict["name"],  # Not using `get` since this should fail if 'name' is missing
            args=config_dict.get("args", ""),
            queues=queues,
            batch=batch_config,
            machines=config_dict.get("machines", []),
            nodes=config_dict.get("nodes"),
            overlap=config_dict.get("overlap", False),
            env=config_dict.get("env", {}),
        )

    def to_dict(self) -> Dict:
        """
        Convert WorkerConfig to dictionary for backward compatibility.

        Returns:
            Dictionary representation of the configuration.
        """
        return {
            "name": self.name,
            "args": self.args,
            "queues": list(self.queues),
            "batch": self.batch.to_dict(),
            "machines": self.machines,
            "nodes": self.nodes,
            "overlap": self.overlap,
            "env": self.env,
        }

    def get_effective_nodes(self) -> Optional[Union[int, str]]:
        """
        Get the effective node count, preferring worker-specific over batch config.

        Returns:
            Node count to use, or None if not specified.
        """
        return self.nodes if self.nodes is not None else self.batch.nodes

    def get_effective_batch_config(self) -> BatchConfig:
        """
        Get the effective batch configuration with worker-specific overrides.

        Returns:
            BatchConfig with worker-specific values applied.
        """
        if self.nodes is not None:
            # Create a copy of batch config with worker's node override
            batch_dict = self.batch.to_dict()
            batch_dict["nodes"] = self.nodes
            return BatchConfig.from_dict(batch_dict)
        return self.batch

    def has_machine_restrictions(self) -> bool:
        """
        Check if this worker has machine restrictions.

        Returns:
            True if machines list is not empty.
        """
        return bool(self.machines)

    def add_queue(self, queue_name: str):
        """
        Add a queue to this worker's queue set.

        Args:
            queue_name: Name of the queue to add.
        """
        self.queues.add(queue_name)

    def remove_queue(self, queue_name: str):
        """
        Remove a queue from this worker's queue set.

        Args:
            queue_name: Name of the queue to remove.
        """
        self.queues.discard(queue_name)

    def update_env(self, env_updates: Dict[str, str]):
        """
        Update environment variables for this worker.

        Args:
            env_updates: Dictionary of environment variable updates.
        """
        self.env.update(env_updates)
