##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides the `BatchManager` class for handling batch job scheduling.
"""
import logging
import os
import subprocess
from typing import Dict, Union

from merlin.study.configurations import BatchConfig
from merlin.utils import convert_timestring, get_flux_alloc, get_flux_version


LOG = logging.getLogger(__name__)


# TODO should the scheduler logic be offloaded to script adapters? It's a bit of a different
# use case than the script adapters are intended for...
class BatchManager:
    """
    Manages batch job scheduling and worker launching across different schedulers.

    This class provides methods for detecting available schedulers, parsing batch
    configurations, and constructing appropriate launch commands for different
    batch systems including Slurm, LSF, Flux, and PBS.

    Attributes:
        batch_config (BatchConfig): The batch configuration object.
        scheduler_legend (Dict): Dictionary containing scheduler-specific information.
        detected_scheduler (str): The automatically detected scheduler type.
    """

    def __init__(self, batch_config: BatchConfig = None):
        """
        Initialize the BatchManager with a batch configuration.

        Args:
            batch_config: BatchConfig object containing batch configuration settings.
                If None, a default BatchConfig will be created.
        """
        self.batch_config = batch_config or BatchConfig()
        self.scheduler_legend = {}
        self.detected_scheduler = None

        # Initialize Flux-specific attributes
        self._flux_exe = None
        self._flux_alloc = None
        self._init_flux_config()

    def _init_flux_config(self):
        """Initialize Flux-specific configuration."""
        flux_path = self.batch_config.flux_path
        if flux_path and not flux_path.endswith("/"):
            flux_path += "/"

        self._flux_exe = os.path.join(flux_path, "flux")

        try:
            self._flux_alloc = get_flux_alloc(self._flux_exe)
        except FileNotFoundError as e:
            LOG.debug(e)
            self._flux_alloc = ""

    def is_parallel(self) -> bool:
        """
        Check if this batch configuration is set up for parallel execution.

        Returns:
            True if batch type is not 'local', indicating parallel processing.
        """
        return self.batch_config.is_parallel()

    def check_scheduler(self, scheduler: str) -> bool:
        """
        Check if a specific scheduler is available on the system.

        Args:
            scheduler: Name of the scheduler to check ('flux', 'slurm', 'lsf', 'pbs').

        Returns:
            True if the scheduler is available, False otherwise.
        """
        if scheduler not in ("flux", "slurm", "lsf", "pbs"):
            LOG.warning(f"Invalid scheduler {scheduler} given to check_scheduler.")
            return False

        # Ensure scheduler legend is populated
        if not self.scheduler_legend:
            self._build_scheduler_legend()

        try:
            process = subprocess.run(self.scheduler_legend[scheduler]["check cmd"], capture_output=True)

            if process.stdout:
                expected_output = self.scheduler_legend[scheduler]["expected check output"]
                lines = process.stdout.splitlines()
                if lines and expected_output in lines[0]:
                    return True
            return False
        except (FileNotFoundError, PermissionError):
            return False

    def detect_scheduler(self, default: str = None) -> str:
        """
        Automatically detect which batch scheduler is available.

        Args:
            default: Default scheduler to return if none are detected.

        Returns:
            Name of the detected scheduler or the default value.
        """
        if self.detected_scheduler is not None:
            return self.detected_scheduler

        # Build scheduler legend if not already done
        if not self.scheduler_legend:
            self._build_scheduler_legend()

        # Check schedulers in priority order
        schedulers_to_check = ["flux", "pbs", "lsf", "slurm"]
        for scheduler in schedulers_to_check:
            LOG.debug(f"check for {scheduler} = {self.check_scheduler(scheduler)}")
            if self.check_scheduler(scheduler):
                self.detected_scheduler = scheduler
                return scheduler

        # Check environment variables for system type
        sys_type = os.environ.get("SYS_TYPE", "")
        if "toss_3" in sys_type:
            self.detected_scheduler = "slurm"
            return "slurm"

        if "blueos" in sys_type:
            self.detected_scheduler = "lsf"
            return "lsf"

        self.detected_scheduler = default
        return default

    def _get_node_count(self, default: int = 1) -> int:
        """
        Determine node count based on environment and scheduler.

        Args:
            default: Default node count if none can be determined.

        Returns:
            Number of nodes to use for the batch job.

        Raises:
            ValueError: If Flux version is too old.
        """
        # Flux version check
        flux_ver = get_flux_version(self._flux_exe, no_errors=True)
        if flux_ver:
            major, minor, _ = map(int, flux_ver.split("."))
            if major < 1 and minor < 17:
                raise ValueError("Flux version is too old. Supported versions are 0.17.0+.")

        # Try to get node count from Flux
        try:
            get_size_proc = subprocess.run("flux getattr size", shell=True, capture_output=True, text=True)
            return int(get_size_proc.stdout)
        except (FileNotFoundError, PermissionError, ValueError):
            pass

        # Check Slurm environment
        if "SLURM_JOB_NUM_NODES" in os.environ:
            return int(os.environ["SLURM_JOB_NUM_NODES"])

        # Check LSF environment
        if "LSB_HOSTS" in os.environ:
            nodes = set(os.environ["LSB_HOSTS"].split())
            return len(nodes) - 1
        if "LSB_MCPU_HOSTS" in os.environ:
            nodes = os.environ["LSB_MCPU_HOSTS"].split()
            return len(nodes) // 2 - 1

        return default

    def _build_scheduler_legend(self, nodes: int = None):
        """
        Build the scheduler legend with configuration for all supported schedulers.

        Args:
            nodes: Number of nodes for the launch command. If None, will attempt
                to determine automatically.
        """
        if nodes is None:
            nodes = self._get_node_count(default=1)

        self.scheduler_legend = {
            "flux": {
                "bank": f" --setattr=system.bank={self.batch_config.bank}",
                "check cmd": ["flux", "resource", "info"],
                "expected check output": b"Nodes",
                "launch": f"{self._flux_alloc} -o pty -N {nodes} --exclusive --job-name=merlin",
                "queue": f" --setattr=system.queue={self.batch_config.queue}",
                "walltime": f" -t {convert_timestring(self.batch_config.walltime, format_method='FSD')}",
            },
            "lsf": {
                "check cmd": ["jsrun", "--help"],
                "expected check output": b"jsrun",
                "launch": f"jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n {nodes}",
            },
            "pbs": {
                "bank": f" -A {self.batch_config.bank}",
                "check cmd": ["qsub", "--version"],
                "expected check output": b"pbs_version",
                "launch": f"qsub -l nodes={nodes}",
                "queue": f" -q {self.batch_config.queue}",
                "walltime": f" -l walltime={convert_timestring(self.batch_config.walltime)}",
            },
            "slurm": {
                "bank": f" -A {self.batch_config.bank}",
                "check cmd": ["sbatch", "--help"],
                "expected check output": b"sbatch",
                "launch": f"srun -N {nodes} -n {nodes}",
                "queue": f" -p {self.batch_config.queue}",
                "walltime": f" -t {convert_timestring(self.batch_config.walltime)}",
            },
        }

    def _get_flux_launch_command(self, existing_launch_cmd: str) -> str:
        """
        Build the Flux-specific launch command.

        Args:
            existing_launch_cmd: The existing launch command or an empty string.

        Returns:
            Flux launch command string.
        """
        default_flux_exec = "flux exec" if existing_launch_cmd else f"{self._flux_exe} exec"
        flux_exec = ""

        if self.batch_config.flux_exec_workers:
            flux_exec = self.batch_config.flux_exec if self.batch_config.flux_exec else default_flux_exec

        if existing_launch_cmd and "flux" not in existing_launch_cmd:
            launch = (
                f"{existing_launch_cmd} {self._flux_exe}"
                f" start {self.batch_config.flux_start_opts} {flux_exec} `which {self.batch_config.shell}` -c"
            )
        else:
            launch = f"{existing_launch_cmd} {flux_exec} `which {self.batch_config.shell}` -c"

        return launch

    def _construct_launch_command(self, nodes: int) -> str:
        """
        Construct the base launch command for the detected scheduler.

        Args:
            nodes: Number of nodes to use.

        Returns:
            The constructed launch command.

        Raises:
            TypeError: If PBS scheduler is used with non-flux batch type.
            KeyError: If workload manager is not found in scheduler legend.
        """
        # Build scheduler legend with the specified nodes
        self._build_scheduler_legend(nodes)

        # Detect the workload manager
        workload_manager = self.detect_scheduler()

        LOG.debug(f"batch_config: {self.batch_config}")

        if self.batch_config.type == "pbs" and workload_manager == self.batch_config.type:
            raise TypeError("The PBS scheduler is only enabled for 'batch: flux' type")

        if self.batch_config.type == "slurm" and workload_manager not in ("lsf", "flux", "pbs"):
            workload_manager = "slurm"

        LOG.debug(f"workload_manager: {workload_manager}")

        try:
            launch_command = self.scheduler_legend[workload_manager]["launch"]
        except KeyError as e:
            LOG.debug(e)
            launch_command = ""

        # If LSF is the workload manager we stop here
        if workload_manager != "lsf" and launch_command:
            # Add bank, queue, and walltime as necessary
            for key in ("bank", "queue", "walltime"):
                config_value = getattr(self.batch_config, key)
                if config_value:
                    try:
                        launch_command += self.scheduler_legend[workload_manager][key]
                    except KeyError as e:
                        LOG.error(e)

            # PBS-specific modification
            if workload_manager == "pbs":
                launch_command += " --"

        return launch_command

    def create_worker_launch_command(self, command: str, nodes: Union[str, int] = None) -> str:
        """
        Create the complete worker launch command.

        Args:
            command: The base command to be launched.
            nodes: Number of nodes to use. Can be an integer, "all", or None.
                If None, will use the batch configuration value.

        Returns:
            Complete launch command ready for execution.

        Raises:
            TypeError: If nodes parameter is invalid or PBS scheduler is misconfigured.
        """
        # Handle local or LSF batch types
        if self.batch_config.type == "local" or "lsf" in self.batch_config.type:
            return command

        # Determine node count
        if nodes is None:
            nodes = self.batch_config.nodes

        if nodes is None or nodes == "all":
            nodes = self._get_node_count(default=1)
        elif not isinstance(nodes, int):
            if isinstance(nodes, str) and nodes != "all":
                raise TypeError("Nodes was passed with an invalid string value (only 'all' is supported).")
            if not isinstance(nodes, str):
                raise TypeError("Nodes parameter must be an integer, 'all', or None.")

        # Build launch command if not provided
        launch_command = self.batch_config.worker_launch
        if not launch_command:
            launch_command = self._construct_launch_command(nodes)

        # Add launch arguments
        if self.batch_config.launch_args:
            launch_command += f" {self.batch_config.launch_args}"

        # Add pre-launch commands
        if self.batch_config.launch_pre:
            launch_command = f"{self.batch_config.launch_pre} {launch_command}"

        LOG.debug(f"launch command: {launch_command}")

        # Add Flux-specific launch settings
        if self.batch_config.type == "flux":
            launch_command = self._get_flux_launch_command(launch_command)

        # Construct final worker command
        worker_cmd = f"{launch_command} {command}"

        return worker_cmd

    def get_batch_info(self) -> Dict:
        """
        Get information about the current batch configuration.

        Returns:
            Dictionary containing batch configuration details.
        """
        batch_info = self.batch_config.to_dict()
        batch_info["is_parallel"] = self.is_parallel()
        batch_info["detected_scheduler"] = self.detect_scheduler()
        return batch_info

    def update_config(self, new_config: Union[Dict, BatchConfig]):
        """
        Update the batch configuration and reset cached values.

        Args:
            new_config: New batch configuration (Dict or BatchConfig).
        """
        if isinstance(new_config, dict):
            new_config = BatchConfig.from_dict(new_config)
            self.batch_config = self.batch_config.merge(new_config)
        elif isinstance(new_config, BatchConfig):
            self.batch_config = new_config
        else:
            raise TypeError("new_config must be a Dict or BatchConfig instance")

        # Reset cached values
        self.scheduler_legend = {}
        self.detected_scheduler = None
        self._init_flux_config()
