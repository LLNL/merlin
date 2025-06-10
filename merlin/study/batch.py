##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module parses the batch section of the yaml specification.

Currently only the batch worker launch for slurm, lsf or flux
are implemented.
"""
import logging
import os
import subprocess
from typing import Dict, Union

from merlin.spec.specification import MerlinSpec
from merlin.utils import convert_timestring, get_flux_alloc, get_flux_version, get_yaml_var


LOG = logging.getLogger(__name__)


def batch_check_parallel(spec: MerlinSpec) -> bool:
    """
    Check for a parallel batch section in the provided MerlinSpec object.

    This function examines the 'batch' section of the given specification to determine
    whether it is configured for parallel execution. It checks the 'type' attribute
    within the batch section, defaulting to 'local' if not specified. If the type
    is anything other than 'local', the function will return True, indicating that
    parallel processing is enabled.

    Args:
        spec (spec.specification.MerlinSpec): An instance of the
            [`MerlinSpec`][spec.specification.MerlinSpec] class that contains the
            configuration details, including the batch section.

    Returns:
        Returns True if the batch type is set to a value other than 'local',
            indicating that parallel processing is enabled; otherwise, returns False.

    Raises:
        AttributeError: If the 'batch' section is not present in the specification,
            an error is logged and an AttributeError is raised.
    """
    parallel = False

    try:
        batch = spec.batch
    except AttributeError as exc:
        LOG.error("The batch section is required in the specification file.")
        raise exc

    btype = get_yaml_var(batch, "type", "local")
    if btype != "local":
        parallel = True

    return parallel


def check_for_scheduler(scheduler: str, scheduler_legend: Dict[str, str]) -> bool:
    """
    Check which scheduler (Flux, Slurm, LSF, or PBS) is the main scheduler for the cluster.

    This function verifies if the specified scheduler is the main scheduler by executing
    a command associated with it from the provided scheduler legend. It returns a boolean
    indicating whether the specified scheduler is active.

    Args:
        scheduler: A string representing the scheduler to check for. Options include 'flux',
            'slurm', 'lsf', or 'pbs'.
        scheduler_legend: A dictionary containing information related to each scheduler,
            including the command to check its status and the expected output. See
            [`construct_scheduler_legend`][study.batch.construct_scheduler_legend]
            for more information on all the settings this dict contains.

    Returns:
        Returns True if the specified scheduler is the main scheduler for the
            cluster, otherwise returns False.

    Raises:
        FileNotFoundError: If the command associated with the scheduler cannot be found.
        PermissionError: If there are insufficient permissions to execute the command.
    """
    # Check for invalid scheduler
    if scheduler not in ("flux", "slurm", "lsf", "pbs"):
        LOG.warning(f"Invalid scheduler {scheduler} given to check_for_scheduler.")
        return False

    # Try to run the check command provided via the scheduler legend
    try:
        process = subprocess.Popen(  # pylint: disable=R1732
            scheduler_legend[scheduler]["check cmd"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # If the desired output exists, return True. Otherwise, return False
        result = process.stdout.readlines()
        if result and len(result) > 0 and scheduler_legend[scheduler]["expected check output"] in result[0]:
            return True
        return False
    except (FileNotFoundError, PermissionError):
        return False


def get_batch_type(scheduler_legend: Dict[str, str], default: str = None) -> str:
    """
    Determine which batch scheduler to use.

    This function checks a predefined list of batch schedulers in a specific order
    to determine which one is available for use. If none of the schedulers are found,
    it checks the system type environment variable to suggest a default scheduler.
    If no suitable scheduler is determined, it returns the specified default value.

    Args:
        scheduler_legend: A dictionary storing information related to each
            scheduler, including commands and expected outputs for checking their
            availability. See [`construct_scheduler_legend`][study.batch.construct_scheduler_legend]
            for more information on all the settings this dict contains.
        default: The default batch scheduler to use if a scheduler cannot be determined.

    Returns:
        The name of the available batch scheduler. Possible options include
            'slurm', 'flux', 'lsf', or 'pbs'. If no scheduler is found, returns
            the specified default value.
    """
    # These schedulers are listed in order of which should be checked for first
    # 1. Flux should be checked first due to slurm emulation scripts
    # 2. PBS should be checked before slurm for testing
    # 3. LSF should be checked before slurm for testing
    # 4. Slurm should be checked last
    schedulers_to_check = ["flux", "pbs", "lsf", "slurm"]
    for scheduler in schedulers_to_check:
        LOG.debug(f"check for {scheduler} = {check_for_scheduler(scheduler, scheduler_legend)}")
        if check_for_scheduler(scheduler, scheduler_legend):
            return scheduler

    SYS_TYPE = os.environ.get("SYS_TYPE", "")  # pylint: disable=C0103
    if "toss_3" in SYS_TYPE:
        return "slurm"

    if "blueos" in SYS_TYPE:
        return "lsf"

    return default


def get_node_count(parsed_batch: Dict, default: int = 1) -> int:
    """
    Determine a default node count based on the environment.

    This function checks the environment and the Flux version to determine the
    appropriate number of nodes to use for batch processing. It first verifies
    the Flux version, then attempts to retrieve the node count from the Flux
    allocation or environment variables specific to Slurm or LSF. If no valid
    node count can be determined, it returns a specified default value.

    Args:
        parsed_batch: A dictionary containing parsed batch configurations.
            See [`parse_batch_block`][study.batch.parse_batch_block] for more
            information on all the settings in this dictionary.
        default: The number of nodes to return if a node count from the
            environment cannot be determined.

    Returns:
        The number of nodes to use for the batch job. This value is determined
            based on the environment and scheduler specifics.

    Raises:
        ValueError: If the Flux version is too old (below 0.17.0).
    """

    # Flux version check
    flux_ver = get_flux_version(parsed_batch["flux exe"], no_errors=True)
    major, minor, _ = map(int, flux_ver.split("."))
    if major < 1 and minor < 17:
        raise ValueError("Flux version is too old. Supported versions are 0.17.0+.")

    # If flux is the scheduler, we can get the size of the allocation with this
    try:
        get_size_proc = subprocess.run("flux getattr size", shell=True, capture_output=True, text=True)
        return int(get_size_proc.stdout)
    except Exception:
        pass

    if "SLURM_JOB_NUM_NODES" in os.environ:
        return int(os.environ["SLURM_JOB_NUM_NODES"])

    # LSB systems reserve one node for launching
    if "LSB_HOSTS" in os.environ:
        nodes = set(os.environ["LSB_HOSTS"].split())
        n_batch_nodes = len(nodes) - 1
        return n_batch_nodes
    if "LSB_MCPU_HOSTS" in os.environ:
        nodes = os.environ["LSB_MCPU_HOSTS"].split()
        n_batch_nodes = len(nodes) // 2 - 1
        return n_batch_nodes

    return default


def parse_batch_block(batch: Dict) -> Dict:
    """
    Parse the batch block of a YAML configuration file.

    This function extracts relevant information from the provided batch block
    dictionary, including paths, execution options, and defaults. It retrieves
    the Flux executable path and allocation details, and populates a dictionary
    with the parsed values.

    Args:
        batch: A dictionary representing the batch block from the YAML
            configuration file.

    Returns:
        A dictionary containing parsed information from the batch block,
            including:\n
            - `btype`: The type of batch job (default is 'local').
            - `nodes`: The number of nodes to use (default is None).
            - `shell`: The shell to use (default is 'bash').
            - `bank`: The bank to charge for the job (default is an empty string).
            - `queue`: The queue to submit the job to (default is an empty string).
            - `walltime`: The maximum wall time for the job (default is an empty string).
            - `launch pre`: Any commands to run before launching (default is an empty string).
            - `launch args`: Arguments for the launch command (default is an empty string).
            - `launch command`: Custom command to launch workers. This will override the
                default launch command (default is an empty string).
            - `flux path`: Optional path to flux bin.
            - `flux exe`: The full path to the Flux executable.
            - `flux exec`: Optional flux exec command to launch workers on all nodes if
                `flux_exec_workers` is True (default is None).
            - `flux alloc`: The Flux allocation retrieved from the executable.
            - `flux opts`: Optional flux start options (default is an empty string).
            - `flux exec workers`: Optional flux argument to launch workers
                on all nodes (default is True).
    """
    flux_path: str = get_yaml_var(batch, "flux_path", "")
    if "/" in flux_path:
        flux_path += "/"

    flux_exe: str = os.path.join(flux_path, "flux")
    flux_alloc: str
    try:
        flux_alloc = get_flux_alloc(flux_exe)
    except FileNotFoundError as e:  # pylint: disable=C0103
        LOG.debug(e)
        flux_alloc = ""

    parsed_batch = {
        "btype": get_yaml_var(batch, "type", "local"),
        "nodes": get_yaml_var(batch, "nodes", None),
        "shell": get_yaml_var(batch, "shell", "bash"),
        "bank": get_yaml_var(batch, "bank", ""),
        "queue": get_yaml_var(batch, "queue", ""),
        "walltime": get_yaml_var(batch, "walltime", ""),
        "launch pre": get_yaml_var(batch, "launch_pre", ""),
        "launch args": get_yaml_var(batch, "launch_args", ""),
        "launch command": get_yaml_var(batch, "worker_launch", ""),
        "flux path": flux_path,
        "flux exe": flux_exe,
        "flux exec": get_yaml_var(batch, "flux_exec", None),
        "flux alloc": flux_alloc,
        "flux opts": get_yaml_var(batch, "flux_start_opts", ""),
        "flux exec workers": get_yaml_var(batch, "flux_exec_workers", True),
    }
    return parsed_batch


def get_flux_launch(parsed_batch: Dict) -> str:
    """
    Build the Flux launch command based on the batch section of the YAML configuration.

    This function constructs the command to launch a Flux job using the parameters
    specified in the parsed batch configuration. It determines the appropriate
    execution command for Flux workers and integrates it with the launch command
    provided in the batch configuration.

    Args:
        parsed_batch: A dictionary containing batch configuration parameters.
            See [`parse_batch_block`][study.batch.parse_batch_block] for more information
            on all the settings in this dictionary.

    Returns:
        The constructed Flux launch command, ready to be executed.
    """
    default_flux_exec = "flux exec" if parsed_batch["launch command"] else f"{parsed_batch['flux exe']} exec"
    flux_exec: str = ""
    if parsed_batch["flux exec workers"]:
        flux_exec = parsed_batch["flux exec"] if parsed_batch["flux exec"] else default_flux_exec

    if parsed_batch["launch command"] and "flux" not in parsed_batch["launch command"]:
        launch: str = (
            f"{parsed_batch['launch command']} {parsed_batch['flux exe']}"
            f" start {parsed_batch['flux opts']} {flux_exec} `which {parsed_batch['shell']}` -c"
        )
    else:
        launch: str = f"{parsed_batch['launch command']} {flux_exec} `which {parsed_batch['shell']}` -c"

    return launch


def batch_worker_launch(
    spec: MerlinSpec,
    com: str,
    nodes: Union[str, int] = None,
    batch: Dict = None,
) -> str:
    """
    Create the worker launch command based on the batch configuration in the
    workflow specification.

    This function constructs a command to launch a worker process using the
    specified batch configuration. It handles different batch types and
    integrates any necessary pre-launch commands, launch arguments, and
    node specifications.

    Args:
        spec (spec.specification.MerlinSpec): An instance of the
            [`MerlinSpec`][spec.specification.MerlinSpec] class that contains the
            configuration details, including the batch section.
        com: The command to launch with the batch configuration.
        nodes: The number of nodes to use in the batch launch. If not specified,
            it will default to the value in the batch configuration.
        batch: An optional batch override from the worker configuration. If not
            provided, the function will attempt to retrieve the batch section from
            the specification.

    Returns:
        The constructed worker launch command, ready to be executed.

    Raises:
        AttributeError: If the batch section is missing in the specification.
        TypeError: If the `nodes` parameter is of an invalid type.
    """
    if batch is None:
        try:
            batch = spec.batch
        except AttributeError:
            LOG.error("The batch section is required in the specification file.")
            raise

    parsed_batch = parse_batch_block(batch)

    # A jsrun submission cannot be run under a parent jsrun so
    # all non flux lsf submissions need to be local.
    if parsed_batch["btype"] == "local" or "lsf" in parsed_batch["btype"]:
        return com

    if nodes is None:
        # Use the value in the batch section
        nodes = parsed_batch["nodes"]

    # Get the number of nodes from the environment if unset
    if nodes is None or nodes == "all":
        nodes = get_node_count(parsed_batch, default=1)
    elif not isinstance(nodes, int):
        raise TypeError("Nodes was passed into batch_worker_launch with an invalid type (likely a string other than 'all').")

    if not parsed_batch["launch command"]:
        parsed_batch["launch command"] = construct_worker_launch_command(parsed_batch, nodes)

    if parsed_batch["launch args"]:
        parsed_batch["launch command"] += f" {parsed_batch['launch args']}"

    # Allow for any pre launch manipulation, e.g. module load
    # hwloc/1.11.10-cuda
    if parsed_batch["launch pre"]:
        parsed_batch["launch command"] = f"{parsed_batch['launch pre']} {parsed_batch['launch command']}"

    LOG.debug(f"launch command: {parsed_batch['launch command']}")

    worker_cmd: str = ""
    if parsed_batch["btype"] == "flux":
        launch = get_flux_launch(parsed_batch)
        worker_cmd = f'{launch} "{com}"'
    else:
        worker_cmd = f"{parsed_batch['launch command']} {com}"

    return worker_cmd


def construct_scheduler_legend(parsed_batch: Dict, nodes: int) -> Dict:
    """
    Constructs a legend of relevant information needed for each scheduler.

    This function generates a dictionary containing configuration details for various
    job schedulers based on the provided batch configuration. The returned dictionary
    includes flags for bank, queue, and walltime, as well as commands to check the
    scheduler and the initial launch command.

    Args:
        parsed_batch: A dictionary of batch configurations, which must include `bank`,
            `queue`, `walltime`, and `flux alloc`. See
            [`parse_batch_block`][study.batch.parse_batch_block] for more information on
            all the settings in this dictionary.
        nodes: The number of nodes to use in the launch command.

    Returns:
        A dictionary containing scheduler-related information, structured as
            follows:\n
            - For each scheduler (e.g., 'flux', 'lsf', 'pbs', 'slurm'):\n
                - `bank` (str): The flag to add a bank to the launch command.
                - `check cmd` (List[str]): The command to run to check if this is the main
                    scheduler for the cluster.
                - `expected check output` (bytes): The expected output from running
                    the check command.
                - `launch` (str): The initial launch command for the scheduler.
                - `queue` (str): The flag to add a queue to the launch command (if
                    applicable).
                - `walltime` (str): The flag to add a walltime to the launch command
                    (if applicable).
    """
    scheduler_legend = {
        "flux": {
            "bank": f" --setattr=system.bank={parsed_batch['bank']}",
            "check cmd": ["flux", "resource", "info"],
            "expected check output": b"Nodes",
            "launch": f"{parsed_batch['flux alloc']} -o pty -N {nodes} --exclusive --job-name=merlin",
            "queue": f" --setattr=system.queue={parsed_batch['queue']}",
            "walltime": f" -t {convert_timestring(parsed_batch['walltime'], format_method='FSD')}",
        },
        "lsf": {
            "check cmd": ["jsrun", "--help"],
            "expected check output": b"jsrun",
            "launch": f"jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n {nodes}",
        },
        # pbs is mainly a placeholder in case a user wants to try it (we don't have it at the lab so it's mostly untested)
        "pbs": {
            "bank": f" -A {parsed_batch['bank']}",
            "check cmd": ["qsub", "--version"],
            "expected check output": b"pbs_version",
            "launch": f"qsub -l nodes={nodes}",
            "queue": f" -q {parsed_batch['queue']}",
            "walltime": f" -l walltime={convert_timestring(parsed_batch['walltime'])}",
        },
        "slurm": {
            "bank": f" -A {parsed_batch['bank']}",
            "check cmd": ["sbatch", "--help"],
            "expected check output": b"sbatch",
            "launch": f"srun -N {nodes} -n {nodes}",
            "queue": f" -p {parsed_batch['queue']}",
            "walltime": f" -t {convert_timestring(parsed_batch['walltime'])}",
        },
    }
    return scheduler_legend


def construct_worker_launch_command(parsed_batch: Dict, nodes: int) -> str:
    """
    Constructs the worker launch command based on the provided batch configuration.

    This function generates a launch command for a worker process when no
    'worker_launch' command is specified in the batch configuration. It
    utilizes the scheduler legend to incorporate necessary flags such as
    bank, queue, and walltime, depending on the workload manager.

    Args:
        parsed_batch: A dictionary of batch configurations, which must include
            `btype`, `bank`, `queue`, and `walltime`. See
            [`parse_batch_block`][study.batch.parse_batch_block] for more information
            on all the settings in this dictionary.
        nodes: The number of nodes to use in the batch launch.

    Returns:
        The constructed launch command for the worker process.

    Raises:
        TypeError: If the PBS scheduler is enabled for a batch type other than 'flux'.
        KeyError: If the workload manager is not found in the scheduler legend.
    """
    # Initialize launch_command and get the scheduler_legend and workload_manager
    launch_command: str = ""
    scheduler_legend: Dict = construct_scheduler_legend(parsed_batch, nodes)
    workload_manager: str = get_batch_type(scheduler_legend)

    LOG.debug(f"parsed_batch: {parsed_batch}")

    if parsed_batch["btype"] == "pbs" and workload_manager == parsed_batch["btype"]:
        raise TypeError("The PBS scheduler is only enabled for 'batch: flux' type")

    if parsed_batch["btype"] == "slurm" and workload_manager not in ("lsf", "flux", "pbs"):
        workload_manager = "slurm"

    LOG.debug(f"workload_manager: {workload_manager}")

    try:
        launch_command = scheduler_legend[workload_manager]["launch"]
    except KeyError as e:  # pylint: disable=C0103
        LOG.debug(e)

    # If lsf is the workload manager we stop here (no need to add bank, queue, walltime)
    if workload_manager != "lsf" or not launch_command:
        # Add bank, queue, and walltime to the launch command as necessary
        for key in ("bank", "queue", "walltime"):
            if parsed_batch[key]:
                try:
                    launch_command += scheduler_legend[workload_manager][key]
                except KeyError as e:  # pylint: disable=C0103
                    LOG.error(e)

        # To read from stdin we append this to the launch command for pbs
        if workload_manager == "pbs":
            launch_command += " --"

    return launch_command
