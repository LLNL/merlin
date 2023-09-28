###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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
This module parses the batch section of the yaml specification.

Currently only the batch worker launch for slurm, lsf or flux
are implemented.

"""
import logging
import os
import subprocess
from typing import Dict, Optional, Union

from merlin.utils import convert_timestring, get_flux_alloc, get_yaml_var


LOG = logging.getLogger(__name__)


def batch_check_parallel(spec):
    """
    Check for a parallel batch section in the yaml file.
    """
    parallel = False

    try:
        batch = spec.batch
    except AttributeError:
        LOG.error("The batch section is required in the specification file.")
        raise

    btype = get_yaml_var(batch, "type", "local")
    if btype != "local":
        parallel = True

    return parallel


def check_for_scheduler(scheduler, scheduler_legend):
    """
    Check which scheduler (Flux, Slurm, LSF, or PBS) is the main
    scheduler for the cluster.
    :param `scheduler`: A string representing the scheduler to check for
                        Options: flux, slurm, lsf, or pbs
    :param `scheduler_legend`: A dict of information related to each scheduler
    :returns: A bool representing whether `scheduler` is the main scheduler for the cluster
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


def get_batch_type(scheduler_legend, default=None):
    """
    Determine which batch scheduler to use.

    :param scheduler_legend: A dict storing info related to each scheduler
    :param default: (str) The default batch scheduler to use if a scheduler
                          can't be determined. The default is None.
    :returns: (str) The batch name (available options: slurm, flux, lsf, pbs).
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


def get_node_count(default=1):
    """
    Determine a default node count based on the environment.

    :param default: (int) The number of nodes to return if a node count from
        the environment cannot be determined.
    :param returns: (int) The number of nodes to use.
    """
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
    A function to parse the batch block of the yaml file.
    :param `batch`: The batch block to read in
    :returns: A dict with all the info (or defaults) from the batch block
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
    Build the flux launch command based on the batch section of the yaml.
    :param `parsed_batch`: A dict of batch configurations
    :returns: The flux launch command
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
    spec: Dict,
    com: str,
    nodes: Optional[Union[str, int]] = None,
    batch: Optional[Dict] = None,
) -> str:
    """
    The configuration in the batch section of the merlin spec
    is used to create the worker launch line, which may be
    different from a simulation launch.

    : param spec : (Dict) workflow specification
    : param com : (str): The command to launch with batch configuration
    : param nodes : (Optional[Union[str, int]]): The number of nodes to use in the batch launch
    : param batch : (Optional[Dict]): An optional batch override from the worker config
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
        nodes = get_node_count(default=1)
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
    Constructs a legend of relevant information needed for each scheduler. This includes:
    - bank (str): The flag to add a bank to the launch command
    - check cmd (list): The command to run to check if this is the main scheduler for the cluster
    - expected check output (str): The expected output from running the check cmd
    - launch (str): The initial launch command for the scheduler
    - queue (str): The flag to add a queue to the launch command
    - walltime (str): The flag to add a walltime to the launch command

    :param `parsed_batch`: A dict of batch configurations
    :param `nodes`: An int representing the number of nodes to use in a launch command
    :returns: A dict of scheduler related information
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
            "launch": f"jsrun -a 1 -c ALL_CPUS -g ALL_SGPUS --bind=none -n {nodes}",
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
    If no 'worker_launch' is found in the batch yaml, this method constructs the needed launch command.

    :param `parsed_batch`: A dict of batch configurations
    :param `nodes`:: The number of nodes to use in the batch launch
    :returns: The launch command
    """
    # Initialize launch_command and get the scheduler_legend and workload_manager
    launch_command: str = ""
    scheduler_legend: Dict = construct_scheduler_legend(parsed_batch, nodes)
    workload_manager: str = get_batch_type(scheduler_legend)

    if parsed_batch["btype"] == "pbs" and workload_manager == parsed_batch["btype"]:
        raise TypeError("The PBS scheduler is only enabled for 'batch: flux' type")

    if parsed_batch["btype"] == "slurm" and workload_manager not in ("lsf", "flux", "pbs"):
        workload_manager = "slurm"

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
