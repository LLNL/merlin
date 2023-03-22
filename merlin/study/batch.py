###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.9.1.
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

from merlin.utils import get_flux_alloc, get_yaml_var


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


def check_for_flux():
    """
    Check if FLUX is the main scheduler for the cluster
    """
    try:
        p = subprocess.Popen(
            ["flux", "resource", "info"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        result = p.stdout.readlines()
        if result and len(result) > 0 and b"Nodes" in result[0]:
            return True
        else:
            return False
    except FileNotFoundError:
        return False


def check_for_slurm():
    """
    Check if SLURM is the main scheduler for the cluster
    """
    try:
        p = subprocess.Popen(
            ["sbatch", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        result = p.stdout.readlines()
        if result and len(result) > 0 and b"sbatch" in result[0]:
            return True
        else:
            return False
    except FileNotFoundError:
        return False


def check_for_lsf():
    """
    Check if LSF is the main scheduler for the cluster
    """
    try:
        p = subprocess.Popen(
            ["jsrun", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        result = p.stdout.readlines()
        if result and len(result) > 0 and b"jsrun" in result[0]:
            return True
        else:
            return False
    except FileNotFoundError:
        return False


def check_for_pbs():
    """
    Check if PBS is the main scheduler for the cluster
    """
    try:
        p = subprocess.Popen(
            ["qsub", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        result = p.stdout.readlines()
        if result and len(result) > 0 and b"pbs_version" in result[0]:
            return True
        else:
            return False
    except FileNotFoundError:
        return False


def get_batch_type(default=None):
    """
    Determine which batch scheduler to use.

    :param default: (str) The default batch scheduler to use if a scheduler
                          can't be determined. The default is None.
    :returns: (str) The batch name (available options: slurm, flux, lsf, pbs).
    """
    # Flux should be checked first due to slurm emulation scripts
    LOG.debug(f"check for flux = {check_for_flux()}")
    if check_for_flux():
        return "flux"

    # PBS should be checked before slurm for testing
    LOG.debug(f"check for pbs = {check_for_pbs()}")
    if check_for_pbs():
        return "pbs"

    # LSF should be checked before slurm for testing
    LOG.debug(f"check for lsf = {check_for_lsf()}")
    if check_for_lsf():
        return "lsf"

    LOG.debug(f"check for slurm = {check_for_slurm()}")
    if check_for_slurm():
        return "slurm"

    SYS_TYPE = os.environ.get("SYS_TYPE", "")
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
    elif "LSB_MCPU_HOSTS" in os.environ:
        nodes = os.environ["LSB_MCPU_HOSTS"].split()
        n_batch_nodes = len(nodes) // 2 - 1
        return n_batch_nodes

    return default


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

    btype: str = get_yaml_var(batch, "type", "local")

    # A jsrun submission cannot be run under a parent jsrun so
    # all non flux lsf submissions need to be local.
    if btype == "local" or "lsf" in btype:
        return com

    if nodes is None:
        # Use the value in the batch section
        nodes = get_yaml_var(batch, "nodes", None)

    # Get the number of nodes from the environment if unset
    if nodes is None or nodes == "all":
        nodes = get_node_count(default=1)
    elif not isinstance(nodes, int):
        raise TypeError("Nodes was passed into batch_worker_launch with an invalid type (likely a string other than 'all').")

    shell: str = get_yaml_var(batch, "shell", "bash")

    launch_pre: str = get_yaml_var(batch, "launch_pre", "")
    launch_args: str = get_yaml_var(batch, "launch_args", "")
    launch_command: str = get_yaml_var(batch, "worker_launch", "")

    if not launch_command:
        launch_command = construct_worker_launch_command(batch, btype, nodes)

    if launch_args:
        launch_command += f" {launch_args}"

    # Allow for any pre launch manipulation, e.g. module load
    # hwloc/1.11.10-cuda
    if launch_pre:
        launch_command = f"{launch_pre} {launch_command}"

    LOG.debug(f"launch_command = {launch_command}")

    worker_cmd: str = ""
    if btype == "flux":
        flux_path: str = get_yaml_var(batch, "flux_path", "")
        if "/" in flux_path:
            flux_path += "/"

        flux_exe: str = os.path.join(flux_path, "flux")

        flux_opts: Union[str, Dict] = get_yaml_var(batch, "flux_start_opts", "")

        flux_exec_workers: Union[str, Dict, bool] = get_yaml_var(batch, "flux_exec_workers", True)

        default_flux_exec = "flux exec" if launch_command else f"{flux_exe} exec"
        flux_exec: str = ""
        if flux_exec_workers:
            flux_exec = get_yaml_var(batch, "flux_exec", default_flux_exec)

        if launch_command and "flux" not in launch_command:
            launch: str = f"{launch_command} {flux_exe} start {flux_opts} {flux_exec} `which {shell}` -c"
        else:
            launch: str = f"{launch_command} {flux_exec} `which {shell}` -c"
        worker_cmd = f'{launch} "{com}"'
    else:
        worker_cmd = f"{launch_command} {com}"

    return worker_cmd


def construct_worker_launch_command(batch: Optional[Dict], btype: str, nodes: int) -> str:
    """
    If no 'worker_launch' is found in the batch yaml, this method constructs the needed launch command.

    : param batch : (Optional[Dict]): An optional batch override from the worker config
    : param btype : (str): The type of batch (flux, local, lsf)
    : param nodes : (int): The number of nodes to use in the batch launch
    """
    launch_command: str = ""
    workload_manager: str = get_batch_type()
    bank: str = get_yaml_var(batch, "bank", "")
    queue: str = get_yaml_var(batch, "queue", "")
    walltime: str = get_yaml_var(batch, "walltime", "")

    if btype == "pbs" and workload_manager == btype:
        raise Exception("The PBS scheduler is only enabled for 'batch: flux' type")

    if btype == "slurm" or workload_manager == "slurm":
        launch_command = f"srun -N {nodes} -n {nodes}"
        if bank:
            launch_command += f" -A {bank}"
        if queue:
            launch_command += f" -p {queue}"
        if walltime:
            launch_command += f" -t {walltime}"

    if workload_manager == "lsf":
        # The jsrun utility does not have a time argument
        launch_command = f"jsrun -a 1 -c ALL_CPUS -g ALL_GPUS --bind=none -n {nodes}"

    if workload_manager == "flux":
        flux_path: str = get_yaml_var(batch, "flux_path", "")
        if "/" in flux_path:
            flux_path += "/"

        flux_exe: str = os.path.join(flux_path, "flux")
        flux_alloc: str = get_flux_alloc(flux_exe)
        launch_command = f"{flux_exe} {flux_alloc} -o pty -N {nodes} --exclusive --job-name=merlin"

        if bank:
            launch_command += f" --setattr=system.bank={bank}"
        if queue:
            launch_command += f" --setattr=system.queue={queue}"
        if walltime:
            launch_command += f" -t {walltime}"

    if workload_manager == "pbs":
        launch_command = f"qsub -l nodes={nodes}"
        # launch_command = f"qsub -l nodes={nodes} -l procs={nodes}"
        if bank:
            launch_command += f" -A {bank}"
        if queue:
            launch_command += f" -q {queue}"
        # if walltime:
        #     launch_command += f" -l walltime={walltime}"
        launch_command += " --"  # To read from stdin

    return launch_command
