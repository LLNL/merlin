###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
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
Merlin script adapter module
"""

import logging
import os

from maestrowf.interfaces.script import SubmissionRecord
from maestrowf.interfaces.script.localscriptadapter import LocalScriptAdapter
from maestrowf.interfaces.script.slurmscriptadapter import SlurmScriptAdapter
from maestrowf.utils import start_process

from merlin.common.abstracts.enums import ReturnCode


LOG = logging.getLogger(__name__)


class MerlinLSFScriptAdapter(SlurmScriptAdapter):
    """
    A SchedulerScriptAdapter class for slurm blocking parallel launches,
    the LSFScriptAdapter uses non-blocking submits.
    """

    key = "merlin-lsf"

    def __init__(self, **kwargs):
        """
        Initialize an instance of the MerinLSFScriptAdapter.
        The MerlinLSFScriptAdapter is the adapter that is used for workflows that
        will execute LSF parallel jobs in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        :param **kwargs: A dictionary with default settings for the adapter.
        """
        super(MerlinLSFScriptAdapter, self).__init__(**kwargs)

        self._cmd_flags = {
            "cmd": "jsrun",
            "ntasks": "--np",
            "nodes": "--nrs",
            "cores per task": "-c",
            "gpus per task": "-g",
            "num resource set": "--nrs",
            "bind": "-b",
            "launch_distribution": "-d",
            "exit_on_error": "-X",
            "lsf": "",
        }

        self._unsupported = {
            "cmd",
            "ntasks",
            "nodes",
            "gpus",
            "walltime",
            "reservation",
            "restart",
            "task_queue",
            "max_retries",
            "pre",
            "post",
            "depends",
            "slurm",
            "flux",
        }

    def get_header(self, step):
        """
        Generate the header present at the top of LSF execution scripts.

        :param step: A StudyStep instance.
        :returns: A string of the header based on internal batch parameters and
            the parameter step.
        """
        return "#!{}".format(self._exec)

    def get_parallelize_command(self, procs, nodes=None, **kwargs):
        """
        Generate the LSF parallelization segement of the command line.
        :param procs: Number of processors to allocate to the parallel call.
        :param nodes: Number of nodes to allocate to the parallel call
            (default = 1).
        :returns: A string of the parallelize command configured using nodes
            and procs.
        """
        if not nodes:
            nodes = 1

        args = [
            # SLURM srun command
            self._cmd_flags["cmd"],
            # Processors segment
            self._cmd_flags["ntasks"],
            str(procs),
            # Resource segment
            self._cmd_flags["nodes"],
            str(nodes),
        ]

        args += [self._cmd_flags["bind"], kwargs.pop("bind", "rs")]

        plane_cpus = int(int(procs) / int(nodes))
        args += [
            self._cmd_flags["launch_distribution"],
            kwargs.pop("launch_distribution", f"plane:{plane_cpus}"),
        ]

        args += [self._cmd_flags["exit_on_error"], kwargs.pop("exit_on_error", "1")]

        supported = set(kwargs.keys()) - self._unsupported
        for key in supported:
            value = kwargs.get(key)
            if key not in self._cmd_flags:
                LOG.warning("'%s' is not supported -- ommitted.", key)
                continue
            if value:
                args += [self._cmd_flags[key], "{}".format(str(value))]

        return " ".join(args)


class MerlinSlurmScriptAdapter(SlurmScriptAdapter):
    """
    A SchedulerScriptAdapter class for slurm blocking parallel launches,
    the SlurmScriptAdapter uses non-blocking submits.
    """

    key = "merlin-slurm"

    def __init__(self, **kwargs):
        """
        Initialize an instance of the MerinSlurmScriptAdapter.
        The MerlinSlurmScriptAdapter is the adapter that is used for workflows that
        will execute SLURM parallel jobs in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        :param **kwargs: A dictionary with default settings for the adapter.
        """
        super(MerlinSlurmScriptAdapter, self).__init__(**kwargs)

        self._cmd_flags["slurm"] = ""
        self._cmd_flags["walltime"] = "-t"

        new_unsupported = [
            "task_queue",
            "max_retries",
            "pre",
            "post",
            "gpus per task",
            "gpus",
            "restart",
            "bind",
            "lsf",
            "flux",
        ]
        self._unsupported = set(list(self._unsupported) + new_unsupported)

    def get_header(self, step):
        """
        Generate the header present at the top of Slurm execution scripts.

        :param step: A StudyStep instance.
        :returns: A string of the header based on internal batch parameters and
            the parameter step.
        """
        return "#!{}".format(self._exec)

    def time_format(self, val):
        """
        This function assumes the time format is in hh:mm::ss
        """
        return val

    def get_parallelize_command(self, procs, nodes=None, **kwargs):
        """
        Generate the SLURM parallelization segement of the command line.
        :param procs: Number of processors to allocate to the parallel call.
        :param nodes: Number of nodes to allocate to the parallel call
            (default = 1).
        :returns: A string of the parallelize command configured using nodes
            and procs.
        """
        args = [
            # SLURM srun command
            self._cmd_flags["cmd"],
            # Processors segment
            self._cmd_flags["ntasks"],
            str(procs),
        ]

        if nodes:
            args += [self._cmd_flags["nodes"], str(nodes)]

        supported = set(kwargs.keys()) - self._unsupported
        for key in supported:
            value = kwargs.get(key)
            if not value:
                continue

            if key not in self._cmd_flags:
                LOG.warning("'%s' is not supported -- ommitted.", key)
                continue

            if key == "walltime":
                args += [
                    self._cmd_flags[key],
                    "{}".format(str(self.time_format(value))),
                ]
            elif "=" in self._cmd_flags[key]:
                args += ["{0}{1}".format(self._cmd_flags[key], str(value))]
            else:
                args += [self._cmd_flags[key], "{}".format(str(value))]

        return " ".join(args)


class MerlinLSFSrunScriptAdapter(MerlinSlurmScriptAdapter):
    """
    A SchedulerScriptAdapter class for lsf blocking parallel launches, using the srun wrapper
    """

    key = "merlin-lsf-srun"

    def __init__(self, **kwargs):
        """
        Initialize an instance of the MerinLSFSrunScriptAdapter.
        The MerlinLSFSrunScriptAdapter is the adapter that is used for workflows that
        will execute LSF parallel jobs in a celery worker with an srun wrapper. The only
        configurable aspect to this adapter is the shell that scripts are executed in.

        :param **kwargs: A dictionary with default settings for the adapter.
        """
        super(MerlinLSFSrunScriptAdapter, self).__init__(**kwargs)


class MerlinFluxScriptAdapter(MerlinSlurmScriptAdapter):
    """
    A SchedulerScriptAdapter class for flux blocking parallel launches,
    the FluxScriptAdapter uses non-blocking submits.
    """

    key = "merlin-flux"

    def __init__(self, **kwargs):
        """
        Initialize an instance of the MerinFluxScriptAdapter.
        The MerlinFluxScriptAdapter is the adapter that is used for workflows that
        will execute flux parallel jobs in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        :param **kwargs: A dictionary with default settings for the adapter.
        """
        flux_command = kwargs.pop("flux_command", "flux mini run")
        super(MerlinFluxScriptAdapter, self).__init__(**kwargs)

        #  "cmd": "flux mini run",
        self._cmd_flags = {
            "cmd": flux_command,
            "ntasks": "-n",
            "nodes": "-N",
            "cores per task": "-c",
            "gpus per task": "-g",
            "walltime": "-t",
            "flux": "",
        }

        if "wreck" in flux_command:
            self._cmd_flags["walltime"] = "-T"

        new_unsupported = [
            "cmd",
            "ntasks",
            "nodes",
            "gpus",
            "reservation",
            "restart",
            "task_queue",
            "max_retries",
            "pre",
            "post",
            "depends",
            "bind",
            "lsf",
            "slurm",
        ]
        self._unsupported = set(new_unsupported)

    def time_format(self, val):
        """
        This function assumes the time format is in dd:hh:mm::ss

        flux requires a d,h,m,s time designation, so this
        function will convert the time to minutes

        """
        _, d, h, m, s = (":0" * 10 + val).rsplit(":", 4)
        return str(int(d) * 24 * 60 + int(h) * 60 + int(m) + int(s) / 60.0) + "m"


class MerlinScriptAdapter(LocalScriptAdapter):
    """
    A ScriptAdapter class for interfacing for execution in Merlin
    """

    key = "merlin-local"

    def __init__(self, **kwargs):
        """
        Initialize an instance of the MerinScriptAdapter.
        The MerlinScriptAdapter is the adapter that is used for workflows that
        will execute in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        :param **kwargs: A dictionary with default settings for the adapter.
        """
        super(MerlinScriptAdapter, self).__init__(**kwargs)

        self.batch_type = "merlin-" + kwargs.get("batch_type", "local")

        if "host" not in kwargs.keys():
            kwargs["host"] = "None"
        if "bank" not in kwargs.keys():
            kwargs["bank"] = "None"
        if "queue" not in kwargs.keys():
            kwargs["queue"] = "None"

        # Using super prevents recursion.
        self.batch_adapter = super(MerlinScriptAdapter, self)
        if self.batch_type != "merlin-local":
            self.batch_adapter = MerlinScriptAdapterFactory.get_adapter(
                self.batch_type
            )(**kwargs)

    def write_script(self, *args, **kwargs):
        """
        TODO
        """
        _, script, restart_script = self.batch_adapter.write_script(*args, **kwargs)
        return True, script, restart_script

    def submit(self, step, path, cwd, job_map=None, env=None):
        """
        Execute the step locally.
        If cwd is specified, the submit method will operate outside of the path
        specified by the 'cwd' parameter.
        If env is specified, the submit method will set the environment
        variables for submission to the specified values. The 'env' parameter
        should be a dictionary of environment variables.

        :param step: An instance of a StudyStep.
        :param path: Path to the script to be executed.
        :param cwd: Path to the current working directory.
        :param job_map: A map of workflow step names to their job identifiers.
        :param env: A dict containing a modified environment for execution.
        :returns: The return code of the command and processID of the command.
        """
        LOG.debug("cwd = %s", cwd)
        LOG.debug("Script to execute: %s", path)
        LOG.debug("starting process %s in cwd %s called %s" % (path, cwd, step.name))
        submission_record = self._execute_subprocess(step.name, path, cwd, env, False)
        retcode = submission_record.return_code
        if retcode == ReturnCode.OK:
            LOG.debug("Execution returned status OK.")
        elif retcode == ReturnCode.RESTART:
            LOG.debug("Execution returned status RESTART.")
            step.restart = True
        elif retcode == ReturnCode.SOFT_FAIL:
            LOG.warning("Execution returned status SOFT_FAIL. ")
        elif retcode == ReturnCode.HARD_FAIL:
            LOG.warning("Execution returned status HARD_FAIL. ")
        elif retcode == ReturnCode.RETRY:
            LOG.debug("Execution returned status RETRY.")
            step.restart = False
        elif retcode == ReturnCode.STOP_WORKERS:
            LOG.debug("Execution returned status STOP_WORKERS")
        else:
            LOG.warning(
                f"Unrecognized Merlin Return code: {retcode}, returning SOFT_FAIL"
            )
            submission_record._info["retcode"] = retcode
            retcode = ReturnCode.SOFT_FAIL

        # Currently, we use Maestro's execute method, which is returning the
        # submission code we want it to return the return code, so we are
        # setting it in here.
        submission_record._subcode = retcode

        return submission_record

    def _execute_subprocess(
        self, output_name, script_path, cwd, env=None, join_output=False
    ):
        """
        Execute the subprocess script locally.
        If cwd is specified, the submit method will operate outside of the path
        specified by the 'cwd' parameter.
        If env is specified, the submit method will set the environment
        variables for submission to the specified values. The 'env' parameter
        should be a dictionary of environment variables.

        :param output_name: Output name for stdout and stderr (output_name.out). If None, don't write.
        :param script_path: Path to the script to be executed.
        :param cwd: Path to the current working directory.
        :param env: A dict containing a modified environment for execution.
        :param join_output: If True, append stderr to stdout
        :returns: The return code of the submission command and job identifier (SubmissionRecord).
        """
        script_bn = os.path.basename(script_path)
        new_output_name = os.path.splitext(script_bn)[0]
        LOG.debug(
            f"script_path={script_path}, output_name={output_name}, new_output_name={new_output_name}"
        )
        p = start_process(script_path, shell=False, cwd=cwd, env=env)
        pid = p.pid
        output, err = p.communicate()
        retcode = p.wait()

        # This allows us to save on iNodes by not writing the output,
        # or by appending error to output
        if output_name is not None:
            o_path = os.path.join(cwd, "{}.out".format(new_output_name))

            with open(o_path, "a") as out:
                out.write(output)

                if join_output:
                    out.write("\n####### stderr follows #######\n")
                    out.write(err)

            if not join_output:
                e_path = os.path.join(cwd, "{}.err".format(new_output_name))
                with open(e_path, "a") as out:
                    out.write(err)

        if retcode == 0:
            LOG.info("Execution returned status OK.")
            return SubmissionRecord(ReturnCode.OK, retcode, pid)
        else:
            _record = SubmissionRecord(ReturnCode.ERROR, retcode, pid)
            _record.add_info("stderr", str(err))
            return _record


class MerlinScriptAdapterFactory(object):
    factories = {
        "merlin-flux": MerlinFluxScriptAdapter,
        "merlin-lsf": MerlinLSFScriptAdapter,
        "merlin-lsf-srun": MerlinLSFSrunScriptAdapter,
        "merlin-slurm": MerlinSlurmScriptAdapter,
        "merlin-local": MerlinScriptAdapter,
    }

    @classmethod
    def get_adapter(cls, adapter_id):
        if adapter_id.lower() not in cls.factories:
            msg = (
                "Adapter '{0}' not found. Specify an adapter that exists "
                "or implement a new one mapping to the '{0}'".format(str(adapter_id))
            )
            LOG.error(msg)
            raise Exception(msg)

        return cls.factories[adapter_id]

    @classmethod
    def get_valid_adapters(cls):
        return cls.factories.keys()
