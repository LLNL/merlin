##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module stores the functionality for adapting bash scripts to use schedulers.

Supported schedulers are currently: Flux, LSF, and Slurm.
"""

import logging
import os
from typing import Dict, List, Set, Tuple, Union

from maestrowf.abstracts.enums import StepPriority
from maestrowf.abstracts.interfaces.scriptadapter import ScriptAdapter
from maestrowf.datastructures.core.study import StudyStep
from maestrowf.interfaces.script import SubmissionRecord
from maestrowf.interfaces.script.localscriptadapter import LocalScriptAdapter
from maestrowf.interfaces.script.slurmscriptadapter import SlurmScriptAdapter
from maestrowf.utils import start_process

from merlin.common.enums import ReturnCode
from merlin.utils import convert_timestring, find_vlaunch_var


LOG = logging.getLogger(__name__)


def setup_vlaunch(step_run: str, batch_type: str, gpu_config: bool):
    """
    Check for the VLAUNCHER keyword in the step run string and configure VLAUNCHER settings.

    This function examines the provided step run command string for the presence of the
    VLAUNCHER keyword. If found, it replaces the keyword with the LAUNCHER keyword and
    extracts relevant MERLIN variables such as nodes, processes, and cores per task.
    It also configures GPU settings based on the provided boolean flag.

    Args:
        step_run: The step.run command string that may contain the VLAUNCHER keyword.
        batch_type: A string representing the type of batch processing being used.
        gpu_config: A boolean indicating whether GPUs should be configured.
    """
    if "$(VLAUNCHER)" in step_run["cmd"]:
        step_run["cmd"] = step_run["cmd"].replace("$(VLAUNCHER)", "$(LAUNCHER)")

        step_run["nodes"] = find_vlaunch_var("NODES", step_run["cmd"])
        step_run["procs"] = find_vlaunch_var("PROCS", step_run["cmd"])
        step_run["cores per task"] = find_vlaunch_var("CORES", step_run["cmd"])

        if find_vlaunch_var("GPUS", step_run["cmd"]):
            if gpu_config:
                step_run["gpus"] = find_vlaunch_var("GPUS", step_run["cmd"])
            else:
                LOG.warning(f"Merlin does not yet have the ability to set GPUs per task with {batch_type}. Coming soon.")


class MerlinLSFScriptAdapter(SlurmScriptAdapter):
    """
    A `SchedulerScriptAdapter` class for SLURM blocking parallel launches.
    The `MerlinLSFScriptAdapter` uses non-blocking submits for executing LSF parallel jobs
    in a Celery worker.

    Attributes:
        key (str): A unique key identifier for the adapter.
        _cmd_flags (Dict[str, str]): A dictionary containing command flags for LSF execution.
        _unsupported (Set[str]): A set of parameters that are unsupported by this adapter.

    Methods:
        get_header: Generates the header for LSF execution scripts.
        get_parallelize_command: Generates the LSF parallelization segment of the command line.
        get_priority: Overrides the abstract method to fix a pylint error.
        write_script: Overwrites the write_script method from the base ScriptAdapter class.
    """

    key: str = "merlin-lsf"

    def __init__(self, **kwargs: Dict):
        """
        Initialize an instance of the `MerinLSFScriptAdapter`.

        The `MerlinLSFScriptAdapter` is the adapter that is used for workflows that
        will execute LSF parallel jobs in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        Args:
            **kwargs: A dictionary with default settings for the adapter.
        """
        super().__init__(**kwargs)

        self._cmd_flags: Dict[str, str] = {
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

        self._unsupported: Set[str] = {
            "cmd",
            "depends",
            "flux",
            "gpus",
            "max_retries",
            "nodes",
            "ntasks",
            "post",
            "pre",
            "reservation",
            "restart",
            "retry_delay",
            "shell",
            "slurm",
            "task_queue",
            "walltime",
        }

    def get_priority(self, priority: StepPriority):
        """
        This is implemented to override the abstract method and fix a pylint error.

        Args:
            priority: Float or
                [`StepPriority`](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/abstracts/enums/index.html#maestrowf.abstracts.enums.StepPriority)
                enum representing priorty.
        """

    def get_header(self, step: StudyStep) -> str:
        """
        Generate the header present at the top of LSF execution scripts.

        Args:
            step: A Maestro StudyStep instance that contains parameters relevant to the execution.

        Returns:
            A string of the header based on internal batch parameters and the parameter step.
        """
        return f"#!{self._exec}"

    def get_parallelize_command(self, procs: int, nodes: int = None, **kwargs: Dict) -> str:
        """
        Generate the LSF parallelization segment of the command line.

        This method constructs a command line segment for parallel execution in LSF.
        It allows specifying the number of processors and nodes to be allocated for the parallel call,
        along with additional command flags through keyword arguments.

        Args:
            procs: Number of processors to allocate to the parallel call.
            nodes: Number of nodes to allocate to the parallel call. Defaults to 1.
            **kwargs: Additional command flags that may be supported by the LSF command.

        Returns:
            A string representing the parallelization command configured using nodes and procs.
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
                args += [self._cmd_flags[key], f"{str(value)}"]

        return " ".join(args)

    def write_script(self, ws_path: str, step: StudyStep) -> Tuple[bool, str, str]:
        """
        This will overwrite the `write_script` method from Maestro's base ScriptAdapter
        class but will eventually call it. This is necessary for the VLAUNCHER to work.

        Args:
            ws_path: The path to the workspace where the scripts will be written.
            step: The Maestro StudyStep object containing information for the step.

        Returns:
            A tuple containing:\n
                - bool: A boolean indicating whether this step is to be scheduled or not.
                        (Merlin can ignore this value.)
                - str: The path to the script for the command.
                - str: The path to the script for the restart command.
        """
        setup_vlaunch(step.run, "lsf", False)

        return super().write_script(ws_path, step)


class MerlinSlurmScriptAdapter(SlurmScriptAdapter):
    """
    A `SchedulerScriptAdapter` class for SLURM blocking parallel launches.

    This class extends the `SlurmScriptAdapter` to provide support for blocking parallel
    launches in SLURM. Unlike the base class, which uses non-blocking submits, this adapter
    is designed for workflows that execute SLURM parallel jobs in a Celery worker.

    Attributes:
        key (str): A unique identifier for the adapter, set to "merlin-slurm".
        _cmd_flags (Dict[str, str]): A dictionary containing command flags for SLURM.
        _unsupported (Set[str]): A set of command flags that are not supported by this adapter.

    Methods:
        get_header: Generates the header for SLURM execution scripts.
        get_parallelize_command: Generates the SLURM parallelization segment of the command line.
        get_priority: Overrides the abstract method to fix a pylint error.
        time_format: Converts a timestring to HH:MM:SS format.
        write_script: Overwrites the write_script method from the base class to ensure VLAUNCHER compatibility.
    """

    key: str = "merlin-slurm"

    def __init__(self, **kwargs: Dict):
        """
        Initialize an instance of the `MerinSlurmScriptAdapter`.

        The `MerlinSlurmScriptAdapter` is the adapter that is used for workflows that
        will execute SLURM parallel jobs in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        Args:
            **kwargs: A dictionary with default settings for the adapter.
        """
        super().__init__(**kwargs)

        self._cmd_flags: Dict[str, str]

        self._cmd_flags["slurm"] = ""
        self._cmd_flags["walltime"] = "-t"

        new_unsupported: List[str] = [
            "bind",
            "flux",
            "gpus per task",
            "gpus",
            "lsf",
            "max_retries",
            "post",
            "pre",
            "restart",
            "retry_delay",
            "shell",
            "task_queue",
        ]
        self._unsupported: Set[str] = set(list(self._unsupported) + new_unsupported)

    def get_priority(self, priority: StepPriority):
        """
        This is implemented to override the abstract method and fix a pylint error.

        Args:
            priority: Float or
                [`StepPriority`](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/abstracts/enums/index.html#maestrowf.abstracts.enums.StepPriority)
                enum representing priorty.
        """

    def get_header(self, step: StudyStep) -> str:
        """
        Generate the header present at the top of Slurm execution scripts.

        Args:
            step: A Maestro StudyStep instance that contains parameters relevant to the execution.

        Returns:
            A string of the header based on internal batch parameters and the parameter step.
        """
        return f"#!{self._exec}"

    def time_format(self, val: Union[str, int]) -> str:
        """
        Convert the input timestring or integer to HH:MM:SS format.

        This method utilizes the [`convert_timestring`][utils.convert_timestring]
        function to convert a given timestring or integer (representing seconds)
        into a formatted string in the 'hours:minutes:seconds' (HH:MM:SS) format.

        Args:
            val: A timestring in the format '[days]:[hours]:[minutes]:seconds' or
                an integer representing time in seconds.

        Returns:
            A string representation of the input time formatted as 'HH:MM:SS'.
        """
        return convert_timestring(val, format_method="HMS")

    def get_parallelize_command(self, procs: int, nodes: int = None, **kwargs: Dict) -> str:
        """
        Generate the SLURM parallelization segment of the command line.

        This method constructs the command line segment required for parallel execution
        in SLURM, including the number of processors and nodes to allocate. It also
        incorporates any additional supported command flags provided in `kwargs`.

        Args:
            procs: The number of processors to allocate for the parallel call.
            nodes: The number of nodes to allocate for the parallel call (default is 1).
            **kwargs: Additional command flags to customize the SLURM command.
                Supported flags include 'walltime' and others defined in the
                `_cmd_flags` attribute, excluding those in the `_unsupported` set.

        Returns:
            A string representing the SLURM parallelization command, formatted with the
                specified number of processors, nodes, and any additional flags.
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
                    f"{str(self.time_format(value))}",
                ]
            elif "=" in self._cmd_flags[key]:
                args += [f"{self._cmd_flags[key]}{str(value)}"]
            else:
                args += [self._cmd_flags[key], f"{str(value)}"]

        return " ".join(args)

    def write_script(self, ws_path: str, step: StudyStep) -> Tuple[bool, str, str]:
        """
        This will overwrite the `write_script` method from Maestro's base ScriptAdapter
        class but will eventually call it. This is necessary for the VLAUNCHER to work.

        Args:
            ws_path: The path to the workspace where the scripts will be written.
            step: The Maestro `StudyStep` object containing information for the step.

        Returns:
            A tuple containing:\n
                - bool: A boolean indicating whether this step is to be scheduled or not.
                        (Merlin can ignore this value.)
                - str: The path to the script for the command.
                - str: The path to the script for the restart command.
        """
        setup_vlaunch(step.run, "slurm", False)

        return super().write_script(ws_path, step)


class MerlinFluxScriptAdapter(MerlinSlurmScriptAdapter):
    """
    A `SchedulerScriptAdapter` class for flux blocking parallel launches.

    The `MerlinFluxScriptAdapter` is designed for workflows that execute flux parallel jobs
    in a Celery worker. It utilizes non-blocking submits and allows for configuration of the
    shell in which scripts are executed.

    Attributes:
        key (str): A unique identifier for the adapter, set to "merlin-flux".
        _cmd_flags (Dict[str, str]): A dictionary containing command-line flags for the flux command.
        _unsupported (Set[str]): A set of command flags that are not supported by this adapter.

    Methods:
        get_priority: Retrieves the priority of the step.
        time_format: Converts a time format to flux standard designation.
        write_script: Writes the script for the specified step and returns relevant paths.
    """

    key: str = "merlin-flux"

    def __init__(self, **kwargs: Dict):
        """
        Initialize an instance of the `MerinFluxScriptAdapter`.

        The `MerlinFluxScriptAdapter` is the adapter that is used for workflows that
        will execute flux parallel jobs in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        Args:
            **kwargs: A dictionary with default settings for the adapter.
        """
        # The flux_command should always be overriden by the study object's flux_command property
        flux_command = kwargs.pop("flux_command", "flux run")
        super().__init__(**kwargs)

        self._cmd_flags: Dict[str, str] = {
            "cmd": flux_command,
            "ntasks": "-n",
            "nodes": "-N",
            "cores per task": "-c",
            "gpus per task": "-g",
            "walltime": "-t",
            "flux": "",
        }  # noqa

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
            "retry_delay",
            "pre",
            "post",
            "depends",
            "bind",
            "lsf",
            "slurm",
        ]
        self._unsupported: Set[str] = set(new_unsupported)  # noqa

    def get_priority(self, priority: StepPriority):
        """
        This is implemented to override the abstract method and fix a pylint error.

        Args:
            priority: Float or
                [`StepPriority`](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/abstracts/enums/index.html#maestrowf.abstracts.enums.StepPriority)
                enum representing priorty.
        """

    def time_format(self, val: Union[str, int]) -> str:
        """
        Convert a time format to Flux Standard Duration (FSD).

        This method takes a time value and converts it into a format that is compatible
        with Flux's standard time representation. The conversion is performed using the
        [`convert_timestring`][utils.convert_timestring] function with the specified format
        method.

        Args:
            val: The time value to be converted. This can be a string representing a time
                duration or an integer representing a time value.

        Returns:
            The time formatted according to Flux Standard Duration (FSD).
        """
        return convert_timestring(val, format_method="FSD")

    def write_script(self, ws_path: str, step: StudyStep) -> Tuple[bool, str, str]:
        """
        This will overwrite the `write_script` method from Maestro's base ScriptAdapter
        class but will eventually call it. This is necessary for the VLAUNCHER to work.

        Args:
            ws_path: The path to the workspace where the scripts will be written.
            step: The Maestro `StudyStep` object containing information for the step.

        Returns:
            A tuple containing:\n
                - bool: A boolean indicating whether this step is to be scheduled or not.
                        (Merlin can ignore this value.)
                - str: The path to the script for the command.
                - str: The path to the script for the restart command.
        """
        setup_vlaunch(step.run, "flux", True)

        return super().write_script(ws_path, step)


class MerlinScriptAdapter(LocalScriptAdapter):
    """
    A `ScriptAdapter` class for interfacing with execution in Merlin.

    This class serves as an adapter for executing scripts in a Celery worker
    environment. It allows for configuration of the execution environment and
    manages the execution of scripts with appropriate logging and error handling.

    Attributes:
        batch_adapter (ScriptAdapter): An instance of a batch adapter used for executing scripts
            based on the specified batch type.
        batch_type (str): The type of batch processing to be used, derived from
            the provided keyword arguments.
        key (str): A unique identifier for the adapter, set to "merlin-local".

    Methods:
        submit: Executes a workflow step locally.
        write_script: Writes a script using the batch adapter.
    """

    key: str = "merlin-local"

    def __init__(self, **kwargs: Dict):
        """
        Initialize an instance of the `MerinScriptAdapter`.

        The `MerlinScriptAdapter` is the adapter that is used for workflows that
        will execute in a celery worker. The only configurable aspect to
        this adapter is the shell that scripts are executed in.

        Args:
            **kwargs: A dictionary with default settings for the adapter.
        """
        super().__init__(**kwargs)

        self.batch_type: str = "merlin-" + kwargs.get("batch_type", "local")

        if "host" not in kwargs:
            kwargs["host"] = "None"
        if "bank" not in kwargs:
            kwargs["bank"] = "None"
        if "queue" not in kwargs:
            kwargs["queue"] = "None"

        # Using super prevents recursion.
        self.batch_adapter: ScriptAdapter = super()
        if self.batch_type != "merlin-local":
            self.batch_adapter = MerlinScriptAdapterFactory.get_adapter(self.batch_type)(**kwargs)

    def write_script(self, *args, **kwargs) -> Tuple[bool, str, str]:
        """
        Generate a script for execution using the batch adapter.

        This method delegates the script writing process to the associated
        batch adapter and returns the generated script along with a restart
        script if applicable.

        Returns:
            A tuple containing:\n
                - bool: A boolean indicating whether this step is to be scheduled or not.
                        (Merlin can ignore this value.)
                - str: The path to the script for the command.
                - str: The path to the script for the restart command.
        """
        _, script, restart_script = self.batch_adapter.write_script(*args, **kwargs)
        return True, script, restart_script

    # Pylint complains that there's too many arguments but it's fine in this case
    def submit(
        self, step: StudyStep, path: str, cwd: str, job_map: Dict = None, env: Dict = None
    ) -> SubmissionRecord:  # pylint: disable=R0913
        """
        Execute a workflow step locally.

        This method runs a specified script in the local environment, allowing for
        customization of the working directory and environment variables. It handles
        the execution of the script and logs the results, including any errors or
        specific return codes.

        Args:
            step: An instance of the StudyStep that contains information about the
                workflow step being executed.
            path: The file path to the script that is to be executed.
            cwd: The current working directory from which the script will be executed.
            job_map: A mapping of workflow step names to their job identifiers.
            env: A dictionary containing environment variables to be set for the execution.

        Returns:
            An object containing the return code of the command, the process ID of the
                command, and any additional information about the execution.
        """
        LOG.debug("cwd = %s", cwd)
        LOG.debug("Script to execute: %s", path)
        LOG.debug(f"starting process {path} in cwd {cwd} called {step.name}")
        submission_record = self._execute_subprocess(step.name, path, cwd, env=env, join_output=False)
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
        elif retcode == ReturnCode.RAISE_ERROR:
            LOG.debug("Execution returned status RAISE_ERROR")
        else:
            LOG.warning(f"Unrecognized Merlin Return code: {retcode}, returning SOFT_FAIL")
            submission_record.add_info("retcode", retcode)
            retcode = ReturnCode.SOFT_FAIL

        # Currently, we use Maestro's execute method, which is returning the
        # submission code we want it to return the return code, so we are
        # setting it in here.
        # TODO: In the refactor/status branch we're overwriting Maestro's execute method (I think) so
        # we should be able to change this (i.e. add code in the overridden execute and remove this line)
        submission_record._subcode = retcode  # pylint: disable=W0212

        return submission_record

    # TODO is there currently ever a scenario where join output is True? We should look into this
    # Pylint is complaining there's too many local variables and args but it makes this function cleaner so ignore
    def _execute_subprocess(
        self, output_name: str, script_path: str, cwd: str, env: Dict = None, join_output: bool = False
    ) -> SubmissionRecord:  # pylint: disable=R0913,R0914
        """
        Execute a subprocess script locally and manage output.

        This method runs a specified script in a subprocess, capturing its output
        and error streams. It allows for customization of the working directory,
        environment variables, and output handling. The output can be saved to
        files, and error messages can be appended to the standard output if desired.

        Args:
            output_name: The base name for the output files (stdout and stderr).
                If None, no output files will be created.
            script_path: The file path to the script that is to be executed.
            cwd: The current working directory from which the script will be executed.
            env: A dictionary containing environment variables to be set for the execution.
            join_output: If True, appends stderr to stdout in the output file.

        Returns:
            An object containing the return code of the command the process ID of the
                command, and any additional information about the execution.
        """
        script_bn = os.path.basename(script_path)
        new_output_name = os.path.splitext(script_bn)[0]
        LOG.debug(f"script_path={script_path}, output_name={output_name}, new_output_name={new_output_name}")
        process = start_process(script_path, shell=False, cwd=cwd, env=env)
        output, err = process.communicate()
        retcode = process.wait()

        # This allows us to save on iNodes by not writing the output,
        # or by appending error to output
        if output_name is not None:
            o_path = os.path.join(cwd, f"{new_output_name}.out")
            with open(o_path, "a") as out:
                out.write(output)

                if join_output:
                    out.write("\n####### stderr follows #######\n")
                    out.write(err)

            if not join_output:
                e_path = os.path.join(cwd, f"{new_output_name}.err")
                with open(e_path, "a") as out:
                    out.write(err)

        if retcode == 0:
            LOG.info("Execution returned status OK.")
            return SubmissionRecord(ReturnCode.OK, retcode, process.pid)

        _record = SubmissionRecord(ReturnCode.ERROR, retcode, process.pid)
        _record.add_info("stderr", str(err))
        return _record


class MerlinScriptAdapterFactory:
    """
    This class routes to the correct `ScriptAdapter`.

    The `MerlinScriptAdapterFactory` is responsible for providing the appropriate
    `ScriptAdapter` based on the specified adapter ID. It maintains a mapping of
    available adapters and offers methods to retrieve them.

    Attributes:
        factories: A dictionary mapping adapter IDs (str) to their corresponding
            `ScriptAdapter` classes.

    Methods:
        get_adapter: Returns the appropriate `ScriptAdapter` class for the given adapter ID.
        get_valid_adapters: Returns a list of valid adapter IDs that can be used with this factory.
    """

    factories: Dict[str, ScriptAdapter] = {
        "merlin-flux": MerlinFluxScriptAdapter,
        "merlin-lsf": MerlinLSFScriptAdapter,
        "merlin-lsf-srun": MerlinSlurmScriptAdapter,
        "merlin-slurm": MerlinSlurmScriptAdapter,
        "merlin-local": MerlinScriptAdapter,
    }

    @classmethod
    def get_adapter(cls, adapter_id: str) -> ScriptAdapter:
        """
        Returns the appropriate `ScriptAdapter` to use.

        This method retrieves the `ScriptAdapter` class associated with the given
        adapter ID. If the adapter ID is not found in the factory's mapping,
        a ValueError is raised.

        Args:
            adapter_id: The ID of the desired `ScriptAdapter`.

        Returns:
            The corresponding `ScriptAdapter` class.

        Raises:
            ValueError: If the specified adapter_id is not found in the factories.
        """
        if adapter_id.lower() not in cls.factories:
            msg = f"""Adapter '{str(adapter_id)}' not found. Specify an adapter that exists
                or implement a new one mapping to the '{str(adapter_id)}'"""
            LOG.error(msg)
            raise ValueError(msg)

        return cls.factories[adapter_id]

    @classmethod
    def get_valid_adapters(cls) -> List[str]:
        """
        Returns the valid ScriptAdapters.

        This method provides a list of all valid adapter IDs that can be used
        with this factory. The IDs are derived from the keys of the factories
        dictionary.

        Returns:
            A list of valid adapter IDs.
        """
        return cls.factories.keys()
