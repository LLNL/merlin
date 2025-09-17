##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module represents all of the logic that goes into a step"""

import logging
import os
import re
from contextlib import suppress
from copy import deepcopy
from typing import Dict, List, Tuple

from celery import current_task
from maestrowf.abstracts.enums import State
from maestrowf.abstracts.interfaces.scriptadapter import ScriptAdapter
from maestrowf.datastructures.core.executiongraph import _StepRecord
from maestrowf.datastructures.core.study import StudyStep
from maestrowf.interfaces.script import SubmissionRecord

from merlin.common.enums import ReturnCode
from merlin.study.script_adapter import MerlinScriptAdapter
from merlin.study.status import read_status, write_status
from merlin.utils import needs_merlin_expansion


LOG = logging.getLogger(__name__)


def get_current_worker() -> str:
    """
    Get the worker on the current running task from Celery.

    This function retrieves the name of the worker that is currently
    executing the task. It extracts the worker's name from the task's
    request hostname.

    Returns:
        The name of the current worker.
    """
    worker = re.search(r"@.+\.", current_task.request.hostname).group()
    worker = worker[1 : len(worker) - 1]
    return worker


def get_current_queue() -> str:
    """
    Get the queue on the current running task from Celery.

    This function retrieves the name of the queue that the current
    task is associated with. It extracts the routing key from the
    task's delivery information and removes the queue tag defined
    in the configuration.

    Returns:
        The name of the current queue.
    """
    from merlin.config.configfile import CONFIG  # pylint: disable=C0415

    queue = current_task.request.delivery_info["routing_key"]
    queue = queue.replace(CONFIG.celery.queue_tag, "")
    return queue


class MerlinStepRecord(_StepRecord):
    """
    This class is a wrapper for the Maestro `_StepRecord` to remove
    a re-submit message and handle status updates.

    Attributes:
        condensed_workspace (str): A condensed version of the workspace path.
        elapsed_time (str): The total elapsed time for the step execution.
        jobid (List[int]): A list of job identifiers assigned by the scheduler.
        maestro_step (StudyStep): The StudyStep object associated with this step.
        merlin_step (Step): The Step object associated with this step.
        restart_limit (int): Upper limit on the number of restart attempts.
        restart_script (str): Script to resume record execution (if applicable).
        run_time (str): The run time for the step execution.
        status (State): The current status of the step.
        to_be_scheduled (bool): Indicates if the record needs scheduling.
        workspace (Variable): The output workspace for this step, represented as a Variable.

    Methods:
        mark_end: Marks the end of the step with the given state.
        mark_restart: Increments the restart count for the step.
        mark_running: Marks the step as running and updates the status file.
        setup_workspace: Initializes the workspace and status file for the step.
    """

    def __init__(self, workspace: str, maestro_step: StudyStep, merlin_step: "Step", **kwargs):
        """
        Initializes the `MerlinStepRecord` class which helps track the status of a step.

        Args:
            workspace: The output workspace for this step.
            maestro_step: The
                [StudyStep](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/datastructures/core/index.html#maestrowf.datastructures.core.StudyStep)
                object associated with this step.
            merlin_step: The [Step][study.step.Step] object associated with this step.
        """
        _StepRecord.__init__(self, workspace, maestro_step, status=State.INITIALIZED, **kwargs)
        self.merlin_step: Step = merlin_step

    @property
    def condensed_workspace(self) -> str:
        """
        Generate a condensed version of the workspace path for display purposes.

        This property constructs a shorter representation of the workspace path by extracting relevant
        components based on the study name and a timestamp pattern. If a match is found using a regular
        expression, the workspace path is split to isolate the condensed portion. If no match is found,
        a fallback method is used to manually create a condensed path based on the step name.

        Returns:
            A string representing the condensed workspace path, which is easier to read and display.
        """
        timestamp_regex = r"\d{8}-\d{6}/"
        match = re.search(rf"{self.merlin_step.study_name}_{timestamp_regex}", self.workspace.value)

        # If we got a match from the regex (which we should always get) then use it to condense the workspace
        if match:
            condensed_workspace = self.workspace.value.split(match.group())[1]
        # Otherwise manually condense (which could have issues if step names/parameters/study names are equivalent)
        else:
            step_name = self.merlin_step.name_no_params()
            end_of_path = self.workspace.value.rsplit(step_name, 1)[1]
            condensed_workspace = f"{step_name}{end_of_path}"

        LOG.debug(f"Condense workspace '{condensed_workspace}'")
        return condensed_workspace

    def _execute(self, adapter: ScriptAdapter, script: str) -> Tuple[SubmissionRecord, int]:
        """
        Executes the script using the provided adapter, overriding the default behavior to ensure
        that the step is marked as running and to facilitate job submission.

        This method overrides the `_execute` method from the base class `_StepRecord` in Maestro.
        It ensures that `self.to_be_scheduled` is always true, allowing for the invocation of
        `self.mark_running()` to update the status of the step.

        Args:
            adapter: The script adapter used to submit jobs.
            script: The script to be submitted to the script adapter.

        Returns:
            A tuple containing the return code and the job identifier from the execution of the script.
        """
        self.mark_running()

        LOG.info(f"Submitting script for {self.name}")
        srecord = adapter.submit(self.step, script, self.workspace.value)

        retcode = srecord.submission_code
        jobid = srecord.job_identifier
        return retcode, jobid

    def mark_running(self):
        """Mark the start time of the record and update the status file."""
        LOG.debug(f"Marking running for {self.name}")
        super().mark_running()
        self._update_status_file()

    def mark_end(self, state: ReturnCode, max_retries: bool = False):
        """
        Marks the end time of the record with the associated termination state
        and updates the status file.

        This method logs the action of marking the end of the step, maps the provided
        termination state to a corresponding Maestro state and result, and updates
        the status file accordingly. If the maximum number of retries has been reached
        for a soft failure, it appends a message to the result.

        Args:
            state: A ReturnCode object representing the end state of the task.
            max_retries: A flag indicating whether the maximum number of retries has been reached.
        """
        LOG.debug(f"Marking end for {self.name}")

        # Dictionary to keep track of associated variables for each return code
        state_mapper = {
            ReturnCode.OK: {
                "maestro state": State.FINISHED,
                "result": "MERLIN_SUCCESS",
            },
            ReturnCode.DRY_OK: {
                "maestro state": State.DRYRUN,
                "result": "MERLIN_DRY_SUCCESS",
            },
            ReturnCode.RETRY: {
                "maestro state": State.FINISHED,
                "result": "MERLIN_RETRY",
            },
            ReturnCode.RESTART: {
                "maestro state": State.FINISHED,
                "result": "MERLIN_RESTART",
            },
            ReturnCode.SOFT_FAIL: {
                "maestro state": State.FAILED,
                "result": "MERLIN_SOFT_FAIL",
            },
            ReturnCode.HARD_FAIL: {
                "maestro state": State.FAILED,
                "result": "MERLIN_HARD_FAIL",
            },
            ReturnCode.STOP_WORKERS: {
                "maestro state": State.CANCELLED,
                "result": "MERLIN_STOP_WORKERS",
            },
            "UNKNOWN": {
                "maestro state": State.UNKNOWN,
                "result": "MERLIN_UNRECOGNIZED",
            },
        }

        # Check if the state provided is valid
        if state not in state_mapper:
            state = "UNKNOWN"

        # Call to super().mark_end() will mark end time and update self.status for us
        super().mark_end(state_mapper[state]["maestro state"])
        step_result = state_mapper[state]["result"]

        # Append a "max retries reached" message to the step result if necessary
        if state == ReturnCode.SOFT_FAIL and max_retries:
            step_result += " (MAX RETRIES REACHED)"

        # Update the status file
        self._update_status_file(result=step_result)

    def mark_restart(self):
        """Increment the number of restarts we've had for this step and update the status file."""
        LOG.debug(f"Marking restart for {self.name}")
        if self.restart_limit == 0 or self._num_restarts < self.restart_limit:
            self._num_restarts += 1
            self._update_status_file()

    def setup_workspace(self):
        """Initialize the record's workspace and status file."""
        LOG.debug(f"Setting up workspace for {self.name}")
        super().setup_workspace()
        self._update_status_file()

    def _update_status_file(
        self,
        result: str = None,
        task_server: str = "celery",
    ):
        """
        Constructs a dictionary containing status information and creates a signature
        for the update_status Celery task. This signature is executed within the method.

        This method checks if a status file already exists; if it does, it updates the
        existing file with the current status information. If not, it initializes a new
        status dictionary. The method also includes optional parameters for the result
        of the task and the task server being used.

        Args:
            result: An optional string representation of a ReturnCode value, applied
                when the step has finished running.
            task_server: An optional parameter to specify the task server being used.
        """

        # This dict is used for converting an enum value to a string for readability
        state_translator: Dict[State, str] = {
            State.INITIALIZED: "INITIALIZED",
            State.RUNNING: "RUNNING",
            State.FINISHED: "FINISHED",
            State.CANCELLED: "CANCELLED",
            State.DRYRUN: "DRY_RUN",
            State.FAILED: "FAILED",
            State.UNKNOWN: "UNKNOWN",
        }

        LOG.debug(f"Marking status for {self.name} as {state_translator[self.status]}.")
        if result:
            LOG.debug(f"Result for {self.name} is {result}")

        status_filepath = f"{self.workspace.value}/MERLIN_STATUS.json"

        LOG.debug(f"Status filepath for {self.name}: '{status_filepath}")

        # If the status file already exists then we can just add to it
        if os.path.exists(status_filepath):
            status_info = read_status(status_filepath, f"{self.workspace.value}/status.lock")
        else:
            # Create the parameter entries
            cmd_params = restart_params = None
            if self.merlin_step.params["cmd"]:
                cmd_params = dict(self.merlin_step.params["cmd"].items())
            if self.merlin_step.params["restart_cmd"]:
                restart_params = dict(self.merlin_step.params["restart_cmd"].items())
            LOG.debug(f"Cmd parameters for {self.name}: {cmd_params}; Restart params: {restart_params}")

            # Inititalize the status_info dict we'll be dumping to the status file
            status_info = {
                self.name: {
                    "parameters": {
                        "cmd": cmd_params,
                        "restart": restart_params,
                    }
                }
            }

        # Put together a dict of status info
        status_info[self.name][self.condensed_workspace] = {
            "status": state_translator[self.status],
            "return_code": result,
            "elapsed_time": self.elapsed_time,
            "run_time": self.run_time,
            "restarts": self.restarts,
        }

        # Add celery specific info
        if task_server == "celery":
            from merlin.celery import app  # pylint: disable=C0415

            # If the tasks are always eager, this is a local run and we won't have workers running
            if not app.conf.task_always_eager:
                status_info[self.name]["task_queue"] = get_current_queue()

                # Add the current worker to the workspace-specific status info
                current_worker = get_current_worker()
                if "workers" not in status_info[self.name][self.condensed_workspace]:
                    status_info[self.name][self.condensed_workspace]["workers"] = [current_worker]
                elif current_worker not in status_info[self.name][self.condensed_workspace]["workers"]:
                    status_info[self.name][self.condensed_workspace]["workers"].append(current_worker)

                # Add the current worker to the overall-step status info
                if "workers" not in status_info[self.name]:
                    status_info[self.name]["workers"] = [current_worker]
                elif current_worker not in status_info[self.name]["workers"]:
                    status_info[self.name]["workers"].append(current_worker)

        LOG.info(f"Writing status for {self.name} to '{status_filepath}...")
        write_status(status_info, status_filepath, f"{self.workspace.value}/status.lock")
        LOG.info(f"Status for {self.name} successfully written.")


class Step:
    """
    This class provides an abstraction for an execution step, which can be
    executed by calling the [`execute`][study.step.Step.execute] method.

    Attributes:
        max_retries (int): Returns the maximum number of retries for this step.
        mstep (_StepRecord): The Maestro StepRecord object associated with this step.
        parameter_info (dict): A dictionary containing information about parameters in the study.
        params (Dict): A dictionary containing command parameters for the step, including 'cmd' and 'restart_cmd'.
        restart (bool): Property to get or set the restart status of the step.
        retry_delay (int): Returns the retry delay for the step (default is 1).
        study_name (str): The name of the study this step belongs to.

    Methods:
        check_if_expansion_needed: Checks if command expansion is needed based on specified labels.
        clone_changing_workspace_and_cmd: Produces a deep copy of the current step, with optional command
            and workspace modifications.
        establish_params: Pulls parameters from the step parameter map if applicable.
        execute: Executes the step using the provided adapter configuration.
        get_cmd: Retrieves the run command text body.
        get_restart_cmd: Retrieves the restart command text body, or None if not available.
        get_task_queue: Retrieves the task queue for the step.
        get_task_queue_from_dict: Static method to get the task queue from a step dictionary.
        get_workspace: Retrieves the workspace where this step is to be executed.
        name: Retrieves the name of the step.
        name_no_params: Gets the original name of the step without parameters or sample labels.
    """

    def __init__(self, maestro_step_record: _StepRecord, study_name: str, parameter_info: Dict):
        """
        Initializes the `Step` object which acts as a way to track everything about a step.

        Args:
            maestro_step_record: The `StepRecord` object.
            study_name: The name of the study
            parameter_info: A dict containing information about parameters in the study
        """
        self.mstep: _StepRecord = maestro_step_record
        self.study_name: str = study_name
        self.parameter_info: Dict = parameter_info
        self.__restart: bool = False
        self.params: Dict = {"cmd": {}, "restart_cmd": {}}
        self.establish_params()

    def get_cmd(self) -> str:
        """
        Retrieve the run command text body for the step.

        Returns:
            The run command text body for the step.
        """
        return self.mstep.step.__dict__["run"]["cmd"]

    def get_restart_cmd(self) -> str:
        """
        Retrieve the restart command text body for the step.

        Returns:
            The restart command text body for the step, or None if no restart command is available.
        """
        return self.mstep.step.__dict__["run"]["restart"]

    def clone_changing_workspace_and_cmd(
        self,
        new_cmd: str = None,
        cmd_replacement_pairs: List[Tuple[str]] = None,
        new_workspace: str = None,
    ) -> "Step":
        """
        Produces a deep copy of the current step, with optional modifications to
        the command and workspace, performing variable substitutions as we go.

        This method creates a new instance of the Step class by cloning the
        current step and allowing for modifications to the command text and
        workspace. It performs variable substitutions in the command based on
        the provided replacement pairs.

        Args:
            new_cmd: If provided, replaces the existing command with this new command.
            cmd_replacement_pairs: A list of pairs where each pair contains a string to
                be replaced and its replacement. The method will perform replacements in
                both the run command and the restart command.
            new_workspace: If provided, sets this as the workspace for the new step. If
                not specified, the current workspace will be used.

        Returns:
            A new Step instance with the modified command and workspace.
        """
        LOG.debug(f"clone called with new_workspace {new_workspace}")
        step_dict = deepcopy(self.mstep.step.__dict__)

        if new_cmd is not None:
            step_dict["run"]["cmd"] = new_cmd

        if cmd_replacement_pairs is not None:
            for str1, str2 in cmd_replacement_pairs:
                cmd = step_dict["run"]["cmd"]
                step_dict["run"]["cmd"] = re.sub(re.escape(str1), str2, cmd, flags=re.I)

                restart_cmd = step_dict["run"]["restart"]
                if restart_cmd:
                    step_dict["run"]["restart"] = re.sub(re.escape(str1), str2, restart_cmd, flags=re.I)

        if new_workspace is None:
            new_workspace = self.get_workspace()
        LOG.debug(f"cloned step with workspace {new_workspace}")
        study_step = StudyStep()
        study_step.name = step_dict["_name"]
        study_step.description = step_dict["description"]
        study_step.run = step_dict["run"]
        return Step(MerlinStepRecord(new_workspace, study_step, self), self.study_name, self.parameter_info)

    def get_task_queue(self) -> str:
        """
        Retrieve the task queue for the current Step.

        Returns:
            The name of the task queue for the Step, which may be influenced
                by the configuration settings.
        """
        return self.get_task_queue_from_dict(self.mstep.step.__dict__)

    @staticmethod
    def get_task_queue_from_dict(step_dict: Dict) -> str:
        """
        Get the task queue from a given Maestro step dictionary.

        This static method extracts the task queue information from the
        provided step dictionary. It considers the configuration settings
        to determine the appropriate queue name, including handling cases
        where the task queue may be omitted.

        Args:
            step_dict: A dictionary representation of a Maestro step, expected
                to contain a "run" key with a "task_queue" entry.

        Returns:
            The name of the task queue. If the task queue is not specified
                or is set to "none", it returns the default queue name based
                on the configuration.
        """
        from merlin.config.configfile import CONFIG  # pylint: disable=C0415

        queue_tag = CONFIG.celery.queue_tag
        omit_tag = CONFIG.celery.omit_queue_tag
        if omit_tag:
            queue = "merlin"
        else:
            queue = queue_tag

        with suppress(TypeError, KeyError):
            val = step_dict["run"]["task_queue"]
            if not (val is None or val.lower() == "none" or val == ""):
                if omit_tag:
                    queue = val
                else:
                    queue = queue_tag + val
        return queue

    @property
    def retry_delay(self) -> int:
        """
        Get the retry delay for the step.

        Returns:
            The retry delay in seconds. Defaults to 1 if not specified.
        """
        default_retry_delay = 1
        return self.mstep.step.__dict__["run"].get("retry_delay", default_retry_delay)

    @property
    def max_retries(self) -> int:
        """
        Get the maximum number of retries for this step.

        Returns:
            The maximum number of retries for the step.
        """
        return self.mstep.step.__dict__["run"]["max_retries"]

    @property
    def restart(self) -> bool:
        """
        Get the restart property.

        Returns:
            True if the step is set to restart, False otherwise.
        """
        return self.__restart

    @restart.setter
    def restart(self, val: bool):
        """
        Set the restart property.

        Args:
            val: The new value for the restart property. It should be
                a boolean value indicating whether the step should restart.
        """
        self.__restart = val

    def establish_params(self):
        """
        Establish parameters for the step from the parameter map.

        This method checks if the current step uses parameters by accessing
        the `step_param_map` from `parameter_info`. If parameters are found
        for the current step, it updates the `params` dictionary with the
        corresponding values.
        """
        try:
            step_params = self.parameter_info["step_param_map"][self.name()]
            for cmd_type in step_params:
                self.params[cmd_type].update(step_params[cmd_type])
        except KeyError:
            pass

    def check_if_expansion_needed(self, labels: List[str]) -> bool:
        """
        Check if expansion is needed based on commands and labels.

        This method determines whether the command associated with the
        current step requires expansion. It checks for the presence of
        default keywords or specified sample column labels.

        Args:
            labels: A list of labels to check against the commands.

        Returns:
            True if the command requires expansion, False otherwise.
        """
        return needs_merlin_expansion(self.get_cmd(), self.get_restart_cmd(), labels)

    def get_workspace(self) -> str:
        """
        Get the workspace for the current step.

        Returns:
            The workspace associated with this step.
        """
        return self.mstep.workspace.value

    def name(self) -> str:
        """
        Get the name of the current step.

        Returns:
            The name of the step.
        """
        return self.mstep.step.__dict__["_name"]

    def name_no_params(self) -> str:
        """
        Get the original name of the step without parameters or sample labels.

        This method retrieves the name of the step and removes any
        parameter labels or sample identifiers that may be included
        in the name. It ensures that the returned name is clean and
        free from extraneous characters, such as trailing periods or
        underscores.

        Returns:
            The cleaned name of the step, free from parameters and sample labels.
        """
        # Get the name with everything still in it
        name = self.name()

        # Remove the parameter labels from the name
        for label in self.parameter_info["labels"]:
            name = name.replace(f"{label}", "")

        # Remove possible leftover characters after condensing the name
        while name.endswith(".") or name.endswith("_"):
            if name.endswith("."):
                split_char = "."
            else:
                split_char = "_"
            split_name = name.rsplit(split_char, 1)
            name = "".join(split_name)

        return name

    def execute(self, adapter_config: Dict) -> ReturnCode:
        """
        Execute the step with the provided adapter configuration.

        This method performs the execution of the step by configuring
        the necessary parameters and invoking the appropriate adapter.
        It updates the adapter configuration based on the step's
        requirements, sets up the workspace, and generates the script
        for execution. If a dry run is specified, it prepares the
        workspace without executing any tasks.

        Args:
            adapter_config (dict): A dictionary containing configuration
                for the maestro script adapter, including:\n
                - `shell`: The shell to use for execution.
                - `batch_type`: The type of batch processing to use.
                - `dry_run`: A boolean indicating whether to perform a
                dry run (setup only, no execution).

        Returns:
            (common.enums.ReturnCode): A [`ReturnCode`][common.enums.ReturnCode] object representing
                the result of the execution.
        """
        # Update shell if the task overrides the default value from the batch section
        default_shell = adapter_config.get("shell")
        shell = self.mstep.step.run.get("shell", default_shell)
        adapter_config.update({"shell": shell})

        # Update batch type if the task overrides the default value from the batch section
        default_batch_type = adapter_config.get("batch_type", adapter_config["type"])
        # Set batch_type to default if unset
        adapter_config.setdefault("batch_type", default_batch_type)
        # Override the default batch: type: from the step config
        batch = self.mstep.step.run.get("batch", None)
        if batch:
            batch_type = batch.get("type", default_batch_type)
            adapter_config.update({"batch_type": batch_type})

        adapter = MerlinScriptAdapter(**adapter_config)
        LOG.debug(f"Maestro step config = {adapter_config}")

        # Preserve the default shell if the step shell is different
        adapter_config.update({"shell": default_shell})
        # Preserve the default batch type if the step batch type is different
        adapter_config.update({"batch_type": default_batch_type})

        self.mstep.setup_workspace()
        self.mstep.generate_script(adapter)

        # dry run: sets up a workspace without executing any tasks. Each step's
        # workspace directory is created, and each step's command script is
        # written to it. The command script is not run, so there is no
        # 'MERLIN_FINISHED' file, nor '<step>.out' nor '<step>.err' log files.
        if adapter_config["dry_run"] is True:
            return ReturnCode.DRY_OK

        # TODO: once maestrowf is updated so that execute returns a
        # submissionrecord, then we need to return the record.return_code here
        # at that point, we can drop the use of MerlinScriptAdapter above, and
        # go back to using the adapter specified by the adapter_config['type']
        # above
        # If the above is done, then merlin_step in tasks.py can be changed to
        # calls to the step execute and restart functions.
        if self.restart and self.get_restart_cmd():
            return ReturnCode(self.mstep.restart(adapter))

        return ReturnCode(self.mstep.execute(adapter))
