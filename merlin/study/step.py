###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
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
"""This module represents all of the logic that goes into a step"""

import logging
import os
import re
from contextlib import suppress
from copy import deepcopy
from typing import Dict, Optional, Tuple

from celery import current_task
from maestrowf.abstracts.enums import State
from maestrowf.datastructures.core.executiongraph import _StepRecord
from maestrowf.datastructures.core.study import StudyStep

from merlin.common.abstracts.enums import ReturnCode
from merlin.study.script_adapter import MerlinScriptAdapter
from merlin.study.status import read_status, write_status
from merlin.utils import needs_merlin_expansion


LOG = logging.getLogger(__name__)


def get_current_worker():
    """Get the worker on the current running task from celery"""
    worker = re.search(r"@.+\.", current_task.request.hostname).group()
    worker = worker[1 : len(worker) - 1]
    return worker


def get_current_queue():
    """Get the queue on the current running task from celery"""
    from merlin.config.configfile import CONFIG  # pylint: disable=C0415

    queue = current_task.request.delivery_info["routing_key"]
    queue = queue.replace(CONFIG.celery.queue_tag, "")
    return queue


class MerlinStepRecord(_StepRecord):
    """
    This class is a wrapper for the Maestro _StepRecord to remove
    a re-submit message and handle status updates.
    """

    def __init__(self, workspace: str, maestro_step: StudyStep, merlin_step: "Step", **kwargs):
        """
        :param `workspace`: The output workspace for this step
        :param `maestro_step`: The StudyStep object associated with this step
        :param `merlin_step`: The Step object associated with this step
        """
        _StepRecord.__init__(self, workspace, maestro_step, status=State.INITIALIZED, **kwargs)
        self.merlin_step = merlin_step

    @property
    def condensed_workspace(self) -> str:
        """
        Put together a smaller version of the workspace path to display.
        :returns: A condensed workspace name
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

    def _execute(self, adapter: "ScriptAdapter", script: str) -> Tuple["SubmissionRecord", int]:  # noqa: F821
        """
        Overwrites _StepRecord's _execute method from Maestro since self.to_be_scheduled is
        always true here. Also, if we didn't overwrite this we wouldn't be able to call
        self.mark_running() for status updates.

        :param `adapter`: The script adapter to submit jobs to
        :param `script`: The script to send to the script adapter
        :returns: A tuple of a return code and the jobid from the execution of `script`
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
        Mark the end time of the record with associated termination state
        and update the status file.

        :param `state`: A merlin ReturnCode object representing the end state of a task
        :param `max_retries`: A bool representing whether we hit the max number of retries or not
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
        """Increment the number of restarts we've had for this step and update the status file"""
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
        result: Optional[str] = None,
        task_server: Optional[str] = "celery",
    ):
        """
        Puts together a dictionary full of status info and creates a signature
        for the update_status celery task. This signature is ran here as well.

        :param `result`:  Optional parameter only applied when we've finished running
                          this step. String representation of a ReturnCode value.
        :param `task_server`: Optional parameter to define the task server we're using.
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
    executed by calling execute.
    """

    def __init__(self, maestro_step_record, study_name, parameter_info):
        """
        :param maestro_step_record: The StepRecord object.
        :param `study_name`: The name of the study
        :param `parameter_info`: A dict containing information about parameters in the study
        """
        self.mstep = maestro_step_record
        self.study_name = study_name
        self.parameter_info = parameter_info
        self.__restart = False
        self.params = {"cmd": {}, "restart_cmd": {}}
        self.establish_params()

    def get_cmd(self):
        """
        get the run command text body"
        """
        return self.mstep.step.__dict__["run"]["cmd"]

    def get_restart_cmd(self):
        """
        get the restart command text body, else return None"
        """
        return self.mstep.step.__dict__["run"]["restart"]

    def clone_changing_workspace_and_cmd(self, new_cmd=None, cmd_replacement_pairs=None, new_workspace=None):
        """
        Produces a deep copy of the current step, performing variable
        substitutions as we go

        :param new_cmd : (Optional) replace the existing cmd with the new_cmd.
        :param cmd_replacement_pairs : (Optional) replaces strings in the cmd
            according to the list of pairs in cmd_replacement_pairs
        :param new_workspace : (Optional) the workspace for the new step.
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

    def get_task_queue(self):
        """Retrieve the task queue for the Step."""
        return self.get_task_queue_from_dict(self.mstep.step.__dict__)

    @staticmethod
    def get_task_queue_from_dict(step_dict):
        """given a maestro step dict, get the task queue"""
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
    def retry_delay(self):
        """Returns the retry delay (default 1)"""
        default_retry_delay = 1
        return self.mstep.step.__dict__["run"].get("retry_delay", default_retry_delay)

    @property
    def max_retries(self):
        """
        Returns the max number of retries for this step.
        """
        return self.mstep.step.__dict__["run"]["max_retries"]

    @property
    def restart(self):
        """
        Get the restart property
        """
        return self.__restart

    @restart.setter
    def restart(self, val):
        """
        Set the restart property ensuring that restart is false
        """
        self.__restart = val

    def establish_params(self):
        """If this step uses parameters, pull them from the step param map."""
        try:
            step_params = self.parameter_info["step_param_map"][self.name()]
            for cmd_type in step_params:
                self.params[cmd_type].update(step_params[cmd_type])
        except KeyError:
            pass

    def check_if_expansion_needed(self, labels):
        """
        :return : True if the cmd has any of the default keywords or spec
            specified sample column labels.
        """
        return needs_merlin_expansion(self.get_cmd(), self.get_restart_cmd(), labels)

    def get_workspace(self):
        """
        :return : The workspace this step is to be executed in.
        """
        return self.mstep.workspace.value

    def name(self):
        """
        :return : The step name.
        """
        return self.mstep.step.__dict__["_name"]

    def name_no_params(self):
        """
        Get the original name of the step without any parameters/samples in the name.
        :returns: A string representing the name of the step
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

    def execute(self, adapter_config):
        """
        Execute the step.

        :param adapter_config : A dictionary containing configuration for
            the maestro script adapter, as well as which sort of adapter
            to use.
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
