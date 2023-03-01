###############################################################################
# Copyright (c) 2022, Lawrence Livermore National Security, LLC.
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

import logging
import re
from contextlib import suppress
from copy import deepcopy
from datetime import datetime

from celery import current_task

from maestrowf.abstracts.enums import State
from maestrowf.datastructures.core.executiongraph import _StepRecord
from maestrowf.datastructures.core.study import StudyStep
from maestrowf.utils import create_parentdir

from merlin.common.abstracts.enums import ReturnCode
from merlin.common.tasks import update_status
from merlin.config.configfile import CONFIG
from merlin.study.script_adapter import MerlinScriptAdapter


LOG = logging.getLogger(__name__)


class MerlinStepRecord(_StepRecord):
    """
    This classs is a wrapper for the Maestro _StepRecord to remove
    a re-submit message.
    """

    def __init__(self, workspace, step, study_name, **kwargs):
        _StepRecord.__init__(self, workspace, step, status=State.INITIALIZED, **kwargs)
        self.status_file = f"{self.workspace.value}/MERLIN_STATUS"
        self.study_name = study_name

        # Save the task queue and worker name from celery as an attribute here
        # Wanted to put this logic in merlin.common.tasks in update_status but celery
        # doesn't have a way to get the parent routing key (queue) and worker name
        # TODO: this isn't always correct, need to look into this
        self.celery_task_queue = current_task.request.delivery_info["routing_key"]
        self.celery_worker_name = current_task.request.hostname

    def _execute(self, adapter, script):
        """
        Overwrites StepRecord's _execute method from Maestro since self.to_be_scheduled is
        always true here. Also, if we didn't overwrite this we wouldn't be able to call
        self.mark_running() for status updates.
        """
        self.mark_running()
        srecord = adapter.submit(self.step, script, self.workspace.value)

        retcode = srecord.submission_code
        jobid = srecord.job_identifier
        return retcode, jobid

    def mark_running(self):
        """Mark the start time of the record and update the status file."""
        # TODO: do we need to modify this somehow for restarts? Do we need to reset start time?
        super().mark_running()

        self._update_status_file()

    def mark_end(self, state, max_retries=False):
        """
        Mark the end time of the record with associated termination state
        and update the status file.
        
        :param `state`: 
        """
        # Call to super().mark_end() will mark end time and update self.status for us
        step_result: str
        if state == ReturnCode.OK:
            super().mark_end(State.FINISHED)
            step_result = f"MERLIN_SUCCESS"
        elif state == ReturnCode.DRY_OK:
            super().mark_end(State.DRY_RUN)
            step_result = f"DRY_SUCCESS"
        elif state == ReturnCode.RETRY:
            super().mark_end(State.FINISHED)
            step_result = f"MERLIN_RETRY"
        elif state == ReturnCode.RESTART:
            super().mark_end(State.FINISHED)
            step_result = f"MERLIN_RESTART"
        elif state == ReturnCode.SOFT_FAIL:
            super().mark_end(State.FAILED)
            step_result = f"MERLIN_SOFT_FAIL"
            if max_retries:
                step_result += " (MAX RETRIES REACHED)"
        elif state == ReturnCode.HARD_FAIL:
            super().mark_end(State.FAILED)
            step_result = f"MERLIN_HARD_FAIL"
        elif state == ReturnCode.STOP_WORKERS:
            super().mark_end(State.CANCELLED)
            step_result = f"MERLIN_STOP_WORKERS"
        else:
            super().mark_end(State.UNKNOWN)
            step_result = f"UNRECOGNIZED_RETURN_CODE"

        self._update_status_file(result=step_result)

    def mark_restart(self):
        # TODO: figure restart out since you haven't thought about it at all yet
        super().mark_restart()
        self._update_status_file()
        # If we want a status entry for each retry then update start time here
        # Maybe we create an attribute to track if this step is on a restart? Then
        # we can see if we should append a line to status file or overwrite

    def setup_workspace(self):
        """Initialize the record's workspace and status file."""
        create_parentdir(self.workspace.value)
        self._update_status_file()

    def _update_status_file(self, result=None):
        """
        Puts together a dictionary full of status info and creates a signature
        for the update_status celery task. This signature is ran here as well.

        :param str result:  Optional parameter only applied when we've finished running
                            this step. String representation of a ReturnCode value.

        :side effect: a celery task is created and started
        """
        state_translator: Dict[State, str] = {
            State.INITIALIZED: "INITIALIZED",
            State.RUNNING: "RUNNING",
            State.FINISHED: "FINISHED",
            State.CANCELLED: "CANCELLED",
            State.DRYRUN: "DRY_RUN",
            State.FAILED: "FAILED",
            State.UNKNOWN: "UNKNOWN"
        }

        status_info = {
            "name": self.name,
            "status": state_translator[self.status],
            "result": result,
            "elapsed_time": self.elapsed_time,
            "run_time": self.run_time,
            "num_restarts": self.restarts,
            "workspace": self.workspace.value,
            "queue": self.celery_task_queue,
            "worker": self.celery_worker_name,
        }
        status_updater = update_status.s(status_info=status_info, status_file=self.status_file)
        status_updater.set(queue=f"{CONFIG.celery.queue_tag}{self.study_name}_status_queue")
        status_updater.apply_async()


class Step:
    """
    This class provides an abstraction for an execution step, which can be
    executed by calling execute.
    """

    def __init__(self, maestro_step_record, study_name):
        """
        :param maestro_step_record: The StepRecord object.
        """
        self.mstep = maestro_step_record
        self.restart = False
        self.study_name = study_name

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
        return Step(MerlinStepRecord(new_workspace, study_step, self.study_name), self.study_name)

    def get_task_queue(self):
        """Retrieve the task queue for the Step."""
        return self.get_task_queue_from_dict(self.mstep.step.__dict__)

    @staticmethod
    def get_task_queue_from_dict(step_dict):
        """given a maestro step dict, get the task queue"""
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
        default_retry_delay = 1
        return self.mstep.step.__dict__["run"].get("retry_delay", default_retry_delay)

    @property
    def max_retries(self):
        """
        Returns the max number of retries for this step.
        """
        return self.mstep.step.__dict__["run"]["max_retries"]

    def __get_restart(self):
        """
        Set the restart property ensuring that restart is false
        """
        return self.__restart

    def __set_restart(self, val):
        """
        Set the restart property ensuring that restart is false
        """
        self.__restart = val

    restart = property(__get_restart, __set_restart)

    def needs_merlin_expansion(self, labels):
        """
        :return : True if the cmd has any of the default keywords or spec
            specified sample column labels.
        """
        needs_expansion = False

        cmd = self.get_cmd()
        for label in labels + [
            "MERLIN_SAMPLE_ID",
            "MERLIN_SAMPLE_PATH",
            "merlin_sample_id",
            "merlin_sample_path",
        ]:
            if f"$({label})" in cmd:
                needs_expansion = True

        # The restart may need expansion while the cmd does not.
        restart_cmd = self.get_restart_cmd()
        if not needs_expansion and restart_cmd:
            for label in labels + [
                "MERLIN_SAMPLE_ID",
                "MERLIN_SAMPLE_PATH",
                "merlin_sample_id",
                "merlin_sample_path",
            ]:
                if f"$({label})" in restart_cmd:
                    needs_expansion = True

        return needs_expansion

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
        step_name = self.name()
        step_dir = self.get_workspace()

        # dry run: sets up a workspace without executing any tasks. Each step's
        # workspace directory is created, and each step's command script is
        # written to it. The command script is not run, so there is no
        # 'MERLIN_FINISHED' file, nor '<step>.out' nor '<step>.err' log files.
        if adapter_config["dry_run"] is True:
            return ReturnCode.DRY_OK

        LOG.info(f"Executing step '{step_name}' in '{step_dir}'...")
        # TODO: once maestrowf is updated so that execute returns a
        # submissionrecord, then we need to return the record.return_code here
        # at that point, we can drop the use of MerlinScriptAdapter above, and
        # go back to using the adapter specified by the adapter_config['type']
        # above
        # If the above is done, then merlin_step in tasks.py can be changed to
        # calls to the step execute and restart functions.
        if self.restart and self.get_restart_cmd():
            return ReturnCode(self.mstep.restart(adapter))
        else:
            return ReturnCode(self.mstep.execute(adapter))
