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

"""Test tasks."""
from __future__ import absolute_import, unicode_literals

import json
import logging
import os
from typing import Any, Dict, List, Optional

# Need to disable an overwrite warning here since celery has an exception that we need that directly
# overwrites a python built-in exception
from celery import chain, chord, group, shared_task, signature
from celery.exceptions import MaxRetriesExceededError, OperationalError, TimeoutError  # pylint: disable=W0622
from filelock import FileLock, Timeout
from redis.exceptions import TimeoutError as RedisTimeoutError

from merlin.common.abstracts.enums import ReturnCode
from merlin.common.sample_index import uniform_directories
from merlin.common.sample_index_factory import create_hierarchy
from merlin.config.utils import Priority, get_priority
from merlin.exceptions import HardFailException, InvalidChainException, RestartException, RetryException
from merlin.router import stop_workers
from merlin.spec.expansion import parameter_substitutions_for_cmd, parameter_substitutions_for_sample
from merlin.study.status import read_status, status_conflict_handler
from merlin.utils import dict_deep_merge


retry_exceptions = (
    IOError,
    OSError,
    AttributeError,
    TimeoutError,
    OperationalError,
    RetryException,
    RestartException,
    FileNotFoundError,
    RedisTimeoutError,
)

LOG = logging.getLogger(__name__)

STOP_COUNTDOWN = 60

# TODO: most of the pylint errors that are disabled in this file are the ones listed below.
# We should refactor this file so that we use more functions to solve all of these errors
# R0912: too many branches
# R0913: too many arguments
# R0914: too many local variables
# R0915: too many statements


@shared_task(  # noqa: C901
    bind=True,
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    priority=get_priority(Priority.HIGH),
)
def merlin_step(self, *args: Any, **kwargs: Any) -> Optional[ReturnCode]:  # noqa: C901 pylint: disable=R0912,R0915
    """
    Executes a Merlin Step
    :param args: The arguments, one of which should be an instance of Step
    :param kwargs: The optional keyword arguments that describe adapter_config and
                   the next step in the chain, if there is one.

    Example kwargs dict:
    {"adapter_config": {'type':'local'},
     "next_in_chain": <Step object>} # merlin_step will be added to the current chord
                                     # with next_in_chain as an argument
    """
    from merlin.study.step import Step  # pylint: disable=C0415

    step: Optional[Step] = None
    LOG.debug(f"args is {len(args)} long")

    arg: Any
    for arg in args:
        if isinstance(arg, Step):
            step = arg
        else:
            LOG.debug(f"discard argument {arg}, not of type Step.")

    config: Dict[str, str] = kwargs.pop("adapter_config", {"type": "local"})
    next_in_chain: Optional[Step] = kwargs.pop("next_in_chain", None)

    if step:
        self.max_retries = step.max_retries
        step_name: str = step.name()
        step_dir: str = step.get_workspace()
        LOG.debug(f"merlin_step: step_name '{step_name}' step_dir '{step_dir}'")
        finished_filename: str = os.path.join(step_dir, "MERLIN_FINISHED")

        # if we've already finished this task, skip it
        result: ReturnCode
        if os.path.exists(finished_filename):
            LOG.info(f"Skipping step '{step_name}' in '{step_dir}'.")
            result = ReturnCode.OK
        else:
            LOG.info(f"Executing step '{step_name}' in '{step_dir}'...")
            result = step.execute(config)
            step.mstep.mark_end(result)

        if result == ReturnCode.OK:
            LOG.info(f"Step '{step_name}' in '{step_dir}' finished successfully.")
            # touch a file indicating we're done with this step
            with open(finished_filename, "a"):
                pass
        elif result == ReturnCode.DRY_OK:
            LOG.info(f"Dry-ran step '{step_name}' in '{step_dir}'.")
        elif result == ReturnCode.RESTART:
            step.restart = True
            try:
                LOG.info(
                    f"Step '{step_name}' in '{step_dir}' is being restarted ({self.request.retries + 1}/{self.max_retries})..."
                )
                step.mstep.mark_restart()
                self.retry(countdown=step.retry_delay, priority=get_priority(Priority.RETRY))
            except MaxRetriesExceededError:
                LOG.warning(
                    f"""*** Step '{step_name}' in '{step_dir}' exited with a MERLIN_RESTART command,
                    but has already reached its retry limit ({self.max_retries}). Continuing with workflow."""
                )
                result = ReturnCode.SOFT_FAIL
                # Need to call mark_end again since we switched from RESTART to SOFT_FAIL
                step.mstep.mark_end(result, max_retries=True)
        elif result == ReturnCode.RETRY:
            step.restart = False
            try:
                LOG.info(
                    f"Step '{step_name}' in '{step_dir}' is being retried ({self.request.retries + 1}/{self.max_retries})..."
                )
                step.mstep.mark_restart()
                self.retry(countdown=step.retry_delay, priority=get_priority(Priority.RETRY))
            except MaxRetriesExceededError:
                LOG.warning(
                    f"""*** Step '{step_name}' in '{step_dir}' exited with a MERLIN_RETRY command,
                    but has already reached its retry limit ({self.max_retries}). Continuing with workflow."""
                )
                result = ReturnCode.SOFT_FAIL
                # Need to call mark_end again since we switched from RETRY to SOFT_FAIL
                step.mstep.mark_end(result, max_retries=True)
        elif result == ReturnCode.SOFT_FAIL:
            LOG.warning(f"*** Step '{step_name}' in '{step_dir}' soft failed. Continuing with workflow.")
        elif result == ReturnCode.HARD_FAIL:
            # stop all workers attached to this queue
            step_queue = step.get_task_queue()
            LOG.error(f"*** Step '{step_name}' in '{step_dir}' hard failed. Quitting workflow.")
            LOG.error(f"*** Shutting down all workers connected to this queue ({step_queue}) in {STOP_COUNTDOWN} secs!")
            shutdown = shutdown_workers.s([step_queue])
            shutdown.set(queue=step_queue)
            shutdown.apply_async(countdown=STOP_COUNTDOWN)
            raise HardFailException
        elif result == ReturnCode.STOP_WORKERS:
            LOG.warning(f"*** Shutting down all workers in {STOP_COUNTDOWN} secs!")
            shutdown = shutdown_workers.s(None)
            shutdown.set(queue=step.get_task_queue())
            shutdown.apply_async(countdown=STOP_COUNTDOWN)
        elif result == ReturnCode.RAISE_ERROR:
            LOG.warning("*** Raising an error ***")
            raise Exception("Exception raised by request from the user")
        else:
            LOG.warning(f"**** Step '{step_name}' in '{step_dir}' had unhandled exit code {result}. Continuing with workflow.")

        # queue off the next task in a chain while adding it to the current chord so that the chordfinisher actually
        # waits for the next task in the chain
        if next_in_chain is not None:
            if self.request.is_eager:
                LOG.debug(f"calling next_in_chain {signature(next_in_chain)}")
                next_in_chain.delay()
            else:
                LOG.debug(f"adding {next_in_chain} to chord")
                self.add_to_chord(next_in_chain, lazy=False)
        return result

    LOG.error("Failed to find step!")
    return None


def is_chain_expandable(chain_, labels):
    """
    Returns whether to expand the steps in the given chain.
    A chain_ is expandable if all the steps are expandable.
    It is not expandable if none of the steps are expandable.
    If neither expandable nor not expandable, we raise an InvalidChainException.
    :param chain_: A list of Step objects representing chain of dependent steps.
    :param labels: The labels

    """

    array_of_bools = [step.check_if_expansion_needed(labels) for step in chain_]

    needs_expansion = all(array_of_bools)

    if needs_expansion is False:
        # if we're not expanding, but at least one step needed expansion, then
        # this is an incompatible chain
        incompatible_chain = any(array_of_bools)

        if incompatible_chain is True:
            LOG.error(
                "INCOMPATIBLE CHAIN - all tasks in a chain need to either "
                "be merlin expanded or all need to not be merlin expanded. "
                "Please report this to merlin@llnl.gov"
            )
            raise InvalidChainException

    return needs_expansion


def prepare_chain_workspace(sample_index, chain_):
    """
    Prepares a user's workspace for each step in the given chain.
    :param chain_: A list of Step objects representing chain of dependent steps.
    :param labels: The labels
    """
    # TODO: figure out faster way to create these directories (probably using
    # yet another task)
    for step in chain_:
        workspace = step.get_workspace()
        LOG.debug(f"Preparing workspace in {workspace}...")

        # If we need to expand it, initialize the workspace for the samples
        sample_index.name = workspace
        sample_index.write_directories()
        sample_index.write_multiple_sample_index_files()
        LOG.debug(f"...workspace {workspace} prepared.")


@shared_task(
    bind=True,
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    priority=get_priority(Priority.LOW),
)
def add_merlin_expanded_chain_to_chord(  # pylint: disable=R0913,R0914
    self,
    task_type,
    chain_,
    samples,
    labels,
    sample_index,
    adapter_config,
    min_sample_id,
):
    """
    Expands tasks in a chain, then adds the expanded tasks to the current chord.
    :param self: The current task.
    :param task_type: The celery task signature type the new tasks should be.
    :param chain_: The list of tasks to expand.
    :param samples:  The sample values to use for each new task.
    :param labels: The sample labels.
    :param sample_index: The sample index that contains the directory structure for tasks.
    :param adapter_config: The adapter config.
    :param min_sample_id: offset to use for the sample_index.
    """
    num_samples = len(samples)
    # Use the index to get a path to each sample
    LOG.debug(f"recursing with {num_samples} samples {samples}")
    if sample_index.is_grandparent_of_leaf or sample_index.is_parent_of_leaf:
        all_chains = []
        LOG.debug(f"gathering up {num_samples} relative paths")
        relative_paths = [
            os.path.dirname(sample_index.get_path_to_sample(sample_id + min_sample_id)) for sample_id in range(num_samples)
        ]
        top_lvl_workspace = chain_[0].get_workspace()
        LOG.debug(f"recursing grandparent with relative paths {relative_paths}")
        for step in chain_:
            # Make a list of new task objects with modified cmd and workspace
            # based off of the parameter substitutions and relative_path for
            # a given sample.
            workspace = step.get_workspace()
            LOG.debug(f"expanding step {step.name()} in workspace {workspace}")
            new_chain = []
            for sample_id, sample in enumerate(samples):
                new_step = task_type.s(
                    step.clone_changing_workspace_and_cmd(
                        new_workspace=os.path.join(workspace, relative_paths[sample_id]),
                        cmd_replacement_pairs=parameter_substitutions_for_sample(
                            sample,
                            labels,
                            sample_id + min_sample_id,
                            relative_paths[sample_id],
                        ),
                    ),
                    adapter_config=adapter_config,
                    top_lvl_workspace=top_lvl_workspace,
                )
                new_step.set(queue=step.get_task_queue())
                new_step.set(task_id=os.path.join(workspace, relative_paths[sample_id]))
                new_chain.append(new_step)

            all_chains.append(new_chain)

        # Only need to condense status files if there's more than 1 sample
        if num_samples > 1:
            condense_sig = condense_status_files.s(
                sample_index=sample_index,
                workspace=top_lvl_workspace,
                condensed_workspace=chain_[0].mstep.condensed_workspace,
            ).set(
                queue=chain_[0].get_task_queue(),
            )
        else:
            condense_sig = None

        LOG.debug("adding chain to chord")
        chain_1d = get_1d_chain(all_chains)
        launch_chain(self, chain_1d, condense_sig=condense_sig)
        LOG.debug("chain added to chord")
    else:
        # recurse down the sample_index hierarchy
        try:
            LOG.debug("recursing down sample_index hierarchy")
            for next_index in sample_index.children.values():
                next_index_name_before = next_index.name
                next_index.name = os.path.join(sample_index.name, next_index.name)
                LOG.debug("generating next step")
                next_step = add_merlin_expanded_chain_to_chord.s(
                    task_type,
                    chain_,
                    samples[next_index.min - min_sample_id : next_index.max - min_sample_id],
                    labels,
                    next_index,
                    adapter_config,
                    next_index.min,
                )
                next_step.set(queue=chain_[0].get_task_queue())
                LOG.debug(f"recursing with range {next_index.min}:{next_index.max}, {next_index.name} {signature(next_step)}")
                LOG.debug(f"queuing samples[{next_index.min}:{next_index.max}] in for {chain_} in {next_index.name}...")
                if self.request.is_eager:
                    next_step.delay()
                else:
                    self.add_to_chord(next_step, lazy=False)
                LOG.debug(f"queued for samples[{next_index.min}:{next_index.max}] in for {chain_} in {next_index.name}")
        except retry_exceptions as e:
            # Reset the index to what it was before so we don't accidentally create a bunch of extra samples upon restart
            next_index.name = next_index_name_before
            raise e

    return ReturnCode.OK


def add_simple_chain_to_chord(self, task_type, chain_, adapter_config):
    """
    Adds a chain of tasks to the current chord.
    :param self: The current task.
    :param task_type: The celery task signature type the new tasks should be.
    :param chain_: The list of tasks to expand.
    :param adapter_config: The adapter config.
    """
    LOG.debug(f"simple chain with {chain_}")
    all_chains = []
    for step in chain_:
        # Make a list of new task signatures with modified cmd and workspace
        # based off of the parameter substitutions and relative_path for
        # a given sample.

        new_steps = [
            task_type.s(step, adapter_config=adapter_config).set(
                queue=step.get_task_queue(),
                task_id=step.get_workspace(),
            )
        ]
        all_chains.append(new_steps)
    chain_1d = get_1d_chain(all_chains)
    launch_chain(self, chain_1d)


def launch_chain(self: "Task", chain_1d: List["Signature"], condense_sig: "Signature" = None):  # noqa: F821
    """
    Given a 1D chain, appropriately launch the signatures it contains.
    If this is a local run, launch the signatures instantly.
    Otherwise, there's two cases:
    a. The chain is dealing with samples (i.e. we'll need to condense status files)
       so create a new chord and add it to the current chord
    b. The chain is NOT dealing with samples so we can just add the signatures to the current chord

    :param `self`: The current task
    :param `chain_1d`: A 1-dimensional list of signatures to launch
    :param `condense_sig`: A signature for condensing the status files. None if condensing isn't needed.
    """
    # If there's nothing in the chain then we won't have to launch anything so check that first
    if chain_1d:
        # Case 1: local run; launch signatures instantly
        if self.request.is_eager:
            for sig in chain_1d:
                sig.delay()
        # Case 2: non-local run; signatures need to be added to the current chord
        else:
            # Case a: we're dealing with a sample hierarchy and need to condense status files when we're done executing tasks
            if condense_sig:
                # This chord makes it so we'll process all tasks in chain_1d, then condense the status files when they're done
                sample_chord = chord(chain_1d, condense_sig)
                self.add_to_chord(sample_chord, lazy=False)
            # Case b: no condensing is needed so just add all the signatures to the chord
            else:
                for sig in chain_1d:
                    self.add_to_chord(sig, lazy=False)


def get_1d_chain(all_chains: List[List["Signature"]]) -> List["Signature"]:  # noqa: F821
    """
    Convert a 2D list of chains into a 1D list.
    :param all_chains: Two-dimensional list of chains [chain_length][number_of_chains]
    :returns: A one-dimensional list representing a chain of tasks
    """
    chain_steps = []
    if len(all_chains) == 1:
        # Steps will be enqueued in a single parallel group
        chain_steps = all_chains[0]

    if len(all_chains) > 1:
        # in this case, we need to make a chain.
        # Celery chains do not natively update any chords they have to be in,
        # which can lead to a chord finishing prematurely,
        # causing future steps to execute before their dependencies are resolved.
        # Celery does provide an API to add a method to a chord dynamically
        # during execution of a task belonging to that chord,
        # so we set up a chain by passing the child member of a chain in as an
        # argument to the signature of the parent member of a chain.
        length = len(all_chains[0])
        for i in range(length):
            # Do the following in reverse order because the replace method
            # generates a new task signature, so we need to make
            # sure we are modifying task signatures before adding them to the
            # kwargs.
            for j in reversed(range(len(all_chains))):
                if j < len(all_chains) - 1:
                    # fmt: off
                    new_kwargs = signature(all_chains[j][i]).kwargs.update(
                        {"next_in_chain": all_chains[j + 1][i]}
                    )
                    # fmt: on
                    all_chains[j][i] = all_chains[j][i].replace(kwargs=new_kwargs)
            chain_steps.append(all_chains[0][i])

    return chain_steps


def gather_statuses(
    sample_index: "SampleIndex", workspace: str, condensed_workspace: str, files_to_remove: List[str]  # noqa: F821
) -> Dict:
    """
    Traverse the sample index and gather all of the statuses into one.

    :param `sample_index`: A SampleIndex object to track this specific sample hierarchy
    :param `workspace`: The full workspace path to the step we're condensing for
    :param `condensed_workspace`: A shortened version of `workspace` that's saved in the status files
    :param `files_to_remove`: An empty list that we'll add filepaths to that need removed
    :returns: A dict of condensed statuses
    """
    LOG.info(f"Gathering statuses to condense for '{condensed_workspace}'")
    condensed_statuses = {}
    for path, _ in sample_index.traverse(conditional=lambda c: c.is_parent_of_leaf):
        # Read in the status data
        sample_workspace = f"{workspace}/{path}"
        status_filepath = f"{sample_workspace}/MERLIN_STATUS.json"
        lock_filepath = f"{sample_workspace}/status.lock"
        if os.path.exists(status_filepath):
            try:
                # NOTE: instead of leaving statuses as dicts read in by JSON, maybe they should each be their own object
                status = read_status(status_filepath, lock_filepath, raise_errors=True)

                # This for loop is just to get the step name that we don't have; it's really not even looping
                for step_name in status:
                    try:
                        # Make sure the status for this sample workspace is in a finished state (not initialized or running)
                        if status[step_name][f"{condensed_workspace}/{path}"]["status"] not in ("INITIALIZED", "RUNNING"):
                            # Add the status data to the statuses we'll write to the condensed file and remove this status file
                            dict_deep_merge(condensed_statuses, status, conflict_handler=status_conflict_handler)
                            files_to_remove.append(status_filepath)
                            files_to_remove.append(lock_filepath)  # Remove the lock files as well as the status files
                    except KeyError:
                        LOG.warning(f"Key error when reading from {sample_workspace}")
            except Timeout:
                # Raising this celery timeout instead will trigger a restart for this task
                raise TimeoutError  # pylint: disable=W0707
            except FileNotFoundError:
                LOG.warning(f"Could not find {status_filepath} while trying to condense. Restarting this task...")
                raise FileNotFoundError  # pylint: disable=W0707
        else:
            # Might be missing a status file in the output if we hit this but we don't want that
            # to fully crash the workflow
            LOG.debug(f"Could not find {status_filepath}, skipping this status file.")

    return condensed_statuses


@shared_task(
    bind=True,
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    priority=get_priority(Priority.LOW),
)
def condense_status_files(self, *args: Any, **kwargs: Any) -> ReturnCode:  # pylint: disable=R0914,W0613
    """
    After a section of the sample tree has finished, condense the status files.

    kwargs should look like so:
    kwargs = {
        "sample_index": SampleIndex Object,
        "workspace": str representing the step's workspace
    }
    """
    # Get the sample index object that we'll use for condensing
    sample_index = kwargs.pop("sample_index", None)
    if not sample_index:
        LOG.warning("Sample index not found. Cannot condense status files.")
        return None

    # Get the full step (or step/parameter) workspace
    workspace = kwargs.pop("workspace", None)
    if not workspace:
        LOG.warning("Workspace not found. Cannot condense status files.")
        return None

    # Get a condensed version of the workspace
    condensed_workspace = kwargs.pop("condensed_workspace", None)
    if not condensed_workspace:
        LOG.warning("Condensed workspace not provided. Cannot condense status files.")
        return None

    # Read in all the statuses from this sample index
    files_to_remove = []
    condensed_statuses = gather_statuses(sample_index, workspace, condensed_workspace, files_to_remove)

    # If there are statuses to write to the condensed status file then write them
    if condensed_statuses:
        condensed_status_filepath = f"{workspace}/MERLIN_STATUS.json"
        condensed_lock_file = f"{workspace}/status.lock"
        lock = FileLock(condensed_lock_file)  # pylint: disable=E0110
        try:
            # Lock the file to avoid race conditions
            with lock.acquire(timeout=20):
                # If the condensed file already exists, grab the statuses from it
                LOG.info(f"Condensing statuses for '{condensed_workspace}' to '{condensed_status_filepath}'")
                if os.path.exists(condensed_status_filepath):
                    with open(condensed_status_filepath, "r") as condensed_status_file:
                        existing_condensed_statuses = json.load(condensed_status_file)
                    # Merging the statuses we're condensing into the already existing statuses
                    # because it's faster at scale than vice versa
                    dict_deep_merge(existing_condensed_statuses, condensed_statuses, conflict_handler=status_conflict_handler)
                    condensed_statuses = existing_condensed_statuses

                # Write the condensed statuses to the condensed status file
                with open(condensed_status_filepath, "w") as condensed_status_file:
                    json.dump(condensed_statuses, condensed_status_file)

                # Remove the status files we just condensed
                for file_to_remove in files_to_remove:
                    LOG.debug(f"Removing '{file_to_remove}'.")
                    os.remove(file_to_remove)
        except Timeout:
            # Raising this celery timeout instead will trigger a restart for this task
            raise TimeoutError  # pylint: disable=W0707

    return ReturnCode.OK


@shared_task(
    bind=True,
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    priority=get_priority(Priority.LOW),
)
def expand_tasks_with_samples(  # pylint: disable=R0913,R0914
    self,
    dag,
    chain_,
    samples,
    labels,
    task_type,
    adapter_config,
    level_max_dirs,
):
    """
    Generate a group of celery chains of tasks from a chain of task names, using merlin
    samples and labels to do variable substitution.

    :param dag : A Merlin DAG.
    :param chain_ : The list of task names to expand into a celery group of celery chains.
    :param samples : The list of lists of merlin sample values to do substitution for.
    :labels : A list of strings containing the label associated with each column in the samples.
    :task_type : The celery task type to create. Currently always merlin_step.
    :adapter_config : A dictionary used for configuring maestro script adapters.
    :level_max_dirs : The max number of directories per level in the sample hierarchy.
    """
    LOG.debug(f"expand_tasks_with_samples called with chain,{chain_}\n")
    # Figure out how many directories there are, make a glob string
    directory_sizes = uniform_directories(len(samples), bundle_size=1, level_max_dirs=level_max_dirs)

    glob_path = "*/" * len(directory_sizes)

    LOG.debug("creating sample_index")
    # Write a hierarchy to get the all paths string
    sample_index = create_hierarchy(
        len(samples),
        bundle_size=1,
        directory_sizes=directory_sizes,
        root="",
        n_digits=len(str(level_max_dirs)),
    )

    LOG.debug("creating sample_paths")
    sample_paths = sample_index.make_directory_string()

    LOG.debug("assembling steps")
    # the steps in the chain
    steps = [dag.step(name) for name in chain_]

    # sub in globs prior to expansion
    # sub the glob command
    steps = [
        step.clone_changing_workspace_and_cmd(cmd_replacement_pairs=parameter_substitutions_for_cmd(glob_path, sample_paths))
        for step in steps
    ]

    # workspaces = [step.get_workspace() for step in steps]
    # LOG.debug(f"workspaces : {workspaces}")

    needs_expansion = is_chain_expandable(steps, labels)

    LOG.debug(f"needs_expansion {needs_expansion}")

    if needs_expansion:
        # prepare_chain_workspace(sample_index, steps)
        sample_index.name = ""
        LOG.debug("queuing merlin expansion tasks")
        found_tasks = False
        conditions = [
            lambda c: c.is_great_grandparent_of_leaf,
            lambda c: c.is_grandparent_of_leaf,
            lambda c: c.is_parent_of_leaf,
            lambda c: c.is_leaf,
        ]
        for condition in conditions:
            if not found_tasks:
                for next_index_path, next_index in sample_index.traverse(conditional=condition):
                    LOG.info(
                        f"generating next step for range {next_index.min}:{next_index.max} {next_index.max - next_index.min}"
                    )
                    next_index.name = next_index_path

                    sig = add_merlin_expanded_chain_to_chord.s(
                        task_type,
                        steps,
                        samples[next_index.min : next_index.max],
                        labels,
                        next_index,
                        adapter_config,
                        next_index.min,
                    )
                    sig.set(queue=steps[0].get_task_queue())

                    if self.request.is_eager:
                        sig.delay()
                    else:
                        LOG.info(f"queuing expansion task {next_index.min}:{next_index.max}")
                        self.add_to_chord(sig, lazy=False)
                    LOG.info(f"merlin expansion task {next_index.min}:{next_index.max} queued")
                    found_tasks = True
    else:
        LOG.debug("queuing simple chain task")
        add_simple_chain_to_chord(self, task_type, steps, adapter_config)
        LOG.debug("simple chain task queued")


# Pylint complains that "self" is unused but it's needed behind the scenes with celery
@shared_task(
    bind=True,
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    acks_late=False,
    reject_on_worker_lost=False,
    name="merlin:shutdown_workers",
    priority=get_priority(Priority.HIGH),
)
def shutdown_workers(self, shutdown_queues):  # pylint: disable=W0613
    """
    This task issues a call to shutdown workers.

    It wraps the stop_celery_workers call as a task.
    It is acknolwedged right away, so that it will not be requeued when
    executed by a worker.

    :param: shutdown_queues: The specific queues to shutdown (list)
    """
    if shutdown_queues is not None:
        LOG.warning(f"Shutting down workers in queues {shutdown_queues}!")
    else:
        LOG.warning("Shutting down workers in all queues!")
    return stop_workers("celery", None, shutdown_queues, None)


# Pylint complains that these args are unused but celery passes args
# here behind the scenes and won't work if these aren't here
@shared_task(
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    name="merlin:chordfinisher",
    priority=get_priority(Priority.LOW),
)
def chordfinisher(*args, **kwargs):  # pylint: disable=W0613
    """.
    It turns out that chain(group,group) in celery does not execute one group
    after another, but executes the groups as if they were independent from
    one another. To get a sync point between groups, we use this method as a
    callback to enforce sync points for chords so we can declare chains of groups
    dynamically.
    """
    return "SYNC"


@shared_task(
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    name="merlin:queue_merlin_study",
    priority=get_priority(Priority.LOW),
)
def queue_merlin_study(study, adapter):
    """
    Launch a chain of tasks based off of a MerlinStudy.
    """
    samples = study.samples
    sample_labels = study.sample_labels
    egraph = study.dag
    LOG.info("Calculating task groupings from DAG.")
    groups_of_chains = egraph.group_tasks("_source")

    # magic to turn graph into celery tasks
    LOG.info("Converting graph to tasks.")
    celery_dag = chain(
        chord(
            group(
                [
                    expand_tasks_with_samples.si(
                        egraph,
                        gchain,
                        samples,
                        sample_labels,
                        merlin_step,
                        adapter,
                        study.level_max_dirs,
                    ).set(queue=egraph.step(chain_group[0][0]).get_task_queue())
                    for gchain in chain_group
                ]
            ),
            chordfinisher.s().set(queue=egraph.step(chain_group[0][0]).get_task_queue()),
        )
        for chain_group in groups_of_chains[1:]
    )
    LOG.info("Launching tasks.")
    return celery_dag.delay(None)
