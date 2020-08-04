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

"""Test tasks."""
from __future__ import absolute_import, unicode_literals

import logging
import os

from celery import chain, chord, group, shared_task, signature
from celery.exceptions import OperationalError, TimeoutError

from merlin.common.abstracts.enums import ReturnCode
from merlin.common.sample_index import uniform_directories
from merlin.common.sample_index_factory import create_hierarchy
from merlin.exceptions import (
    HardFailException,
    InvalidChainException,
    RestartException,
    RetryException,
)
from merlin.router import stop_workers
from merlin.spec.expansion import (
    parameter_substitutions_for_cmd,
    parameter_substitutions_for_sample,
)
from merlin.study.step import Step


retry_exceptions = (
    IOError,
    OSError,
    AttributeError,
    TimeoutError,
    OperationalError,
    RetryException,
    RestartException,
)

LOG = logging.getLogger(__name__)

STOP_COUNTDOWN = 60


@shared_task(bind=True, autoretry_for=retry_exceptions, retry_backoff=True)
def merlin_step(self, *args, **kwargs):
    """
    Executes a Merlin Step
    :param args: The arguments, one of which should be an instance of Step
    :param kwargs: The optional keyword arguments that describe adapter_config and
                   the next step in the chain, if there is one.

    Example kwargs dict:
    {"adapter_config": {'type':'local'},
     "next_in_chain": <Step object> } # merlin_step will be added to the current chord
                                      # with next_in_chain as an argument
    """
    step = None
    LOG.debug(f"args is {len(args)} long")

    for a in args:
        if isinstance(a, Step):
            step = a
        else:
            LOG.debug(f"discard argument {a}")

    config = kwargs.pop("adapter_config", {"type": "local"})
    next_in_chain = kwargs.pop("next_in_chain", None)

    if step:
        self.max_retries = step.max_retries
        step_name = step.name()
        step_dir = step.get_workspace()
        LOG.debug(f"merlin_step: step_name '{step_name}' step_dir '{step_dir}'")
        finished_filename = os.path.join(step_dir, "MERLIN_FINISHED")
        # if we've already finished this task, skip it
        if os.path.exists(finished_filename):
            LOG.info(f"Skipping step '{step_name}' in '{step_dir}'.")
            result = ReturnCode.OK
        else:
            result = step.execute(config)
        if result == ReturnCode.OK:
            LOG.info(f"Step '{step_name}' in '{step_dir}' finished successfully.")
            # touch a file indicating we're done with this step
            open(finished_filename, "a").close()
        elif result == ReturnCode.DRY_OK:
            LOG.info(f"Dry-ran step '{step_name}' in '{step_dir}'.")
        elif result == ReturnCode.RESTART:
            LOG.info(f"** Restarting step '{step_name}' in '{step_dir}'.")
            step.restart = True
            raise RestartException
        elif result == ReturnCode.RETRY:
            LOG.warning(f"** Retrying step '{step_name}' in '{step_dir}'.")
            step.restart = False
            raise RetryException
        elif result == ReturnCode.SOFT_FAIL:
            LOG.warning(
                f"*** Step '{step_name}' in '{step_dir}' soft failed. Continuing with workflow."
            )
        elif result == ReturnCode.HARD_FAIL:

            # stop all workers attached to this queue
            step_queue = step.get_task_queue()
            LOG.error(
                f"*** Step '{step_name}' in '{step_dir}' hard failed. Quitting workflow."
            )
            LOG.error(
                f"*** Shutting down all workers connected to this queue ({step_queue}) in {STOP_COUNTDOWN} secs!"
            )
            shutdown = shutdown_workers.s([step_queue])
            shutdown.set(queue=step_queue)
            shutdown.apply_async(countdown=STOP_COUNTDOWN)

            raise HardFailException
        elif result == ReturnCode.STOP_WORKERS:
            LOG.warning(f"*** Shutting down all workers in {STOP_COUNTDOWN} secs!")
            shutdown = shutdown_workers.s(None)
            shutdown.set(queue=step.get_task_queue())
            shutdown.apply_async(countdown=STOP_COUNTDOWN)
        else:
            LOG.warning(
                f"**** Step '{step_name}' in '{step_dir}' had unhandled exit code {result}. Continuing with workflow."
            )
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

    array_of_bools = [step.needs_merlin_expansion(labels) for step in chain_]

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


@shared_task(bind=True, autoretry_for=retry_exceptions, retry_backoff=True)
def add_merlin_expanded_chain_to_chord(
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
    # Use the index to get a path to each sample
    LOG.debug(f"recursing with {len(samples)} samples {samples}")
    if sample_index.is_grandparent_of_leaf or sample_index.is_parent_of_leaf:
        all_chains = []
        LOG.debug(f"gathering up {len(samples)} relative paths")
        relative_paths = [
            os.path.dirname(sample_index.get_path_to_sample(sample_id + min_sample_id))
            for sample_id in range(len(samples))
        ]
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
                        new_workspace=os.path.join(
                            workspace, relative_paths[sample_id]
                        ),
                        cmd_replacement_pairs=parameter_substitutions_for_sample(
                            sample,
                            labels,
                            sample_id + min_sample_id,
                            relative_paths[sample_id],
                        ),
                    ),
                    adapter_config=adapter_config,
                )
                new_step.set(queue=step.get_task_queue())
                new_chain.append(new_step)

            all_chains.append(new_chain)
        LOG.debug(f"adding chain to chord")
        add_chains_to_chord(self, all_chains)
        LOG.debug(f"chain added to chord")
    else:
        # recurse down the sample_index hierarchy
        LOG.debug(f"recursing down sample_index hierarchy")
        for next_index in sample_index.children.values():
            next_index.name = os.path.join(sample_index.name, next_index.name)
            LOG.debug("generating next step")
            next_step = add_merlin_expanded_chain_to_chord.s(
                task_type,
                chain_,
                samples[
                    next_index.min - min_sample_id : next_index.max - min_sample_id
                ],
                labels,
                next_index,
                adapter_config,
                next_index.min,
            )
            next_step.set(queue=chain_[0].get_task_queue())
            LOG.debug(
                f"recursing with range {next_index.min}:{next_index.max}, {next_index.name} {signature(next_step)}"
            )
            LOG.debug(
                f"queuing samples[{next_index.min}:{next_index.max}] in for {chain_} in {next_index.name}..."
            )
            if self.request.is_eager:
                next_step.delay()
            else:
                self.add_to_chord(next_step, lazy=False)
            LOG.debug(
                f"queued for samples[{next_index.min}:{next_index.max}] in for {chain_} in {next_index.name}"
            )

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
                queue=step.get_task_queue()
            )
        ]
        all_chains.append(new_steps)
    add_chains_to_chord(self, all_chains)


def add_chains_to_chord(self, all_chains):
    """
    Adds chains to the current chord.
    :param self: The current task whose chord we will add the chains' tasks to.
    :param all_chains: Two-dimensional list of chains [chain_length][number_of_chains]
    """

    if len(all_chains) == 1:
        # enqueue the steps as a single parallel group
        LOG.debug(f"launching group with {signature(all_chains[0][0])}")
        for sig in all_chains[0]:
            if self.request.is_eager:
                sig.delay()
            else:
                self.add_to_chord(sig, lazy=False)

    if len(all_chains) > 1:
        # in this case, we need to make a chain.
        # Celery chains do not natively update any chords they have to be in,
        # which can lead to a chord finishing prematurely,
        # causing future steps to execute before their dependencies are resolved.
        # Celery does provide an API to add a method to a chord dynamically
        # during execution of a task belonging to that chord,
        # so we set up a chain by passing the child member of a chain in as an
        # argument to the signature of the parent member of a chain.
        chain_steps = []
        length = len(all_chains[0])
        for i in range(length):
            # Do the following in reverse order because the replace method
            # generates a new task signature, so we need to make
            # sure we are modifying task signatures before adding them to the
            # kwargs.
            for g in reversed(range(len(all_chains))):
                if g < len(all_chains) - 1:
                    new_kwargs = signature(all_chains[g][i]).kwargs.update(
                        {"next_in_chain": all_chains[g + 1][i]}
                    )
                    all_chains[g][i] = all_chains[g][i].replace(kwargs=new_kwargs)
            chain_steps.append(all_chains[0][i])

        for sig in chain_steps:
            LOG.debug(f"launching chain {signature(sig)}")
            if self.request.is_eager:
                sig.delay()
            else:
                self.add_to_chord(sig, lazy=False)
    return ReturnCode.OK


@shared_task(bind=True, autoretry_for=retry_exceptions, retry_backoff=True)
def expand_tasks_with_samples(
    self,
    dag,
    chain_,
    samples,
    labels,
    task_type,
    adapter_config,
    level_max_dirs,
    **kwargs,
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
    directory_sizes = uniform_directories(
        len(samples), bundle_size=1, level_max_dirs=level_max_dirs
    )

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
        step.clone_changing_workspace_and_cmd(
            cmd_replacement_pairs=parameter_substitutions_for_cmd(
                glob_path, sample_paths
            )
        )
        for step in steps
    ]

    # workspaces = [step.get_workspace() for step in steps]
    # LOG.debug(f"workspaces : {workspaces}")

    needs_expansion = is_chain_expandable(steps, labels)

    LOG.debug(f"needs_expansion {needs_expansion}")

    if needs_expansion:
        # prepare_chain_workspace(sample_index, steps)
        sample_index.name = ""
        LOG.debug(f"queuing merlin expansion tasks")
        found_tasks = False
        conditions = [
            lambda c: c.is_great_grandparent_of_leaf,
            lambda c: c.is_grandparent_of_leaf,
            lambda c: c.is_parent_of_leaf,
            lambda c: c.is_leaf,
        ]
        for condition in conditions:
            if not found_tasks:
                for next_index_path, next_index in sample_index.traverse(
                    conditional=condition
                ):
                    LOG.info(
                        f"generating next step for range {next_index.min}:{next_index.max} {next_index.max-next_index.min}"
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
                        LOG.info(
                            f"queuing expansion task {next_index.min}:{next_index.max}"
                        )
                        self.add_to_chord(sig, lazy=False)
                    LOG.info(
                        f"merlin expansion task {next_index.min}:{next_index.max} queued"
                    )
                    found_tasks = True
    else:
        LOG.debug(f"queuing simple chain task")
        add_simple_chain_to_chord(self, task_type, steps, adapter_config)
        LOG.debug(f"simple chain task queued")


@shared_task(
    bind=True,
    autoretry_for=retry_exceptions,
    retry_backoff=True,
    acks_late=False,
    reject_on_worker_lost=False,
    name="merlin:shutdown_workers",
)
def shutdown_workers(self, shutdown_queues):
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
        LOG.warning(f"Shutting down workers in all queues!")
    return stop_workers("celery", None, shutdown_queues, None)


@shared_task(
    autoretry_for=retry_exceptions, retry_backoff=True, name="merlin:chordfinisher"
)
def chordfinisher(*args, **kwargs):
    """.
    It turns out that chain(group,group) in celery does not execute one group
    after another, but executes the groups as if they were independent from
    one another. To get a sync point between groups, we use this method as a
    callback to enforce sync points for chords so we can declare chains of groups
    dynamically.
    """
    return "SYNC"


@shared_task(
    autoretry_for=retry_exceptions, retry_backoff=True, name="merlin:queue_merlin_study"
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
            chordfinisher.s().set(
                queue=egraph.step(chain_group[0][0]).get_task_queue()
            ),
        )
        for chain_group in groups_of_chains[1:]
    )
    LOG.info("Launching tasks.")
    return celery_dag.delay(None)
