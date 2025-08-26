##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Generic step executor extracted from merlin_step Celery task.

This module contains the pure business logic for executing Merlin steps
without any Celery dependencies, extracted from the original merlin_step function.
"""

import logging
import time
import os
from typing import Dict, Any, Optional

from merlin.common.enums import ReturnCode
from merlin.study.step import Step
from celery.exceptions import MaxRetriesExceededError

LOG = logging.getLogger(__name__)


class ShutdownWorkers:
    """Mock Celery task for shutting down workers."""
    
    def s(self, queues):
        """Create a signature for shutdown_workers with given queues."""
        LOG.warning(f"shutdown_workers signature created for queues {queues}")
        return self
        
    def set(self, **kwargs):
        """Set signature options."""
        LOG.warning(f"shutdown_workers signature set with options {kwargs}")
        return self
        
    def apply_async(self):
        """Apply signature asynchronously."""
        LOG.warning("shutdown_workers signature applied asynchronously")
        return self


# Module-level instance to match expected interface
shutdown_workers = ShutdownWorkers()


class StepExecutionResult:
    """Simple result object for step execution."""
    
    def __init__(self, return_code: ReturnCode, step_name: str = "", step_dir: str = "", 
                 execution_time: float = 0, finished_filename: str = ""):
        self.return_code = return_code
        self.step_name = step_name
        self.step_dir = step_dir
        self.execution_time = execution_time
        self.finished_filename = finished_filename


class GenericStepExecutor:
    """Backend-agnostic step execution logic extracted from merlin_step."""
    
    def __init__(self):
        self.retry_count = 0
        self.max_retries = None
    
    def execute_step(self, step: Step, adapter_config: Dict[str, str], 
                    retry_count: int = 0, max_retries: Optional[int] = None,
                    next_in_chain: Optional[Step] = None) -> StepExecutionResult:
        """
        Execute a Merlin step with backend-agnostic logic.
        
        This contains the core logic extracted from the original merlin_step
        Celery task, but without Celery-specific dependencies.
        
        Args:
            step: The Step object to execute
            adapter_config: Configuration for the script adapter
            retry_count: Current retry attempt count
            max_retries: Maximum number of retries allowed
            next_in_chain: Next step in the chain (for coordination)
            
        Returns:
            TaskExecutionResult with execution outcome
        """
        start_time = time.time()
        task_id = step.get_workspace()
        
        try:
            # Set max retries from step if not provided
            if max_retries is None:
                max_retries = step.max_retries
            
            step_name = step.name()
            step_dir = step.get_workspace()
            finished_filename = os.path.join(step_dir, "MERLIN_FINISHED")
            
            # Check if we've already finished this task, skip it
            if os.path.exists(finished_filename):
                LOG.info(f"Skipping step '{step_name}' in '{step_dir}'.")
                return StepExecutionResult(
                    return_code=ReturnCode.OK,
                    step_name=step_name,
                    step_dir=step_dir,
                    execution_time=time.time() - start_time,
                    finished_filename=finished_filename
                )
            
            LOG.info(f"Executing step: {step_name} in {step_dir}")
            
            # Execute the step directly (similar to original merlin_step)
            result_code = step.execute(adapter_config)
            # Don't call mark_end here - let retry logic handle it to avoid double calls
            
            # Handle retry logic (extracted from original merlin_step)
            if result_code == ReturnCode.RESTART:
                if retry_count < max_retries:
                    LOG.info(f"Step '{step.name()}' requesting restart ({retry_count + 1}/{max_retries})")
                    step.restart = True
                    step.mstep.mark_restart()
                    return StepExecutionResult(
                        return_code=ReturnCode.RESTART,
                        step_name=step_name,
                        step_dir=step_dir,
                        execution_time=time.time() - start_time,
                        finished_filename=finished_filename
                    )
                else:
                    LOG.warning(f"Step '{step.name()}' has reached its retry limit. Marking as SOFT_FAIL.")
                    step.mstep.mark_end(ReturnCode.SOFT_FAIL, max_retries=True)
                    return StepExecutionResult(
                        return_code=ReturnCode.SOFT_FAIL,
                        step_name=step_name,
                        step_dir=step_dir,
                        execution_time=time.time() - start_time,
                        finished_filename=finished_filename
                    )
            elif result_code == ReturnCode.RETRY:
                if retry_count < max_retries:
                    LOG.info(f"Step '{step.name()}' requesting retry ({retry_count + 1}/{max_retries})")
                    step.mstep.mark_restart()  # Call mark_restart for retry
                    return StepExecutionResult(
                        return_code=ReturnCode.RETRY,
                        step_name=step_name,
                        step_dir=step_dir,
                        execution_time=time.time() - start_time,
                        finished_filename=finished_filename
                    )
                else:
                    LOG.warning(f"Step '{step.name()}' has reached its retry limit. Marking as SOFT_FAIL.")
                    step.mstep.mark_end(ReturnCode.SOFT_FAIL, max_retries=True)
                    return StepExecutionResult(
                        return_code=ReturnCode.SOFT_FAIL,
                        step_name=step_name,
                        step_dir=step_dir,
                        execution_time=time.time() - start_time,
                        finished_filename=finished_filename
                    )
            elif result_code == ReturnCode.RAISE_ERROR:
                LOG.error(f"Step '{step_name}' returned RAISE_ERROR - raising exception")
                raise Exception("Exception raised by request from the user")
            elif result_code == ReturnCode.HARD_FAIL:
                LOG.error(f"Step '{step_name}' returned HARD_FAIL - shutting down workers")
                task_queue = step.get_task_queue() if hasattr(step, 'get_task_queue') else 'default'
                shutdown_sig = shutdown_workers.s([task_queue])
                shutdown_sig.set(queue=task_queue)
                shutdown_sig.apply_async()
            elif result_code == ReturnCode.STOP_WORKERS:
                LOG.warning(f"Step '{step_name}' returned STOP_WORKERS - shutting down workers")
                # STOP_WORKERS shuts down all workers, not just a specific queue
                shutdown_sig = shutdown_workers.s(None)
                shutdown_sig.apply_async()
            
            # Create finished file for successful completion
            if result_code == ReturnCode.OK:
                LOG.info(f"Step '{step_name}' in '{step_dir}' finished successfully.")
                # Touch a file indicating we're done with this step
                try:
                    with open(finished_filename, "a"):
                        pass
                except (OSError, IOError) as e:
                    LOG.warning(f"Could not create finished file {finished_filename}: {e}")
            elif result_code == ReturnCode.DRY_OK:
                LOG.info(f"Dry-ran step '{step_name}' in '{step_dir}'.")
            
            # Mark the end for non-retry cases
            step.mstep.mark_end(result_code)
            
            LOG.info(f"Step {step_name} completed with return code: {result_code}")
            
            return StepExecutionResult(
                return_code=result_code,
                step_name=step_name,
                step_dir=step_dir,
                execution_time=time.time() - start_time,
                finished_filename=finished_filename
            )
            
        except Exception as e:
            LOG.error(f"Step {step.name()} failed with error: {e}")
            # Re-raise exception to match test expectations
            raise