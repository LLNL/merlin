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
from typing import Dict, Any, Optional

from merlin.common.enums import ReturnCode
from merlin.study.step import Step
from merlin.exceptions import MaxRetriesExceededError

LOG = logging.getLogger(__name__)


class StepExecutionResult:
    """Simple result object for step execution."""
    
    def __init__(self, return_code: ReturnCode, step_name: str = "", execution_time: float = 0):
        self.return_code = return_code
        self.step_name = step_name
        self.execution_time = execution_time


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
            # Import here to avoid circular dependencies
            from merlin.execution.step_executor import StepExecutor, RetryHandler  # pylint: disable=C0415
            
            # Set max retries from step if not provided
            if max_retries is None:
                max_retries = step.max_retries
            
            LOG.info(f"Executing step: {step.name()} in {step.get_workspace()}")
            
            # Execute the step using existing StepExecutor
            executor = StepExecutor()
            result = executor.execute_step(step, adapter_config)
            
            # Handle retry logic generically (extracted from original merlin_step)
            if RetryHandler.should_retry(result.return_code):
                updated_return_code = RetryHandler.handle_retry(
                    step, result.return_code, retry_count
                )
                if updated_return_code in (ReturnCode.RESTART, ReturnCode.RETRY):
                    if retry_count < max_retries:
                        LOG.info(f"Step {step.name()} requesting retry ({retry_count + 1}/{max_retries})")
                        return StepExecutionResult(
                            return_code=updated_return_code,
                            step_name=step.name(),
                            execution_time=time.time() - start_time
                        )
                    else:
                        LOG.warning(f"Step '{step.name()}' has reached its retry limit. Marking as SOFT_FAIL.")
                        step.mstep.mark_end(ReturnCode.SOFT_FAIL, max_retries=True)
                        return StepExecutionResult(
                            return_code=ReturnCode.SOFT_FAIL,
                            step_name=step.name(),
                            execution_time=time.time() - start_time
                        )
                result.return_code = updated_return_code
            
            LOG.info(f"Step {step.name()} completed with return code: {result.return_code}")
            
            return StepExecutionResult(
                return_code=result.return_code,
                step_name=step.name(),
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            LOG.error(f"Step {step.name()} failed with error: {e}")
            return StepExecutionResult(
                return_code=ReturnCode.HARD_FAIL,
                step_name=step.name(),
                execution_time=time.time() - start_time
            )