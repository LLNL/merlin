

"""

"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from merlin.study.study import MerlinStudy


class TaskStatus(Enum):
    """
    Enumeration of possible states for a task during workflow execution.
    
    This enum provides a standardized way to track and report the lifecycle
    state of individual tasks as they progress through the execution pipeline.
    
    Values:
        PENDING: Task is queued and waiting to be executed.
        RUNNING: Task is currently being executed.
        COMPLETED: Task has finished successfully.
        FAILED: Task encountered an error and could not complete.
        SKIPPED: Task was intentionally skipped due to conditions or dependencies.
    
    Example:
        ```python
        result = TaskResult(
            task_name="analyze_data",
            status=TaskStatus.RUNNING,
            start_time=time.time()
        )
        
        # Later, after task completion
        result.status = TaskStatus.COMPLETED
        result.end_time = time.time()
        ```
    """
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


# TODO in Merlin 2.0 we can probably convert this and ExecutionContext into data models for MerlinDatabase to ingest
@dataclass
class TaskResult:
    """
    Represents the execution result and metadata for a single task.
    
    This class captures comprehensive information about a task's execution,
    including its final state, timing information, outputs, and any errors
    encountered. It serves as the primary data structure for tracking task
    outcomes and enabling workflow monitoring and debugging.
    
    Attributes:
        task_name (str): Unique identifier/name of the task that was executed.
        status (TaskStatus): Current execution state of the task.
        start_time (Optional[float]): Unix timestamp when task execution began,
            None if not started yet.
        end_time (Optional[float]): Unix timestamp when task execution completed,
            None if still running or not started.
        result (Any): The return value or output produced by the task,
            None if no result or task failed.
        error (Optional[str]): Error message if the task failed,
            None if no error occurred.
        celery_id (Optional[str]): Celery task ID for distributed execution tracking,
            None for local execution or non-Celery backends.
    
    Example:
        ```python
        # Successful task result
        success_result = TaskResult(
            task_name="data_preprocessing",
            status=TaskStatus.COMPLETED,
            start_time=1693843200.0,
            end_time=1693843260.0,
            result={"processed_rows": 1000, "output_file": "processed_data.csv"},
            celery_id="abc123-def456-ghi789"
        )
        
        # Failed task result
        failed_result = TaskResult(
            task_name="model_training",
            status=TaskStatus.FAILED,
            start_time=1693843300.0,
            end_time=1693843400.0,
            error="Insufficient memory: required 8GB, available 4GB"
        )
        
        # Calculate execution duration
        if success_result.start_time and success_result.end_time:
            duration = success_result.end_time - success_result.start_time
            print(f"Task completed in {duration:.1f} seconds")
        ```
    """
    task_name: str
    status: TaskStatus
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    celery_id: Optional[str] = None


@dataclass
class ExecutionContext:  # TODO entry(ies) for samples?
    """
    Execution context and configuration passed to task executors.
    
    This class encapsulates all the contextual information needed by task
    executors to properly run workflows, including study configuration,
    parameter information, and execution metadata. It serves as a data
    container that travels with the execution plan through the execution pipeline.
    
    Attributes:
        study (study.study.MerlinStudy): The complete Merlin study configuration containing
            DAG structure, samples, and workflow specifications.
        parameter_info (Dict): Parameter definitions and configurations used
            for task parameterization and sample expansion.
        execution_id (str): Unique identifier for this specific workflow execution
            run, useful for tracking and logging.
        metadata (Dict): Additional arbitrary metadata that may be needed
            during execution. Initialized as empty dict if not provided.
    
    Example:
        ```python
        from merlin.study.study import MerlinStudy
        
        # Create execution context for a study run
        context = ExecutionContext(
            study=my_merlin_study,
            parameter_info={
                "temperature": {"type": "float", "range": [100, 500]},
                "pressure": {"type": "int", "values": [1, 5, 10]}
            },
            execution_id="study_run_20240904_143022",
            metadata={
                "user": "researcher",
                "cluster": "quartz",
                "submit_time": "2024-09-04T14:30:22Z"
            }
        )
        
        # Pass context to executor
        executor = CeleryExecutor(app=celery_app)
        results = executor.execute_plan(execution_plan, context)
        ```
    """
    study: MerlinStudy
    parameter_info: Dict
    execution_id: str
    metadata: Dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}