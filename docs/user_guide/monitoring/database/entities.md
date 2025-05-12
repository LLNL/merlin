# Understanding Merlin's Database Entities

Merlin's database has the following entities:

- [`StudyEntity`](#study-entity)
- [`RunEntity`](#run-entity)
- Two worker entities:
    - [`LogicalWorkerEntity`](#logical-worker-entity)
    - [`PhysicalWorkerEntity`](#physical-worker-entity)

## Study Entity

The `StudyEntity` represents a single “study” or experiment grouping in Merlin. Each entry is unique to study name (defined in the [`description` block](../../specification.md#the-description-block) of the specification file). Each study acts as a namespace under which runs are organized and tracked.

| Key    | Type          | Description                                                      |
| ------ | ------------- | ---------------------------------------------------------------- |
| `id`   | `uuid4`       | Primary key. Unique identifier for the study.                    |
| `name` | `str`         | Human-readable name for the study, unique to each `StudyEntity`. |
| `runs` | `List[uuid4]` | List of Run IDs associated with this study.                      |

**Relationships:**

- One-to-many with [`RunEntity`](#run-entity): A single study can have multiple runs.

## Run Entity

The `RunEntity` represents a single execution of a study. It captures the configuration, intermediate data, and relationships to other entities (like workers and studies).

| Column         | Type             | Description                                                                       |
| -------------- | ---------------- | --------------------------------------------------------------------------------- |
| `id`           | `uuid4`          | Primary key. Unique identifier for the run.                                       |
| `study_id`     | `uuid4`          | Foreign key → StudyEntity(`id`). Which study this run belongs to.                 |
| `workspace`    | `str`            | Filesystem path where outputs of the study are stored.                            |
| `steps`        | `List[uuid4]`    | Ordered list of Step IDs executed in this run.                                    |
| `queues`       | `List[str]`      | List of task queue names used by this run.                                        |
| `workers`      | `List[uuid4]`    | List of [`LogicalWorker`](#logical-worker-entity) IDs serving tasks for this run. |
| `parent`       | `uuid4 \| NULL`  | ID of parent run (if this run was started by another run).                        |
| `child`        | `uuid4 \| NULL`  | ID of child run (if this run spawned a new run).                                  |
| `run_complete` | `bool`           | Indicates whether the run has finished.                                           |
| `parameters`   | `Dict`           | Arbitrary key/value parameters provided to the run.                               |
| `samples`      | `Dict`           | Arbitrary samples provided to the run.                                            |

**Relationships:**

- Many-to-one with [`StudyEntity`](#study-entity): Multiple runs can be assigned to the same study.
- Many-to-many with [`LogicalWorkerEntity`](#logical-worker-entity): Multiple runs can be linked to multiple logical workers.
- Optional one-to-one with parent/child `RunEntity`: A single run can link to another run.

## Worker Entities

Merlin supports two distinct worker models: logical and physical. Logical workers define high-level behavior and configuration. Physical workers represent actual runtime processes launched from logical definitions. The below sections will go into further detail on both entities.

### Logical Worker Entity

The `LogicalWorkerEntity` defines an abstract worker configuration — including queues and a name — which serves as a template for actual (physical) worker instances. Each logical worker is unique to its name and queues. For instance, `LogicalWorker(name=worker1, queues=[queue1, queue2])` is different from `LogicalWorker(name=worker1, queues=[queue1])` which is also different from `LogicalWorker(name=worker2, queues=[queue1, queue2])`.

| Column             | Type             | Description                                                                                      |
| ------------------ | ---------------- | ------------------------------------------------------------------------------------------------ |
| `id`               | `uuid4`          | Primary key. Deterministically generated from `name` + `queues`.                                 |
| `name`             | `str`            | Logical name of the worker (e.g., `"data-processor"`).                                           |
| `queues`           | `List[str]`      | The set of queue names this logical worker listens on.                                           |
| `runs`             | `List[uuid4]`    | List of [`RunEntity`](#run-entity) IDs currently using this logical worker.                      |
| `physical_workers` | `List[uuid4]`    | List of [`PhysicalWorker`](#physical-worker-entity) IDs instantiated from this logical template. |

**Relationships:**

- One-to-many with [`PhysicalWorkerEntity`](#physical-worker-entity): A single logical worker can have multiple physical worker instances.
- Many-to-many with [`RunEntity`](#run-entity): Multiple logical workers can be linked to multiple runs.

### Physical Worker Entity

The `PhysicalWorkerEntity` represents an actual running instance of a worker process, created from a logical worker definition. It contains runtime-specific metadata for monitoring and control.

| Column                | Type           | Description                                                     |
| --------------------- | -------------- | --------------------------------------------------------------- |
| `id`                  | `uuid4`        | Primary key. Unique identifier for this running process.        |
| `logical_worker_id`   | `uuid4`        | Foreign key → LogicalWorkerEntity(`id`).                        |
| `name`                | `str`          | Full Celery worker name (e.g., `celery@hostname`).              |
| `launch_cmd`          | `str`          | Exact CLI used to start the worker.                             |
| `args`                | `Dict`         | Additional runtime args or config passed to the worker process. |
| `pid`                 | `str`          | OS process ID in string format.                                 |
| `status`              | `WorkerStatus` | Current status (e.g., `RUNNING`, `STOPPED`).                    |
| `heartbeat_timestamp` | `datetime`     | Last time the worker checked in.                                |
| `latest_start_time`   | `datetime`     | When this process was most recently (re)launched.               |
| `host`                | `str`          | Hostname or IP where this process is running.                   |
| `restart_count`       | `int`          | How many times the process has been restarted.                  |

**Relationships:**

- Many-to-one with [`LogicalWorkerEntity`](#logical-worker-entity): Multiple physical workers can be linked to multiple logical workers.
