# Changelog
All notable changes to Merlin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.13.0]
### Added
- API documentation for Merlin's core codebase
- Added support for Python 3.12 and 3.13
- Added additional tests for the `merlin run` and `merlin purge` commands
- Aliased types to represent different types of pytest fixtures
- New test condition `StepFinishedFilesCount` to help search for `MERLIN_FINISHED` files in output workspaces
- Added "Unit-tests" GitHub action to run the unit test suite
- Added `CeleryTaskManager` context manager to the test suite to ensure tasks are safely purged from queues if tests fail
- Added `command-tests`, `workflow-tests`, and `integration-tests` to the Makefile
- Added tests and docs for the new `merlin config` options
- Python 3.8 now requires `orderly-set==5.3.0` to avoid a bug with the deepdiff library
- New step 'Reinstall pip to avoid vendored package corruption' to CI workflow jobs that use pip
- New GitHub actions to reduce common code in CI
- COPYRIGHT file for ownership details
- New check for copyright headers in the Makefile
- A page in the docs explaining the `feature_demo` example
- Unit tests for the `spec/` folder
- Batch block now supports placeholder entries
- Automatic task retry for Celery's `BackendStoreError`

### Changed
- The `merlin config` command:
  - Now defaults to the LaunchIT setup
  - No longer required to have configuration named `app.yaml`
  - New subcommands:
    - `create`: Creates a new configuration file
    - `update-broker`: Updates the `broker` section of the configuration file
    - `update-backend`: Updates the `results_backend` section of the configuration file
    - `use`: Point your active configuration to a new configuration file
- The `merlin server` command no longer modifies the `~/.merlin/app.yaml` file by default. Instead, it modifies the `./merlin_server/app.yaml` file.
- Dropped support for Python 3.7
- Celery settings have been updated to try to improve resiliency
- Ported all distributed tests of the integration test suite to pytest
  - There is now a `commands/` directory and a `workflows/` directory under the integration suite to house these tests
  - Removed the "Distributed-tests" GitHub action as these tests will now be run under "Integration-tests"
- Removed `e2e-distributed*` definitions from the Makefile
- CI to use new actions
- Copyright headers in all files
  - These now point to the LICENSE and COPYRIGHT files
  - LICENSE: Legal permissions (e.g., MIT terms)
  - COPYRIGHT: Ownership, institutional metadata
  - Make commands that change version/copyright year have been modified
- Refactored the `main.py` module so that it's broken into smaller, more-manageable pieces

### Fixed
- Running Merlin locally no longer requires an `app.yaml` configuration file
- Removed dead lgtm link
- Potential security vulnerabilities related to logging
- Bug where the `--task-status` and `--return-code` filters of `merlin detailed-status` only accepted filters in all caps
- Bug where absolute path was required in the broker password field
- Bug where potential studies list was not alphabetically sorted when running `merlin status <yaml>`

## [1.12.2]
### Added
- Conflict handler option to the `dict_deep_merge` function in `utils.py`
- Ability to add module-specific pytest fixtures
- Added fixtures specifically for testing status functionality
- Added tests for reading and writing status files, and status conflict handling
- Added tests for the `dict_deep_merge` function
- Pytest-mock as a dependency for the test suite (necessary for using mocks and fixtures in the same test)
- New github action test to make sure target branch has been merged into the source first, so we know histories are ok
- Check in the status commands to make sure we're not pulling statuses from nested workspaces
- Added `setuptools` as a requirement for python 3.12 to recognize the `pkg_resources` library
- Patch to celery results backend to stop ChordErrors being raised and breaking workflows when a single task fails
- New step return code `$(MERLIN_RAISE_ERROR)` to force an error to be raised by a task (mainly for testing)
  - Added description of this to docs
- New test to ensure a single failed task won't break a workflow
- Several new unit tests for the following subdirectories:
  - `merlin/common/`
  - `merlin/config/`
  - `merlin/examples/`
  - `merlin/server/`
- Context managers for the `conftest.py` file to ensure safe spin up and shutdown of fixtures
  - `RedisServerManager`: context to help with starting/stopping a redis server for tests
  - `CeleryWorkersManager`: context to help with starting/stopping workers for tests
- Ability to copy and print the `Config` object from `merlin/config/__init__.py`
- Equality method to the `ContainerFormatConfig` and `ContainerConfig` objects from `merlin/server/server_util.py`

### Changed
- `merlin info` is cleaner and gives python package info
- merlin version now prints with every banner message
- Applying filters for `merlin detailed-status` will now log debug statements instead of warnings
- Modified the unit tests for the `merlin status` command to use pytest rather than unittest
- Added fixtures for `merlin status` tests that copy the workspace to a temporary directory so you can see exactly what's run in a test
- Batch block and workers now allow for variables to be used in node settings
- Task id is now the path to the directory
- Split the `start_server` and `config_server` functions of `merlin/server/server_commands.py` into multiple functions to make testing easier
- Split the `create_server_config` function of `merlin/server/server_config.py` into two functions to make testing easier
- Combined `set_snapshot_seconds` and `set_snapshot_changes` methods of `RedisConfig` into one method `set_snapshot`

### Fixed
- Bugfix for output of `merlin example openfoam_wf_singularity`
- A bug with the CHANGELOG detection test when the target branch isn't in the ci runner history
- Link to Merlin banner in readme
- Issue with escape sequences in ascii art (caught by python 3.12)
- Bug where Flux wasn't identifying total number of nodes on an allocation
  - Not supporting Flux versions below 0.17.0


## [1.12.1]
### Added
- New Priority.RETRY value for the Celery task priorities. This will be the new highest priority.
- Support for the status command to handle multiple workers on the same step
- Documentation on how to run cross-node workflows with a containerized server (`merlin server`)

### Changed
- Modified some tests in `test_status.py` and `test_detailed_status.py` to accommodate bugfixes for the status commands

### Fixed
- Bugfixes for the status commands:
  - Fixed "DRY RUN" naming convention so that it outputs in the progress bar properly
  - Fixed issue where a step that was run with one sample would delete the status file upon condensing
  - Fixed issue where multiple workers processing the same step would break the status file and cause the workflow to crash
  - Added a catch for the JSONDecodeError that would potentially crash a run
  - Added a FileLock to the status write in `_update_status_file()` of `MerlinStepRecord` to avoid potential race conditions (potentially related to JSONDecodeError above)
  - Added in `export MANPAGER="less -r"` call behind the scenes for `detailed-status` to fix ASCII error

## [1.12.0]
### Added
- A new command `merlin queue-info` that will print the status of your celery queues
  - By default this will only pull information from active queues
  - There are options to look for specific queues (`--specific-queues`), queues defined in certain spec files (`--spec`; this is the same functionality as the `merlin status` command prior to this update), and queues attached to certain steps (`--steps`)
  - Queue info can be dumped to outfiles with `--dump`
- A new command `merlin detailed-status` that displays task-by-task status information about your study
  - This has options to filter by return code, task queues, task statuses, and workers
  - You can set a limit on the number of tasks to display
  - There are 3 options to modify the output display
- Docs for all of the monitoring commands
- New file `merlin/study/status.py` dedicated to work relating to the status command
  - Contains the Status and DetailedStatus classes
- New file `merlin/study/status_renderers.py` dedicated to formatting the output for the detailed-status command
- New file `merlin/common/dumper.py` containing a Dumper object to help dump output to outfiles
- Study name and parameter info now stored in the DAG and MerlinStep objects
- Added functions to `merlin/display.py` that help display status information:
  - `display_task_by_task_status` handles the display for the `merlin detailed-status` command
  - `display_status_summary` handles the display for the `merlin status` command
  - `display_progress_bar` generates and displays a progress bar
- Added new methods to the MerlinSpec class:
  - get_worker_step_map()
  - get_queue_step_relationship()
  - get_tasks_per_step()
  - get_step_param_map()
- Added methods to the MerlinStepRecord class to mark status changes for tasks as they run (follows Maestro's StepRecord format mostly)
- Added methods to the Step class:
  - establish_params()
  - name_no_params()
- Added a property paramater_labels to the MerlinStudy class
- Added two new utility functions:
  - dict_deep_merge() that deep merges two dicts into one
  - ws_time_to_dt() that converts a workspace timestring (YYYYMMDD-HHMMSS) to a datetime object
- A new celery task `condense_status_files` to be called when sets of samples finish
- Added a celery config setting `worker_cancel_long_running_tasks_on_connection_loss` since this functionality is about to change in the next version of celery
- Tests for the Status and DetailedStatus classes
  - this required adding a decent amount of test files to help with the tests; these can be found under the tests/unit/study/status_test_files directory
- Pytest fixtures in the `conftest.py` file of the integration test suite
  - NOTE: an export command `export LC_ALL='C'` had to be added to fix a bug in the WEAVE CI. This can be removed when we resolve this issue for the `merlin server` command
- Coverage to the test suite. This includes adding tests for:
  - `merlin/common/`
  - `merlin/config/`
  - `merlin/examples/`
  - `celeryadapter.py`
- Context managers for the `conftest.py` file to ensure safe spin up and shutdown of fixtures
  - `RedisServerManager`: context to help with starting/stopping a redis server for tests
  - `CeleryWorkersManager`: context to help with starting/stopping workers for tests
- Ability to copy and print the `Config` object from `merlin/config/__init__.py`

### Changed
- Reformatted the entire `merlin status` command
  - Now accepts both spec files and workspace directories as arguments
  - Removed the --steps flag
  - Replaced the --csv flag with the --dump flag
  - New functionality:
    - Shows step_by_step progress bar for tasks
    - Displays a summary of task statuses below the progress bar
- Split the `add_chains_to_chord` function in `merlin/common/tasks.py` into two functions:
  - `get_1d_chain` which converts a 2D list of chains into a 1D list
  - `launch_chain` which launches the 1D chain
- Pulled the needs_merlin_expansion() method out of the Step class and made it a function instead
- Removed `tabulate_info` function; replaced with tabulate from the tabulate library
- Moved `verify_filepath` and `verify_dirpath` from `merlin/main.py` to `merlin/utils.py`
- The entire documentation has been ported to MkDocs and re-organized
  - *Dark Mode*
  - New "Getting Started" example for a simple setup tutorial
  - More detail on configuration instructions
  - There's now a full page on installation instructions
  - More detail on explaining the spec file
  - More detail with the CLI page
  - New "Running Studies" page to explain different ways to run studies, restart them, and accomplish command line substitution
  - New "Interpreting Output" page to help users understand how the output workspace is generated in more detail
  - New "Examples" page has been added
  - Updated "FAQ" page to include more links to helpful locations throughout the documentation
  - Set up a place to store API docs
  - New "Contact" page with info on reaching Merlin devs
- The Merlin tutorial defaults to using Singularity rather than Docker for the OpenFoam example. Minor tutorial fixes have also been applied.

### Fixed
- The `merlin status` command so that it's consistent in its output whether using redis or rabbitmq as the broker
- The `merlin monitor` command will now keep an allocation up if the queues are empty and workers are still processing tasks
- Add the restart keyword to the specification docs
- Cyclical imports and config imports that could easily cause ci issues
## [1.11.1]
### Fixed
- Typo in `batch.py` that caused lsf launches to fail (`ALL_SGPUS` changed to `ALL_GPUS`)

## [1.11.0]
### Added
- New reserved variable:
  - `VLAUNCHER`: The same functionality as the `LAUNCHER` variable, but will substitute shell variables `MERLIN_NODES`, `MERLIN_PROCS`, `MERLIN_CORES`, and `MERLIN_GPUS` for nodes, procs, cores per task, and gpus

### Changed
- Hardcoded Sphinx v5.3.0 requirement is now removed so we can use latest Sphinx

### Fixed
- A bug where the filenames in iterative workflows kept appending `.out`, `.partial`, or `.expanded` to the filenames stored in the `merlin_info/` subdirectory
- A bug where a skewed sample hierarchy was created when a restart was necessary in the `add_merlin_expanded_chain_to_chord` task

## [1.10.3]
### Added
- The *.conf regex for the recursive-include of the merlin server directory so that pip will add it to the wheel
- A note to the docs for how to fix an issue where the `merlin server start` command hangs

### Changed
- Bump certifi from 2022.12.7 to 2023.7.22 in /docs
- Bump pygments from 2.13.0 to 2.15.0 in /docs
- Bump requests from 2.28.1 to 2.31.0 in /docs

## [1.10.2]
### Fixed
- A bug where the .orig, .partial, and .expanded file names were using the study name rather than the original file name
- A bug where the openfoam_wf_singularity example was not being found
- Some build warnings in the docs (unknown targets, duplicate targets, title underlines too short, etc.)
- A bug where when the output path contained a variable that was overridden, the overridden variable was not changed in the output_path
- A bug where permission denied errors happened when checking for system scheduler

### Added
- Tests for ensuring `$(MERLIN_SPEC_ORIGINAL_TEMPLATE)`, `$(MERLIN_SPEC_ARCHIVED_COPY)`, and `$(MERLIN_SPEC_EXECUTED_RUN)` are stored correctly
- A pdf download format for the docs
- Tests for cli substitutions

### Changed
- The ProvenanceYAMLFileHasRegex condition for integration tests now saves the study name and spec file name as attributes instead of just the study name
  - This lead to minor changes in 3 tests ("local override feature demo", "local pgen feature demo", and "remote feature demo") with what we pass to this specific condition
- Updated scikit-learn requirement for the openfoam_wf_singularity example
- Uncommented Latex support in the docs configuration to get pdf builds working

## [1.10.1]
### Fixed
- A bug where assigning a worker all steps also assigned steps to the default worker

### Added
- Tests to make sure the default worker is being assigned properly

### Changed
- Requirement name in examples/workflows/remote_feature_demo/requirements.txt and examples/workflows/feature_demo/requirements.txt from sklearn to scikit-learn since sklearn is now deprecated

## [1.10.0]
### Fixed
- Pip wheel wasn't including .sh files for merlin examples
- The learn.py script in the openfoam_wf* examples will now create the missing Energy v Lidspeed plot
- Fixed the flags associated with the `stop-workers` command (--spec, --queues, --workers)
- Fixed the --step flag for the `run-workers` command
- Fixed most of the pylint errors that we're showing up when you ran `make check-style`
  - Some errors have been disabled rather than fixed. These include:
    - Any pylint errors in merlin_template.py since it's deprecated now
    - A "duplicate code" instance between a function in `expansion.py` and a method in `study.py`
      - The function is explicitly not creating a MerlinStudy object so the code *must* be duplicate here
    - Invalid-name (C0103): These errors typically relate to the names of short variables (i.e. naming files something like f or errors e)
    - Unused-argument (W0613): These have been disabled for celery-related functions since celery *does* use these arguments behind the scenes
    - Broad-exception (W0718): Pylint wants a more specific exception but sometimes it's ok to have a broad exception
    - Import-outside-toplevel (C0415): Sometimes it's necessary for us to import inside a function. Where this is the case, these errors are disabled
    - Too-many-statements (R0915): This is disabled for the `setup_argparse` function in `main.py` since it's necessary to be big. It's disabled in `tasks.py` and `celeryadapter.py` too until we can get around to refactoring some code there
    - No-else-return (R1705): These are disabled in `router.py` until we refactor the file
    - Consider-using-with (R1732): Pylint wants us to consider using with for calls to subprocess.run or subprocess.Popen but it's not necessary
    - Too-many-arguments (R0913): These are disabled for functions that I believe *need* to have several arguments
      - Note: these could be fixed by using *args and **kwargs but it makes the code harder to follow so I'm opting to not do that
    - Too-many-local-variables (R0914): These are disabled for functions that have a lot of variables
      - It may be a good idea at some point to go through these and try to find ways to shorten the number of variables used or split the functions up
    - Too-many-branches (R0912): These are disabled for certain functions that require a good amount of branching
      - Might be able to fix this in the future if we split functions up more
    - Too-few-public-methods (R0903): These are disabled for classes we may add to in the future or "wrapper" classes
    - Attribute-defined-outside-init (W0201): These errors are only disabled in `specification.py` as they occur in class methods so init() won't be called
- Fixed an issue where the walltime value in the batch block was being converted to an integer instead of remaining in HH:MM:SS format

### Added
- Now loads np.arrays of dtype='object', allowing mix-type sample npy
- Added a singularity container openfoam_wf example
- Added flux native worker launch support
- Added PBS flux launch support
- Added check_for_flux, check_for_slurm, check_for_lsf, and check_for_pbs utility functions
- Tests for the `stop-workers` command
- A function in `run_tests.py` to check that an integration test definition is formatted correctly
- A new dev_workflow example `multiple_workers.yaml` that's used for testing the `stop-workers` command
- Ability to start 2 subprocesses for a single test
- Added the --distributed and --display-tests flags to run_tests.py
  - --distributed: only run distributed tests
  - --display-tests: displays a table of all existing tests and the id associated with each test
- Added the --disable-logs flag to the `run-workers` command
- Merlin will now assign `default_worker` to any step not associated with a worker
- Added `get_step_worker_map()` as a method in `specification.py`
- Added `tabulate_info()` function in `display.py` to help with table formatting
- Added get_flux_alloc function for new flux version >= 0.48.x interface change
- New flags to the `query-workers` command
  - `--queues`: query workers based on the queues they're associated with
  - `--workers`: query workers based on a regex of the names you're looking for
  - `--spec`: query workers based on the workers defined in a spec file

### Changed
- Changed celery_regex to celery_slurm_regex in test_definitions.py
- Reformatted how integration tests are defined and part of how they run
  - Test values are now dictionaries rather than tuples
  - Stopped using `subprocess.Popen()` and `subprocess.communicate()` to run tests and now instead use `subprocess.run()` for simplicity and to keep things up-to-date with the latest subprocess release (`run()` will call `Popen()` and `communicate()` under the hood so we don't have to handle that anymore)
- Rewrote the README in the integration tests folder to explain the new integration test format
- Reformatted `start_celery_workers()` in `celeryadapter.py` file. This involved:
  - Modifying `verify_args()` to return the arguments it verifies/updates
  - Changing `launch_celery_worker()` to launch the subprocess (no longer builds the celery command)
  - Creating `get_celery_cmd()` to do what `launch_celery_worker()` used to do and build the celery command to run
  - Creating `_get_steps_to_start()`, `_create_kwargs()`, and `_get_workers_to_start()` as helper functions to simplify logic in `start_celery_workers()`
- Modified the `merlinspec.json` file:
  - the minimum `gpus per task` is now 0 instead of 1
  - variables defined in the `env` block of a spec file can now be arrays
- Refactored `batch.py`:
  - Merged 4 functions (`check_for_slurm`, `check_for_lsf`, `check_for_flux`, and `check_for_pbs`) into 1 function named `check_for_scheduler`
    - Modified `get_batch_type` to accommodate this change
  - Added a function `parse_batch_block` to handle all the logic of reading in the batch block and storing it in one dict
  - Added a function `get_flux_launch` to help decrease the amount of logic taking place in `batch_worker_launch`
  - Modified `batch_worker_launch` to use the new `parse_batch_block` function
  - Added a function `construct_scheduler_legend` to build a dict that keeps as much information as we need about each scheduler stored in one place
  - Cleaned up the `construct_worker_launch_command` function to utilize the newly added functions and decrease the amount of repeated code
- Changed get_flux_cmd for new flux version >=0.48.x interface
- The `query-workers` command now prints a table as its' output
  - Each row of the `Workers` column has the name of an active worker
  - Each row of the `Queues` column has a list of queues associated with the active worker

## [1.9.1]
### Fixed
- Added merlin/spec/merlinspec.json to MANIFEST.in so pip will actually install it when ran
- Fixed a bug where "from celery import Celery" was failing on python 3.7
- Numpy error about numpy.str not existing from a new numpy release
- Made merlin server configurations into modules that can be loaded and written to users

## [1.9.0]
### Added
- Added support for Python 3.11
- Update docker docs for new rabbitmq and redis server versions
- Added lgtm.com Badge for README.md
- More fixes for lgtm checks.
- Added merlin server command as a container option for broker and results_backend servers.
- Added new documentation for merlin server in docs and tutorial
- Added the flux_exec batch argument to allow for flux exec arguments,
  e.g. flux_exec: flux exec -r "0-1" to run celery workers only on
  ranks 0 and 1 of a multi-rank allocation
- Additional argument in test definitions to allow for a post "cleanup" command
- Capability for non-user block in yaml
- .readthedocs.yaml and requirements.txt files for docs
- Small modifications to the Tutorial, Getting Started, Command Line, and Contributing pages in the docs
- Compatibility with the newest version of Maestro (v. 1.1.9dev1)
- JSON schema validation for Merlin spec files
- New tests related to JSON schema validation
- Instructions in the "Contributing" page of the docs on how to add new blocks/fields to the spec file
- Brief explanation of the $(LAUNCHER) variable in the "Variables" page of the docs

### Changed
- Removed support for Python 3.6
- Rename lgtm.yml to .lgtm.yml
- New shortcuts in specification file (sample_vector, sample_names, spec_original_template, spec_executed_run, spec_archived_copy)
- Update requirements to require redis 4.3.4 for acl user channel support
- Added ssl to the broker and results backend server checks when "merlin info" is called
- Removed theme_override.css from docs/_static/ since it is no longer needed with the updated version of sphinx
- Updated docs/Makefile to include a pip install for requirements and a clean command
- Update to the Tutorial and Contributing pages in the docs
- Changed what is stored in a Merlin DAG
  - We no longer store the entire Maestro ExecutionGraph object
  - We now only store the adjacency table and values obtained from the ExecutionGraph object
- Modified spec verification
- Update to require maestrowf 1.9.1dev1 or later

### Fixed
- Fixed return values from scripts with main() to fix testing errors. 
- CI test for CHANGELOG modifcations
- Typo "cert_req" to "cert_reqs" in the merlin config docs
- Removed emoji from issue templates that were breaking doc builds
- Including .temp template files in MANIFEST
- Styling in the footer for docs
- Horizontal scroll overlap in the variables page of the docs
- Reordered small part of Workflow Specification page in the docs in order to put "samples" back in the merlin block

## [1.8.5]
### Added
- Code updates to satisfy lgtm CI security checker

### Fixed
- A bug in the ssl config was not returning the proper values

## [1.8.4]
### Added
- Auto-release of pypi packages
- Workflows Community Initiative metadata file

### Fixed
- Old references to stale branches

## [1.8.3]
### Added
- Test for `merlin example list`
- Python 3.10 to testing

### Fixed
- The Optimization workflow example now has a ready to use workflow (`optimization_basic.yaml`). This solves the issue faced before with `merlin example list`.
- Redis dependency handled implictly by celery for cross-compatibility

## [1.8.2]
### Added
- Re-enabled distributed integration testing. Added additional examination to distributed testing.

### Fixed
- 'shell' added to unsupported and new_unsupported lists in script_adapter.py, prevents
  `'shell' is not supported -- ommitted` message.
- Makefile target for install-merlin fixed so venv is properly activated to install merlin

### Changed
- Updated the optimization workflow example with a new python template editor script
- CI now splits linting and testing into different tasks for better utilization of
  parallel runners, significant and scalable speed gain over previous setup
- CI now uses caching to restore environment of dependencies, reducing CI runtime
  significantly again beyond the previous improvement. Examines for potential updates to
  dependencies so the environment doesn't become stale.
- CI now examines that the CHANGELOG is updated on PRs.
- Added PyLint pipeline to Github Actions CI (currently no-fail-exit).
- Corrected integration test for dependency to only examine release dependencies.
- PyLint adherence for: celery.py, opennplib.py, config/__init__.py, broker.py,
	configfile.py, formatter.py, main.py, router.py
- Integrated Black and isort into CI

## [1.8.1]

### Fixed
- merlin purge queue name conflict & shell quote escape
- task priority support for amqp, amqps, rediss, redis+socket brokers
- Flake8 compliance

## [1.8.0]

### Added
- `retry_delay` field in a step to specify a countdown in seconds prior to running a
  restart or retry.
- New merlin example `restart_delay` that demonstrates usage of this feature.
- Condition failure reporting, to give greater insight into what caused test failure.
- New fields in config file: `celery.omit_queue_tag` and `celery.queue_tag`, for
  users who wish to have complete control over their queue names. This is a feature 
  of the task priority change.

### Changed
- `feature_demo` now uses `merlin-spellbook` instead of its own scripts.
- Removed the `--mpi=none` `srun` default launch argument. This can be added by
  setting the `launch_args` argument in the batch section in the spec.
- Merlin CI is now handled by Github Actions.
- Certain tests and source code have been refactored to abide by Flake8 conventions.
- Reorganized the `tests` module. Made `unit` dir alongside `integration` dir. Decomposed
  `run_tests.py` into 3 files with distinct responsibilities. Renamed `Condition` classes.
  Grouped cli tests by sub-category for easier developer interpretation.
- Lowered the command line test log level to "ERROR" to reduce spam in `--verbose` mode.
- Now prioritizing workflow tasks over task-expansion tasks, enabling improved
  scalability and server stability.
- Flake8 examination slightly modified for more generous cyclomatic complexity.
	- Certain tests and source code have been refactored to abide by Flake8 conventions.
- `walltime` can be specified in any of hours:minutes:seconds, minutes:seconds or
  seconds format and will be correctly translated for the right batch system syntax

### Fixed
- For Celery calls, explictly wrapped queue string in quotes for robustness. This fixes
a bug that occurred on tsch but not bash in which square brackets in the queue name were 
misinterpreted and caused the command to break.

## [1.7.9]

### Fixed
- Bug that caused steps to raise a fatal error (instead of soft failing) after maxing
  out step retries. Occurred if steps were part of a chord.

## [1.7.8]

### Fixed
- Bug that caused step restarts to lose alternate shell specification, and
  associated CLI `restart_shell` test.

## [1.7.7]

### Fixed
- Bug that caused example workflows with a variable reference in their
  name to be listed by `merlin example list` with variable reference notation.
- Bug that caused `requirements.txt` files to be excluded from generated
  `merlin example` dirs.
- Bug that causes step restarts to lose alternate shell specification. Also added
  CLI test for this case.

### Changed
- Default broker server password is now `jackalope-password`, since `rabbit` is
  currently accessed by developers only.

## [1.7.6]

### Added
- The first version of an optimization workflow, which can be accessed with
  `merlin example optimization`.
- Dev requirement library for finding dependencies (and `make reqlist` target)

### Changed
- Improved warning and help messages about `no_errors`

### Fixed
- Pinned to celery>5.0.3 so that `merlin purge` works again

## [1.7.5]

### Changed
- Now requiring Celery version 5.x.
- Further improvements to the `null_spec` example.

## [1.7.4]

### Fixed
- Users will no longer see the message, "Cannot set the submission time of '<step name>'
  because it has already been set", when tasks are restarted.
- Bug causing `merlin restart` to break.

### Changed
- Improved internal logic beyond the crude fixes of the prior 2 patches.
- Added a developer cli test for the minimum valid spec format.
- Improvements to the `null_spec` example, used for measuring overhead in merlin. Includes
  a new `null_chain` and removes the now-redundant `sim_spec`.

## [1.7.3]

### Fixed
- Completed 1.7.2 fix for `merlin run-workers`.

## [1.7.2]

### Fixed
- Fatal bug triggered by a spec missing the `env` or `global.parameters` sections.

## [1.7.1]

### Added
- When using the `--samplesfile` flag, the samples file is now copied to `merlin_info` for
  provenance.

### Fixed
- Exceptions in connection check sub-process will now be caught.

## [1.7.0]

### Added
- The ability to override any value of the celery configuration thru `app.yaml` in `celery.override`.
- Support and faq entry for `pgen` with `merlin run --pgen` and optional `--parg`.
- Documentation on `level_max_dirs`.
- Easier-to-read provenance specs.
- Documentation on the new 3 types of provenance spec.

### Fixed
- Flux test example data collection for new versions of flux.
- Fixed Docker ubuntu version.
- Removed expansion of env variables in shell sections (`cmd` and `restart`) of provenance
  specs. This allows the shell command itself to expand environment variables, and gives
  users greater flexibility.
- Allowed environment variables to be properly expanded in study `description.name`.
- Tilde (~) now properly expands as part of a path in non-shell sections.
- The rediss cert_reqs keyword was changed to ssl_cert_reqs.

### Changed
- Updated tutorial redis version to 6.0.5.

## [1.6.2]

### Added
- The sample generation command now logs `stdout`, `stderr`, and `cmd.sh` to `merlin_info/`.
- 12 hidden test specs and associated cli tests, for cli tests with specs that we
  do not want in `merlin examples`.
- Inside `merlin_info/`, added provenance specs `<name>.orig.yaml`, `<name>.expanded.yaml`, and
  `<name>.partial.yaml` (identical to the original spec, but with expanded user variables).

### Fixed
- Updated to new celery (4.4.5) syntax for signature return codes.
- Corrected prior visibility timeout bugfix.
- Fixed and reactivated 3 cli tests.
- Added the `bank` and `walltime` keywords to the batch slurm launch, these
  will not alter the lsf launch.

### Changed
- Slightly improved logic by using regex to match variable tokens.
- Reduced instances of I/O, `MerlinStudy` logic is now in-memory to a greater extent.

## [1.6.1]

### Fixed
- Error if app.yaml does not have visibility timeout seconds.

## [1.6.0]

### Added
- The broker name can now be amqps (with ssl) or amqp (without ssl).
- The encryption key will now be created when running merlin config.
- The merlin info connection check will now enforce a minute timeout
  check for the server connections.

### Fixed
- Added a check for initial running workers when using merlin monitor to
  eliminate race condition.
- A bug that did not change the filename of the output workspace nor of the provenance spec
  when a user variable was included in the `description.name` field.
- Temporarily locked Celery version at 4.4.2 to avoid fatal bug.

### Changed
- The default rabbitmq vhost is now <user> instead of /<user>.
- Changed default visibility timeout from 2 hours to 24 hours. Exposed this in the config
  file.
- The monitor function will now check the queues to determine when
  to exit.

## [1.5.3]

### Fixed
- Temporarily locked maestro version to avoid fatal bug introduced by maestro v1.1.7.

## [1.5.2]

### Added
- A faq entry for --mpibind when using slurm on LC.
- Version of the openfoam workflow that works without docker.
- In 'merlin examples', a version of the openfoam workflow that works without docker.

### Fixed
- The batch system will now check LSB_MCPU_HOSTS to determine the number
  of nodes on blueos systems in case LSB_HOSTS is not present.
- A few typos, partially finished material, and developer comments in the tutorials.
- PEP-8 violations like unused imports, bad formatting, broken code.
- A bug where the batch shell was not overriding the default.

### Changed
- Removed mysql dependencies and added sqlalchemy to the celery module.
- Removed mysql install from travis.
- Improved the celery worker error messages.
- The slurm launch for celery workers no longer uses the --pty option,
  this can be added by setting launch_args in the batch section.
- Adjusted wording in openfoam_wf_no_docker example.

## [1.5.1]

### Added
- merlin example `null_spec`, which may be used to generate overhead data for merlin.

### Fixed
- The task creation bottleneck.
- Bug that caused the `cmd` stdout and stderr files of a step to be overwritten by that same step's `restart`
  section.

### Changed
- Updated tutorial docs.
- Relocated code that ran upon import from file body to functions. Added the respective
  function calls.

## [1.5.0]

### Added
- `HelpParser`, which automatically prints help messages when command line commands return an error.
- Optional ssl files for the broker and results backend config.
- A url keyword in the app.yaml file to override the entire broker or results backend configuration.
- The `all` option to `batch.nodes`.
- Auto zero-padding of sample directories, e.g. 00/00, 00/01 .. 10/10
- `$(MERLIN_STOP_WORKERS)` exit code that shuts down all workers
- The `merlin monitor` command, which blocks while celery workers are running.
  This can be used at the end of a batch submission script to keep the
  allocation alive while workers are present.  
- The ~/.merlin dir will be searched for the results password.
- A warning whenever an unrecognized key is found in a Merlin spec; this may
  help users find small mistakes due to spelling or indentation more quickly.

### Fixed
- Bug that prevented an empty username for results backend and broker when using redis.
- Bug that prevented `OUTPUT_PATH` from being an integer.
- Slow sample speed in `hello_samples.yaml` from the hello example.
- Bug that always had sample directory tree start with "0"
- "Error" message whenever a non-zero return code is given
- The explicit results password (when not a file) will be read if certs path is None and it will be stripped of any whitespace.
- Misleading log message for `merlin run-workers --echo`.
- A few seconds of lag that occurred in all merlin cli commands; merlin was always searching
  thru workflow examples, even when user's command had nothing to do with workflow examples.

### Changed
- Updated docs from `pip3 install merlinwf` to `pip3 install merlin`.
- Script launching uses Merlin submission instead of subclassing maestro submit
- `$(MERLIN_HARD_FAIL)` now shuts down only workers connected to the bad step's queue
- Updated all tutorial modules

## [1.4.1] [2020-03-06]

### Changed
- Updated various aspects of tutorial documentation.

## [1.4.0] 2020-03-02

### Added
- The walltime keyword is now enabled for the slurm and flux batch types.
- LAUNCHER keywords, (slurm,flux,lsf) for specifying arguments specific
  to that parallel launcher in the run section.
- Exception messages to `merlin info`.
- Preliminary tutorial modules for early testers.

### Removed
- The problematic step `stop_workers` in `feature_demo.yaml`.

### Fixed
- Syntax errors in web doc file `merlin_variables.rst`.

### Removed
- The exclusive and signal keywords and bind for slurm in a step. The bind
  keyword is now lsf only.

## [1.3.0] 2020-02-21

### Added
- cli test flag `--local`, which can be used in place of listing out the id of each
  local cli test.
- A Merlin Dockerfile and some accompanying web documentation.
- Makefile target `release`.
- The merlin config now takes an optional --broker argument, the
  value can be None, default rabbitmq broker, or redis for a redis
  local broker.
- Missing doc options for run and run-workers.
- Check server access when `merlin info` is run.
- A port option to rabbitmq config options.
- Author and author_email to setup.py.

### Removed
- Makefile targets `pull` and `update`.
- Unneeded variables from `simple_chain.yaml`.
- All `INFO`-level logger references to Celery.

### Changed
- Updated the Merlin Sphinx web docs, including info about command line commands.
- Example workflows use python3 instead of python.
- Updated `merlin info` to lookup python3 and and pip3.
- Altered user in Dockerfile and removed build tools.
- MANIFEST.in now uses recursive-include.
- Updated docker docs.
- `make clean` is more comprehensive, now cleans docs, build files, and release files.
- The celery keyword is no longer required in `app.yaml`.

## [1.2.3] 2020-01-27

### Changed
- Adjusted `maestrowf` requirement to `maestrowf>=1.1.7dev0`.

## [1.2.2] 2020-01-24

### Removed
- Unused directory `workflows/` at the top level (not to be confused with
  `merlin/examples/workflows/`)

### Fixed
- Bug related to missing package `merlin/examples/workflows` in PYPI distribution.

## [1.2.1] 2020-01-24

### Fixed
- Bug related to a missing path in `MANIFEST.in`.
- Error message when trying to run merlin without the app config file.

## [1.2.0] 2020-01-23

### Added
- `version_tests.sh`, for CI checking that the merlin version is incremented
  before changes are merged into master.
- Allow for the maestro `$(LAUNCHER)` syntax in tasks, this requires the
  nodes and procs variables in the task just as in maestro. The LAUNCHER keyword
  is implemented for flux, lsf, slurm and local types.  The lsf type
  will use the LLNL srun wrapper for jsrun when the lsf-srun batch type
  is used. The flux version will be checked to determine the proper format
  of the parallel launch call.
- Local CLI tests for the above `$(LAUNCHER)` feature.
- `machines` keyword, in the `merlin.resources.workers.<name>` section. This allows
  the user to assign workers (and thence, steps) to a given machine.
  All of the machines must have access to the `OUTPUT_PATH`, The
  steps list is mandatory for all workers. Once the machines are added, then only
  the workers for the given set of steps on the specific machine will start. The
  workers must be individually started on all of the listed machines separately by
  the user (`merlin run-workers`).
- New step field `restart`. This command runs when merlin receives a
  `$(MERLIN_RESTART)` exception. If no `restart` field is found, the `cmd`
  command re-runs instead.

### Fixed
- A bug in the `flux_test` example workflow.

### Changed
- Improved the `fix-style` dev Makefile target.
- Improved the `version` dev Makefile target.
- Updated travis logic.
- `MERLIN_RESTART` (which re-ran the `cmd` of a step) has been renamed to `MERLIN_RETRY`.


## [1.1.1] - 2020-01-09

### Added
- Makefile target `version` for devs to auto-increment the version.

## [1.1.0] - 2020-01-07

### Added
- Development dependencies install via pip: `pip install "merlinwf[dev]"`.
- `merlin status <yaml spec>` that returns queues, number of connected
  workers and number of unused tasks in each of those queues.
- `merlin example` cli command, which allows users to start running the
  examples immediately (even after pip-installing).

### Fixed
- `MANIFEST.in` fixes as required by Spack.
- `requirements.txt` just has release components, not dev deps.
- A bug related to the deprecated word 'unicode' in `openfilelist.py`.
- Broken Merlin logo image on PyPI summary page.
- Documentation typos.

### Changed
- Made `README.md` more concise and user-friendly.

### Removed
- Dependencies outside the requirements directory.
- LLNL-specific material in the Makefile.

### Deprecated
- `merlin-templates` cli command, in favor of `merlin example`.


## [1.0.5] - 2019-12-05

### Fixed
- Change the form of the maestrowf git requirement.


## [1.0.4] - 2019-12-04

### Added
- `requirements.txt` files and `scripts` directories for internal workflow examples.

### Fixed
- Added missing dependency `tabulate` to `release.txt`.

## [1.0.3] - 2019-12-04

### Added
Added the requirements files to the MANIFEST.in file for source
distributions.

## [1.0.2] - 2019-12-04
Negligible changes related to PyPI.

## [1.0.1] - 2019-12-04
Negligible changes related to PyPI.

## [1.0.0] - 2019-11-19
First public release. See the docs and merlin -h for details.
Here are some highlights.

### Added
- A changelog!
- Templated workflows generator. See `merlin-templates`
- Steps in any language. Set with study.step.run.shell in spec file. For instance:
```
    - name: python2_hello
      description: |
          do something in python2
      run:
          cmd: |
            print "OMG is this in python2?"
            print "Variable X2 is $(X2)"
          shell: /usr/bin/env python2
          task_queue: pyth2_hello
```
- Integration testing `make cli-tests`
- Style rules (isort and black). See `make check-style` and `make fix-style`
- Dry-run ability for workflows, which will cause workers to setup workspaces,
but skip execution (all variables will be expanded). Eg: `merlin run --local --dry-run`.
- Command line override of variable names. `merlin run --vars OUTPUT_PATH=/run/here/instead`

### Changed
- MerlinSpec class subclasses from Maestro

### Deprecated
- `merlin kill-workers`. Use `merlin stop-workers`

### Removed
- Dependencies on optional tools
- Unused fields in example workflows

### Fixed
- Multi-type samples (eg strings as well as floats)
- Single sample and single feature samples

### Security
- Auto-encryption of backend traffic
