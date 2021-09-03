# Changelog
All notable changes to Merlin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
