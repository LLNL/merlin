# Changelog
All notable changes to Merlin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
  is implmented for flux, lsf, slurm and local types.  The lsf type
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
  examples immedately (even after pip-installing).

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
