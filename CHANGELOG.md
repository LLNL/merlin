# Changelog
All notable changes to Merlin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
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
