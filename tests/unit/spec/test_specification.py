##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `specification.py` file of the `spec/` folder.
"""

import json
import logging
import os
import shlex
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

import pytest
import yaml
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.spec import all_keys
from merlin.spec.specification import MerlinSpec
from tests.fixture_types import FixtureCallable, FixtureStr


@pytest.fixture
def spec_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the MerlinSpec functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary output directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for MerlinSpec tests.
    """
    return create_testing_dir(temp_output_dir, "spec_testing")


@pytest.fixture
def spec() -> MerlinSpec:
    """
    Instantiates a MerlinSpec instance.

    Returns:
        A MerlinSpec instance for testing.
    """
    return MerlinSpec()


def test_yaml_sections_returns_expected_structure(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that the `yaml_sections` property returns the expected structure with correct section keys.

    This test verifies that `MerlinSpec.yaml_sections`:
    - Aggregates and renames internal attributes to the expected YAML-compatible format.
    - Maps `environment` to `env` and `globals` to `global.parameters`.
    - Returns a dictionary with all standard spec sections correctly named and populated.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mocker.patch.object(spec, "description", {"desc": "test"})
    mocker.patch.object(spec, "batch", {"type": "slurm"})
    mocker.patch.object(spec, "environment", {"VAR": "val"})
    mocker.patch.object(spec, "study", {"steps": []})
    mocker.patch.object(spec, "globals", {"FOO": "BAR"})
    mocker.patch.object(spec, "merlin", {"name": "spec"})
    mocker.patch.object(spec, "user", {"config": "custom"})

    expected = {
        "description": {"desc": "test"},
        "batch": {"type": "slurm"},
        "env": {"VAR": "val"},
        "study": {"steps": []},
        "global.parameters": {"FOO": "BAR"},
        "merlin": {"name": "spec"},
        "user": {"config": "custom"},
    }

    assert spec.yaml_sections == expected


def test_sections_returns_expected_structure(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that the `sections` property returns the internal spec dictionary using original attribute names.

    This test ensures `MerlinSpec.sections`:
    - Reflects the internal Python attribute names (e.g., `environment`, `globals`).
    - Does not rename keys (unlike `yaml_sections`).
    - Includes all standard sections with their assigned values.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mocker.patch.object(spec, "description", {"desc": "test"})
    mocker.patch.object(spec, "batch", {"type": "pbs"})
    mocker.patch.object(spec, "environment", {"ENV_VAR": "abc"})
    mocker.patch.object(spec, "study", {"steps": ["step1"]})
    mocker.patch.object(spec, "globals", {"alpha": 1})
    mocker.patch.object(spec, "merlin", {"project": "demo"})
    mocker.patch.object(spec, "user", {"profile": "dev"})

    expected = {
        "description": {"desc": "test"},
        "batch": {"type": "pbs"},
        "environment": {"ENV_VAR": "abc"},
        "study": {"steps": ["step1"]},
        "globals": {"alpha": 1},
        "merlin": {"project": "demo"},
        "user": {"profile": "dev"},
    }

    assert spec.sections == expected


def test_load_specification_success(mocker: MockerFixture, spec_testing_dir: FixtureStr):
    """
    Test that `load_specification` successfully loads a spec file and populates the expected fields.

    This test:
    - Creates a temporary YAML spec file with minimal valid content.
    - Mocks `MerlinSpec.load_spec_from_string` to return a dummy spec instance.
    - Asserts that `load_specification` returns the mocked object.
    - Verifies that the `path` and `specroot` fields are set correctly.
    - Confirms that `warn_unrecognized_keys` is called if `suppress_warning=False`.

    Args:
        mocker: PyTest mocker fixture.
        spec_testing_dir: The path to the temporary testing directory for MerlinSpec tests.
    """
    mock_spec = mocker.Mock(spec=MerlinSpec)
    mock_populate = mocker.patch.object(MerlinSpec, "load_spec_from_string", return_value=mock_spec)
    mock_warn = mocker.patch.object(mock_spec, "warn_unrecognized_keys")

    test_spec_file = os.path.join(spec_testing_dir, "test_load_specification_success.yaml")
    with open(test_spec_file, "w+") as test_spec:
        test_spec.write("description: {}\nmerlin: {}\nuser: {}\n")

    result = MerlinSpec.load_specification(test_spec_file, suppress_warning=False)

    assert result is mock_spec
    assert result.path == test_spec_file
    assert result.specroot == str(Path(test_spec_file).parent)
    mock_populate.assert_called_once()
    mock_warn.assert_called_once()


def test_load_specification_raises_exception_on_failure():
    """
    Test that `load_specification` raises an exception when given a non-existent file path.

    This test:
    - Calls the method with a path that does not exist.
    - Asserts that an exception is raised (likely FileNotFoundError or IOError).
    """
    bad_path = "/nonexistent/spec.yaml"

    with pytest.raises(Exception):
        MerlinSpec.load_specification(bad_path)


def test_load_spec_from_string_calls_expected_methods(mocker: MockerFixture):
    """
    Test that `load_spec_from_string` properly populates, verifies, and processes spec defaults.

    This test:
    - Mocks `_populate_spec` to return a dummy `MerlinSpec` object.
    - Patches `verify` and `process_spec_defaults` methods to track calls.
    - Sets up `yaml_sections` with `walltime` values that should be converted to HH:MM:SS format.
    - Asserts that each relevant method is called once.
    - Confirms that walltime values in the study and merlin blocks are formatted correctly.

    Args:
        mocker: PyTest mocker fixture.
    """
    fake_spec = mocker.Mock(spec=MerlinSpec)

    # These are the mutable sections that will be mutated inside the function
    study_section = [{"walltime": 3600}]
    merlin_section = {"walltime": 7200}

    # Patch methods
    mock_populate = mocker.patch.object(MerlinSpec, "_populate_spec", return_value=fake_spec)
    mocker.patch.object(fake_spec, "verify")
    mocker.patch.object(fake_spec, "process_spec_defaults")

    # Return those actual objects from yaml_sections so the function mutates them
    fake_spec.yaml_sections = {
        "study": study_section,
        "merlin": merlin_section,
    }

    # Set these so we can check them after mutation
    fake_spec.study = study_section
    fake_spec.merlin = merlin_section

    result = MerlinSpec.load_spec_from_string("fake_yaml", needs_IO=True, needs_verification=True)

    assert result is fake_spec
    mock_populate.assert_called_once()
    fake_spec.process_spec_defaults.assert_called_once()
    fake_spec.verify.assert_called_once()

    assert fake_spec.study[0]["walltime"] == "01:00:00"
    assert fake_spec.merlin["walltime"] == "02:00:00"


def test_populate_spec_loads_expected_fields(mocker: MockerFixture):
    """
    Test that `_populate_spec` correctly loads all top-level fields from the YAML input stream.

    This test:
    - Simulates a YAML specification with all main sections: `description`, `env`, `batch`, `study`,
      `global.parameters`, `merlin`, and `user`.
    - Mocks `load_merlin_block` and `load_user_block` to isolate the test from external I/O.
    - Asserts that each field is correctly parsed into the `MerlinSpec` object.

    Args:
        mocker: PyTest mocker fixture.
    """
    yaml_content = """
description:
  name: test
env:
  variables:
    FOO: BAR
batch:
  type: slurm
study:
  - name: step1
    run: echo hello
global.parameters:
  param1: value1
merlin:
  resources: {}
user:
  notes: hello
"""
    stream = StringIO(yaml_content)

    # Patch merlin/user block loaders
    mocker.patch.object(MerlinSpec, "load_merlin_block", return_value={"resources": {}})
    mocker.patch.object(MerlinSpec, "load_user_block", return_value={"notes": "hello"})

    spec = MerlinSpec._populate_spec(stream)

    assert spec.description == {"name": "test"}
    assert spec.environment == {"variables": {"FOO": "BAR"}}
    assert spec.batch == {"type": "slurm"}
    assert spec.study == [{"name": "step1", "run": "echo hello"}]
    assert spec.globals == {"param1": "value1"}
    assert spec.merlin == {"resources": {}}
    assert spec.user == {"notes": "hello"}


def test_get_study_step_names_returns_correct_names(spec: MerlinSpec):
    """
    Test that `get_study_step_names` returns a list of step names in the order defined.

    This test:
    - Sets up a study section with multiple steps, each with a name and run command.
    - Asserts that the method returns a list of step names in sequence.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.study = [
        {"name": "step1", "run": "echo 1"},
        {"name": "step2", "run": "echo 2"},
        {"name": "step3", "run": "echo 3"},
    ]

    result = spec.get_study_step_names()

    assert result == ["step1", "step2", "step3"]


def test_get_study_step_names_empty(spec: MerlinSpec):
    """
    Test that `get_study_step_names` returns an empty list when no study steps are defined.

    This test:
    - Assigns an empty list to `spec.study`.
    - Asserts that the method returns an empty list.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.study = []

    result = spec.get_study_step_names()

    assert result == []


def test_verify_loads_schema_and_calls_all_verification_methods(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that `verify` loads the JSON schema and calls all section-specific verification methods.

    This test:
    - Mocks file operations to simulate loading a schema from disk.
    - Verifies that the correct schema sections are passed to each corresponding
      `verify_*` method on the `MerlinSpec` instance.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """

    # Mock schema content
    fake_schema = {
        "DESCRIPTION": {"desc": "schema"},
        "ENV": {"env": "schema"},
        "STUDY_STEP": {"study": "schema"},
        "PARAM": {"param": "schema"},
        "MERLIN": {"merlin": "schema"},
        "BATCH": {"batch": "schema"},
    }

    # Patch built-in open and json.load
    mocker.patch("os.path.abspath", return_value="/fake/path/specification.py")
    mocker.patch("os.path.dirname", return_value="/fake/path")
    mocker.patch("builtins.open", mocker.mock_open(read_data=json.dumps(fake_schema)))
    mocker.patch("json.load", return_value=fake_schema)

    # Patch all verification methods called inside verify
    mock_verify_description = mocker.patch.object(spec, "verify_description")
    mock_verify_environment = mocker.patch.object(spec, "verify_environment")
    mock_verify_study = mocker.patch.object(spec, "verify_study")
    mock_verify_parameters = mocker.patch.object(spec, "verify_parameters")
    mock_verify_merlin = mocker.patch.object(spec, "verify_merlin_block")
    mock_verify_batch = mocker.patch.object(spec, "verify_batch_block")

    spec.verify()

    mock_verify_description.assert_called_once_with(fake_schema["DESCRIPTION"])
    mock_verify_environment.assert_called_once_with(fake_schema["ENV"])
    mock_verify_study.assert_called_once_with(fake_schema["STUDY_STEP"])
    mock_verify_parameters.assert_called_once_with(fake_schema["PARAM"])
    mock_verify_merlin.assert_called_once_with(fake_schema["MERLIN"])
    mock_verify_batch.assert_called_once_with(fake_schema["BATCH"])


def test_verify_workers_with_valid_steps(spec: MerlinSpec):
    """
    Test that `_verify_workers` completes successfully when all assigned worker steps are valid.

    This test:
    - Defines a valid study with two steps.
    - Assigns workers to valid step names and "all".
    - Ensures `_verify_workers` does not raise any exceptions.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.study = [{"name": "prep"}, {"name": "run"}]
    spec.merlin = {
        "resources": {
            "workers": {
                "worker1": {"steps": ["prep", "run"]},
                "worker2": {"steps": ["all"]},
            }
        }
    }

    # Should not raise
    spec._verify_workers()


def test_verify_workers_with_invalid_step_raises(spec: MerlinSpec):
    """
    Test that `_verify_workers` raises a ValueError when a worker references an undefined step.

    This test:
    - Sets up a study with only one valid step ("prep").
    - Assigns a worker to "prep" and an invalid step ("train").
    - Asserts that a ValueError is raised with an appropriate message.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.study = [{"name": "prep"}]  # Only "prep" is valid
    spec.merlin = {
        "resources": {
            "workers": {
                "worker1": {"steps": ["prep", "train"]},  # "train" is invalid
            }
        }
    }

    with pytest.raises(ValueError, match="Step with the name train"):
        spec._verify_workers()


def test_verify_merlin_block_validates_and_verifies_workers(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that `verify_merlin_block` calls schema validation and worker verification.

    This test verifies that:
    - The 'merlin' block is passed to the schema validator.
    - The `_verify_workers` method is invoked to check worker configuration.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {"resources": {"workers": {}}}
    mock_schema = {"type": "object"}

    validate_mock = mocker.patch("merlin.spec.specification.YAMLSpecification.validate_schema")
    verify_workers_mock = mocker.patch.object(spec, "_verify_workers")

    spec.verify_merlin_block(mock_schema)

    validate_mock.assert_called_once_with("merlin", spec.merlin, mock_schema)
    verify_workers_mock.assert_called_once()


def test_verify_batch_block_with_lsf_walltime_logs_warning(mocker: MockerFixture, caplog: CaptureFixture, spec: MerlinSpec):
    """
    Test that a warning is logged when 'walltime' is used with LSF in the batch block.

    This test confirms that:
    - The batch block is validated against the schema.
    - A warning is logged when 'walltime' is set but not supported by the 'lsf' batch type.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
        spec: A MerlinSpec instance for testing.
    """
    caplog.set_level(logging.WARNING)

    spec.batch = {"type": "lsf", "walltime": "01:00:00"}
    mock_schema = {"type": "object"}

    validate_mock = mocker.patch("merlin.spec.specification.YAMLSpecification.validate_schema")

    spec.verify_batch_block(mock_schema)

    validate_mock.assert_called_once_with("batch", spec.batch, mock_schema)
    assert "The walltime argument is not available in lsf." in caplog.text


def test_verify_batch_block_with_slurm_skips_walltime_check(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that no warning is logged when 'walltime' is used with SLURM in the batch block.

    This test ensures that:
    - The batch block is validated successfully for SLURM.
    - No warning is logged, since SLURM supports the 'walltime' parameter.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {"type": "slurm", "walltime": "02:00:00"}
    mock_schema = {"type": "object"}

    validate_mock = mocker.patch("merlin.spec.specification.YAMLSpecification.validate_schema")
    log_mock = mocker.patch("merlin.spec.specification.LOG")

    spec.verify_batch_block(mock_schema)

    validate_mock.assert_called_once_with("batch", spec.batch, mock_schema)
    log_mock.warning.assert_not_called()


def test_verify_batch_block_with_placeholder_values(spec: MerlinSpec):
    """
    Tests that the `verify_batch_block` method works when placeholder values are provided in
    the batch block.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {"type": "slurm", "queue": None, "bank": ""}

    # Load the MerlinSpec schema file
    dir_path = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(dir_path, "..", "..", "..", "merlin", "spec", "merlinspec.json")
    with open(schema_path, "r") as json_file:
        schema = json.load(json_file)

    spec.verify_batch_block(schema)


def test_load_merlin_block_with_section_present():
    """
    Test that `load_merlin_block` correctly extracts the 'merlin' section from a YAML stream.

    This test ensures that when a valid 'merlin' block is present in the input YAML,
    it is parsed and returned as a dictionary with expected nested structure.
    """
    yaml_content = """
    merlin:
      resources:
        workers:
          worker1:
            steps: ["step1"]
    """
    stream = StringIO(yaml_content)
    result = MerlinSpec.load_merlin_block(stream)

    assert result == {"resources": {"workers": {"worker1": {"steps": ["step1"]}}}}


def test_load_merlin_block_missing_section_logs_warning(caplog: CaptureFixture):
    """
    Test that `load_merlin_block` logs a warning and returns an empty dict when the section is missing.

    This test verifies that:
    - When no 'merlin' section is found in the YAML stream, the method logs a warning.
    - An empty dictionary is returned.

    Args:
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.WARNING)

    yaml_content = """
    study:
      - name: step1
        run: echo hello
    """
    stream = StringIO(yaml_content)

    result = MerlinSpec.load_merlin_block(stream)

    assert result == {}
    assert "Workflow specification missing" in caplog.text


def test_load_user_block_with_section_present():
    """
    Test that `load_user_block` returns a dictionary of user metadata when the section is present.

    This test confirms that the method correctly parses and returns the contents of the
    'user' section from a valid YAML input stream.
    """
    yaml_content = """
    user:
      author: me
      contact: me@example.com
    """
    stream = StringIO(yaml_content)
    result = MerlinSpec.load_user_block(stream)

    assert result == {"author": "me", "contact": "me@example.com"}


def test_load_user_block_missing_section_returns_empty():
    """
    Test that `load_user_block` returns an empty dictionary if the 'user' section is missing.

    This ensures that the method fails gracefully (without error) when the YAML input
    does not include the 'user' block.
    """
    yaml_content = """
    description:
      name: test_study
    """
    stream = StringIO(yaml_content)
    result = MerlinSpec.load_user_block(stream)

    assert result == {}


def test_process_spec_defaults_fills_missing_sections(spec: MerlinSpec):
    """
    Test that `process_spec_defaults` populates missing configuration sections with defaults.

    This test verifies that:
    - The `batch` section is populated with default `type` and `shell` values if empty.
    - The `environment` section is given a default `variables` key if not present.
    - Study steps receive default fields in the `run` section, including `shell`, `task_queue`, and `max_retries`.
    - If no workers are defined in `resources`, a `default_worker` is created and assigned to all steps.

    The test ensures proper fallback logic and default injection for minimal specs.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {}
    spec.environment = {}
    spec.globals = {}
    spec.study = [{"name": "step1", "run": {"cmd": "echo hello"}}]
    spec.merlin = {"resources": {"task_server": "celery", "overlap": False, "workers": None}, "samples": None}

    spec.process_spec_defaults()

    # Batch and env should have default values filled in
    assert spec.batch["type"] == "local"
    assert spec.batch["shell"] == "/bin/bash"
    assert "variables" in spec.environment

    # Study step should have defaults like shell, task_queue
    step_run = spec.study[0]["run"]
    assert "shell" in step_run
    assert step_run["shell"] == "/bin/bash"
    assert step_run["task_queue"] == "merlin"
    assert step_run["max_retries"] == 30

    # Workers should be set to default_worker
    assert "default_worker" in spec.merlin["resources"]["workers"]
    assert spec.merlin["resources"]["workers"]["default_worker"]["steps"] == ["all"]


def test_process_spec_defaults_adds_vlauncher_vars(spec: MerlinSpec):
    """
    Test that `process_spec_defaults` inserts Merlin environment variables into commands using $(VLAUNCHER).

    This test ensures that:
    - When a study step's `cmd` includes the `$(VLAUNCHER)` placeholder,
      the command string is updated to include `MERLIN_NODES` and `MERLIN_PROCS` exports.
    - These variables are based on the step's `nodes` value and a default `procs` value (if unspecified).
    - The original command remains intact and is not overwritten.

    This test is specific to vlauncher workflows and related environment handling.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {"shell": "/bin/bash"}
    spec.environment = {}
    spec.globals = {}
    spec.study = [{"name": "step1", "run": {"cmd": "$(VLAUNCHER)\necho run", "nodes": 4}}]
    spec.merlin = {"resources": {"task_server": "celery", "overlap": False, "workers": None}, "samples": None}

    spec.process_spec_defaults()

    run_cmd = spec.study[0]["run"]["cmd"]
    assert "MERLIN_NODES=4" in run_cmd
    assert "MERLIN_PROCS=1" in run_cmd
    assert "$(VLAUNCHER)" in run_cmd  # Ensure original content preserved


def test_process_spec_defaults_assigns_unassigned_steps_to_default_worker(spec: MerlinSpec):
    """
    Test that `process_spec_defaults` assigns unclaimed steps to a default worker.

    This test verifies that:
    - If some steps are not explicitly claimed by any defined worker,
      a `default_worker` is created in `resources.workers`.
    - The unassigned steps are added to the `default_worker`'s step list.
    - Steps already claimed by other workers are left unchanged.

    The test uses a partial worker assignment to validate the fallback mechanism.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {"shell": "/bin/bash"}
    spec.environment = {}
    spec.globals = {}
    spec.study = [
        {"name": "prep", "run": {"cmd": "echo prep"}},
        {"name": "run", "run": {"cmd": "echo run"}},
        {"name": "analyze", "run": {"cmd": "echo analyze"}},
    ]
    spec.merlin = {
        "resources": {"task_server": "celery", "overlap": False, "workers": {"worker1": {"steps": ["prep"]}}},
        "samples": None,
    }

    spec.process_spec_defaults()

    workers = spec.merlin["resources"]["workers"]
    assert "worker1" in workers
    assert "default_worker" in workers
    assert sorted(workers["default_worker"]["steps"]) == ["analyze", "run"]


def test_process_spec_defaults_does_not_add_default_worker_if_all_steps_covered(spec: MerlinSpec):
    """
    Test that `process_spec_defaults` does not add a default worker when all steps are explicitly assigned.

    This test verifies that:
        - If every step in the `study` is already covered by an existing worker's `steps` list,
          then no `"default_worker"` is injected into the `merlin.resources.workers` dictionary.

    The test sets up two study steps (`prep`, `run`) and assigns them to separate workers.
    After calling `process_spec_defaults`, the result should not contain a default worker.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {"shell": "/bin/bash"}
    spec.environment = {}
    spec.globals = {}
    spec.study = [
        {"name": "prep", "run": {"cmd": "echo prep"}},
        {"name": "run", "run": {"cmd": "echo run"}},
    ]
    spec.merlin = {
        "resources": {
            "task_server": "celery",
            "overlap": False,
            "workers": {"worker1": {"steps": ["prep"]}, "worker2": {"steps": ["run"]}},
        },
        "samples": None,
    }

    spec.process_spec_defaults()

    workers = spec.merlin["resources"]["workers"]
    assert "default_worker" not in workers


def test_process_spec_defaults_fills_missing_sample_fields(spec: MerlinSpec):
    """
    Test that `process_spec_defaults` fills in missing sample generation fields with defaults.

    This test verifies that:
        - When the `samples.generate` section exists but lacks required fields such as `"cmd"`,
          the method populates them with default values.
        - It also ensures that `"level_max_dirs"` is added to `samples` if missing.

    The test initializes a minimal spec and confirms that `process_spec_defaults` injects the
    necessary default configuration values into the `samples` section.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.batch = {"shell": "/bin/bash"}
    spec.environment = {}
    spec.globals = {}
    spec.study = [{"name": "step1", "run": {"cmd": "echo"}}]
    spec.merlin = {"resources": {"task_server": "celery", "overlap": False, "workers": None}, "samples": {"generate": {}}}

    spec.process_spec_defaults()

    assert "cmd" in spec.merlin["samples"]["generate"]
    assert "level_max_dirs" in spec.merlin["samples"]


def test_fill_missing_defaults_adds_missing_keys():
    """
    Test that `fill_missing_defaults` adds missing keys from the defaults to the target dict.

    This test verifies that:
        - Existing keys in the target are preserved.
        - Keys present in the defaults but missing from the target are added.
        - The resulting dict contains all keys from the union of both inputs, without overwriting
          target values.
    """
    target = {"a": 1}
    defaults = {"a": 1, "b": 2, "c": 3}
    MerlinSpec.fill_missing_defaults(target, defaults)

    assert target == {"a": 1, "b": 2, "c": 3}


def test_fill_missing_defaults_replaces_none_values():
    """
    Test that `fill_missing_defaults` replaces `None` values in the target dict.

    This test ensures that:
        - Keys in the target dict with `None` values are replaced with corresponding
          values from the defaults dict.
        - Keys with valid values in the target dict remain unchanged.
        - Missing keys from the target dict are added from the defaults.
    """
    target = {"a": None, "b": 5}
    defaults = {"a": 10, "b": 99, "c": 42}
    MerlinSpec.fill_missing_defaults(target, defaults)

    assert target == {"a": 10, "b": 5, "c": 42}


def test_fill_missing_defaults_does_not_overwrite_existing():
    """
    Test that `fill_missing_defaults` does not overwrite existing non-None values.

    This test ensures that:
        - Keys already defined in the target dict (with non-None values) are not modified.
        - New keys present only in the defaults are added to the target.
    """
    target = {"a": 1, "b": 2}
    defaults = {"a": 99, "b": 100, "c": 3}
    MerlinSpec.fill_missing_defaults(target, defaults)

    assert target == {"a": 1, "b": 2, "c": 3}


def test_fill_missing_defaults_handles_nested_dicts():
    """
    Test that `fill_missing_defaults` correctly merges nested dictionaries.

    This test ensures that:
        - `None` values inside nested dicts are replaced by default values.
        - Existing non-None nested values are preserved.
        - Entirely missing keys inside nested dicts are added from the defaults.
    """
    target = {"outer": {"inner1": None, "inner2": "keep"}}
    defaults = {"outer": {"inner1": "default1", "inner2": "default2", "inner3": "default3"}}

    MerlinSpec.fill_missing_defaults(target, defaults)

    assert target == {"outer": {"inner1": "default1", "inner2": "keep", "inner3": "default3"}}


def test_fill_missing_defaults_skips_non_dicts():
    """
    Test that `fill_missing_defaults` handles mismatched types gracefully.

    This test ensures that:
        - If a key exists in both target and defaults but the values are not dictionaries,
          the default value is applied only if the key is missing from the target.
        - It does not attempt recursive merging on non-dict types.
    """
    target = {"a": 1}
    defaults = {"a": 1, "b": "not_a_dict"}
    MerlinSpec.fill_missing_defaults(target, defaults)

    assert target == {"a": 1, "b": "not_a_dict"}


def test_check_section_logs_warning_for_unrecognized_keys(caplog: CaptureFixture):
    """
    Test that `check_section` logs warnings for keys not in the set of known keys.

    This test provides:
        - A sample spec section containing one recognized key (`valid`) and two extra keys.
        - A set of known keys that includes only `"valid"`.

    It verifies that:
        - A warning is logged for each unrecognized key using the expected message format.

    Args:
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.WARNING)
    section = {"valid": 1, "extra1": "x", "extra2": "y"}
    known_keys = {"valid"}

    MerlinSpec.check_section("example.section", section, known_keys)

    assert "Unrecognized key 'extra1' found in spec section 'example.section'." in caplog.text
    assert "Unrecognized key 'extra2' found in spec section 'example.section'." in caplog.text


def test_warn_unrecognized_keys_logs_expected_warnings(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `warn_unrecognized_keys` to ensure it logs warnings for all unknown keys in the spec.

    This test:
        - Sets up a `MerlinSpec` object with a variety of unrecognized keys across sections
          including description, batch, environment, globals, study, merlin, workers, and samples.
        - Mocks the known key sets in `all_keys` to define what is considered valid.
        - Patches the logger to capture all warning calls.

    It verifies that:
        - A warning is logged for every unrecognized key.
        - The log messages contain the correct section context and key name.
        - The total number of warnings matches the number of unexpected keys defined (9).

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    # Patch internal data to simulate a loaded spec
    spec.description = {"name": "test", "unexpected": "extra"}  # 1 unrecognized
    spec.batch = {"type": "local", "unexpected_batch_key": True}  # 1 unrecognized
    spec.environment = {"variables": {}, "unexpected_env": 42}  # 1 unrecognized
    spec.globals = {"group1": {"values": [1], "unexpected_param": "oops"}}  # 1 unrecognized
    spec.study = [
        {
            "name": "step1",
            "run": {"cmd": "echo hi", "unexpected_run_key": True},  # 1 unrecognized
            "unexpected_step_key": 123,  # 1 unrecognized
        }
    ]
    spec.merlin = {
        "resources": {"workers": {"workerA": {"nodes": 1, "unexpected_worker_key": "oops"}}},  # 1 unrecognized
        "samples": {"unexpected_sample_key": "bad"},  # 1 unrecognized
        "unexpected_merlin_key": "??",  # 1 unrecognized
    }

    mock_log = mocker.patch("merlin.spec.specification.LOG")

    # Patch known keys to simulate validation logic
    all_keys.DESCRIPTION = {"name"}
    all_keys.BATCH = {"type"}
    all_keys.ENV = {"variables"}
    all_keys.PARAMETER = {"values"}
    all_keys.STUDY_STEP = {"name", "run"}
    all_keys.STUDY_STEP_RUN = {"cmd"}
    all_keys.MERLIN = {"resources", "samples"}
    all_keys.MERLIN_RESOURCES = {"workers"}
    all_keys.WORKER = {"nodes"}
    all_keys.SAMPLES = set()

    spec.warn_unrecognized_keys()

    # Gather all warning messages
    messages = [call.args[0] for call in mock_log.warning.call_args_list]

    # Expect 9 warnings for 9 unrecognized keys
    assert len(messages) == 9

    expected_fragments = [
        "Unrecognized key 'unexpected' found in spec section 'description'",
        "Unrecognized key 'unexpected_batch_key' found in spec section 'batch'",
        "Unrecognized key 'unexpected_env' found in spec section 'env'",
        "Unrecognized key 'unexpected_param' found in spec section 'global.parameters'",
        "Unrecognized key 'unexpected_step_key' found in spec section 'step1'",
        "Unrecognized key 'unexpected_run_key' found in spec section 'step1.run'",
        "Unrecognized key 'unexpected_merlin_key' found in spec section 'merlin'",
        "Unrecognized key 'unexpected_worker_key' found in spec section 'merlin.resources.workers workerA'",
        "Unrecognized key 'unexpected_sample_key' found in spec section 'merlin.samples'",
    ]

    for expected in expected_fragments:
        assert any(expected in msg for msg in messages)


def test_dump_returns_valid_yaml(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that `dump` returns a valid YAML string representing the spec.

    This test:
        - Mocks the `yaml_sections` property to return a basic spec structure.
        - Mocks `_dict_to_yaml` to return a static YAML string.
        - Calls `dump()` and verifies that:
            - The return value is a string.
            - The string can be parsed as valid YAML.
            - Specific expected keys and values appear in the parsed result.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_yaml_sections = {
        "description": {"name": "Test"},
        "batch": {"type": "local"},
        "env": {},
        "study": [],
        "global.parameters": {},
        "merlin": {},
        "user": {},
    }
    mocker.patch.object(MerlinSpec, "yaml_sections", new_callable=mocker.PropertyMock).return_value = mock_yaml_sections
    mock_dict_to_yaml = mocker.patch.object(
        spec, "_dict_to_yaml", return_value="description:\n  name: Test\nbatch:\n  type: local\n"
    )

    result = spec.dump()

    assert isinstance(result, str)
    yaml_dict = yaml.safe_load(result)
    assert "description" in yaml_dict
    assert yaml_dict["description"]["name"] == "Test"
    mock_dict_to_yaml.assert_called_once()


def test_dump_raises_value_error_for_invalid_yaml(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test that `dump` raises a `ValueError` when it produces invalid YAML output.

    This test:
        - Mocks the `yaml_sections` to include an unserializable object.
        - Mocks `_dict_to_yaml` to return an invalid YAML string.
        - Expects that `dump()` will raise a `ValueError` with a specific error message.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch.object(MerlinSpec, "yaml_sections", new_callable=mocker.PropertyMock).return_value = {
        "bad": {"data": object()}
    }  # unserializable
    mocker.patch.object(spec, "_dict_to_yaml", return_value="bad:\n  data: !!python/object:unserializable")

    with pytest.raises(ValueError, match="Error parsing provenance spec"):
        spec.dump()


@pytest.mark.parametrize(
    "obj, key_stack, expected_output",
    [
        ("simple string", ["root"], "simple string"),  # Test string
        (True, ["root"], "true"),  # Test bool
        (False, ["root"], "false"),  # Test bool
        (None, ["root"], ""),  # Test None
        ({"key1": "value1", "key2": "value2"}, ["root"], "key1: value1\nkey2: value2"),  # Test dict
        (["x", "y", "z"], ["sources"], "- x\n- y\n- z"),  # Test list
    ],
)
def test_dict_to_yaml_parametrized(obj: Any, key_stack: List[str], expected_output: str, spec: MerlinSpec):
    """
    Parametrized test for `_dict_to_yaml`, covering various input types.

    This test runs multiple scenarios using different object types to ensure
    the `_dict_to_yaml` method generates the expected YAML formatting. It includes:
        - Simple strings
        - Booleans
        - None values
        - Dictionaries with string keys/values
        - Lists under specific key contexts

    Each case compares the produced YAML against the expected output line-by-line.

    Args:
        obj: The Python object to serialize (string, bool, None, dict, or list).
        key_stack: A list of keys indicating the YAML hierarchy context.
        expected_output: The expected YAML string output for the given input.
    """
    tab = "  "  # two-space indent
    result = spec._dict_to_yaml(obj, "", key_stack, tab)

    # Normalize whitespace for test comparison
    for line in expected_output.strip().splitlines():
        assert line in result


def test_process_string_single_line(spec: MerlinSpec):
    """
    Test `_process_string` with a single-line string input.

    This test passes a basic one-line string with no newline characters and
    verifies that the string is returned unchanged, regardless of indentation settings.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    result = spec._process_string("hello world", lvl=0, tab="  ")
    assert result == "hello world"


def test_process_string_multiline(spec: MerlinSpec):
    """
    Test `_process_string` with a multi-line string input.

    This test simulates a string containing newlines and checks that:
        - The output is YAML-compatible.
        - The string is prefixed with a `|` to indicate a block scalar.
        - Each line is properly indented according to the provided tab.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    input_string = "line 1\nline 2\nline 3"
    expected = "|\n  line 1\n  line 2\n  line 3"  # assuming tab=2
    result = spec._process_string(input_string, lvl=0, tab="  ")
    assert result == expected


def test_process_list_with_hyphens(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `_process_list` formatting when the list should be expanded with hyphens.

    This test simulates a list of two elements under a key context like `"study"`,
    which triggers block-style list formatting (YAML-style).

    It verifies that:
        - Each element is rendered as a separate line prefixed with `-`.
        - Proper indentation is applied.
        - `_dict_to_yaml` is used to format each list element.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_yaml = mocker.patch.object(MerlinSpec, "_dict_to_yaml")
    mock_yaml.side_effect = lambda e, s, k, t: f"item_{e}"
    obj = ["a", "b"]
    string = ""
    key_stack = ["study"]
    result = spec._process_list(obj, string, key_stack, lvl=0, tab="  ")

    # Should produce a YAML-style list with hyphens and indentation
    expected = "\n  - item_a\n  - item_b\n"
    assert result == expected


def test_process_list_inline_array(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `_process_list` formatting when the list should be rendered inline.

    This test simulates a list under a context like `"globals"`, which triggers
    inline formatting (YAML inline array).

    It verifies that:
        - The entire list is returned as a single line in square brackets.
        - Each element is processed via `_dict_to_yaml`.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_yaml = mocker.patch.object(MerlinSpec, "_dict_to_yaml")
    mock_yaml.side_effect = lambda e, s, k, t: f"item_{e}"
    obj = [1, 2]
    string = ""
    key_stack = ["globals"]
    result = spec._process_list(obj, string, key_stack, lvl=0, tab="  ")

    # Inline list format (e.g., [item_1, item_2])
    expected = "[item_1, item_2]"
    assert result == expected


def test_process_dict_nested(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `_process_dict` formatting when called from a non-list context.

    This test:
        - Mocks `_dict_to_yaml` to return a dummy formatted string.
        - Simulates a dictionary with two keys.
        - Uses a `key_stack` that does not end in "elem", indicating a standard nested context.

    It verifies that:
        - The result includes indented lines for each key-value pair.
        - Each line is prefixed with the correct indentation and key.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_yaml = mocker.patch.object(MerlinSpec, "_dict_to_yaml")
    mock_yaml.side_effect = lambda v, s, k, t: f"value_{v}"

    obj = {"key1": "v1", "key2": "v2"}
    string = ""
    key_stack = ["not_elem"]
    result = spec._process_dict(obj, string, key_stack, lvl=0, tab="  ")

    expected = "\n  key1: value_v1\n" "  key2: value_v2\n"
    assert result == expected


def test_process_dict_inside_list_elem(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `_process_dict` formatting when called from inside a list element (`elem`).

    This test:
        - Mocks `_dict_to_yaml` to return a dummy formatted string.
        - Provides a dictionary to simulate a list element context.
        - Uses a `key_stack` ending in "elem" to trigger list formatting logic.

    It verifies that:
        - The result flattens key-value pairs without indentation or nesting syntax.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_yaml = mocker.patch.object(MerlinSpec, "_dict_to_yaml")
    mock_yaml.side_effect = lambda v, s, k, t: f"value_{v}"

    obj = {"k": "v"}
    string = ""
    key_stack = ["elem"]
    result = spec._process_dict(obj, string, key_stack, lvl=1, tab="  ")

    expected = "k: value_v\n"
    assert result == expected


def test_get_step_worker_map_all_steps(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_step_worker_map` when all workers are assigned to all steps.

    This test:
        - Configures two workers with `steps: ["all"]`.
        - Defines two study steps: "stepA" and "stepB".

    It verifies that:
        - Both workers are assigned to all steps in the result map.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {
        "resources": {
            "workers": {
                "worker1": {"steps": ["all"]},
                "worker2": {"steps": ["all"]},
            }
        }
    }
    mocker.patch.object(spec, "get_study_step_names", return_value=["stepA", "stepB"])

    result = spec.get_step_worker_map()

    assert result == {
        "stepA": ["worker1", "worker2"],
        "stepB": ["worker1", "worker2"],
    }


def test_get_step_worker_map_mixed_steps(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_step_worker_map` with a mix of "all" and step-specific worker assignments.

    This test:
        - Configures three workers:
            - `worker1` is assigned to all steps.
            - `worker2` to "stepA" only.
            - `worker3` to "stepB" only.
        - Defines three study steps: "stepA", "stepB", "stepC".

    It verifies that:
        - Step-specific assignments are honored.
        - Workers with "all" access appear on every step.
        - The result map lists correct workers per step.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {
        "resources": {
            "workers": {
                "worker1": {"steps": ["all"]},
                "worker2": {"steps": ["stepA"]},
                "worker3": {"steps": ["stepB"]},
            }
        }
    }

    mocker.patch.object(spec, "get_study_step_names", return_value=["stepA", "stepB", "stepC"])
    result = spec.get_step_worker_map()

    assert result == {
        "stepA": ["worker1", "worker2"],
        "stepB": ["worker1", "worker3"],
        "stepC": ["worker1"],
    }


def test_get_worker_step_map_all_and_specific(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_worker_step_map` with a combination of workers assigned to "all" and specific steps.

    This test configures:
        - `worker1` assigned to `"all"` steps.
        - `worker2` assigned to a subset of steps.
        - Available study steps: stepA, stepB, stepC.

    It verifies that:
        - `worker1` is correctly mapped to all study steps.
        - `worker2` is only mapped to the specified subset.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {
        "resources": {
            "workers": {
                "worker1": {"steps": ["all"]},
                "worker2": {"steps": ["stepA", "stepC"]},
            }
        }
    }

    mocker.patch.object(spec, "get_study_step_names", return_value=["stepA", "stepB", "stepC"])
    result = spec.get_worker_step_map()

    assert result == {
        "worker1": ["stepA", "stepB", "stepC"],
        "worker2": ["stepA", "stepC"],
    }


def test_get_worker_step_map_empty_workers(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_worker_step_map` when no workers are defined in the spec.

    This test sets:
        - An empty `workers` dictionary in the Merlin configuration.
        - A single study step to ensure the system is initialized.

    It verifies that:
        - The resulting worker-step map is an empty dictionary.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {"resources": {"workers": {}}}

    mocker.patch.object(spec, "get_study_step_names", return_value=["step1"])
    result = spec.get_worker_step_map()

    assert result == {}


def make_step(name: str, task_queue: str = None):
    """
    Create a mock step object for use in tests.

    This helper builds a `SimpleNamespace` representing a study step,
    with an optional `task_queue` field inside the `run` section.

    Args:
        name: The name of the step.
        spec: Optional name of the task queue assigned to the step.

    Returns:
        A `SimpleNamespace` object representing a `Step`.
    """
    run = {"task_queue": task_queue} if task_queue else {}
    step = SimpleNamespace(name=name, run=run)
    return step


def test_get_task_queues_with_and_without_tag(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_task_queues` with configurable prefix and override via `omit_tag`.

    This test sets:
        - Queue tag in config as "[merlin]_".
        - Steps with and without defined task queues.

    It validates that:
        - Prefix is added when `omit_tag=False` and config allows tagging.
        - Prefix is omitted when `omit_tag=True`.
        - Prefix is omitted when config disables tagging globally (`omit_queue_tag=True`).

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_config = SimpleNamespace(celery=SimpleNamespace(omit_queue_tag=False, queue_tag="[merlin]_"))
    mocker.patch("merlin.config.configfile.CONFIG", mock_config)

    steps = [
        make_step("step1", "queue1"),
        make_step("step2", "queue2"),
        make_step("step3"),  # no task_queue
    ]
    mocker.patch.object(spec, "get_study_steps", return_value=steps)

    # Case 1: omit_tag=False and CONFIG.omit_queue_tag=False → prefix is added
    result = spec.get_task_queues(omit_tag=False)
    assert result == {
        "step1": "[merlin]_queue1",
        "step2": "[merlin]_queue2",
    }

    # Case 2: omit_tag=True overrides config → prefix is omitted
    result2 = spec.get_task_queues(omit_tag=True)
    assert result2 == {
        "step1": "queue1",
        "step2": "queue2",
    }

    # Case 3: CONFIG.omit_queue_tag=True → prefix is omitted
    mock_config.celery.omit_queue_tag = True
    result3 = spec.get_task_queues(omit_tag=False)
    assert result3 == {
        "step1": "queue1",
        "step2": "queue2",
    }


def test_get_queue_step_relationship_basic(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_queue_step_relationship` for grouping steps by queue with tag prefix.

    This test sets:
        - Config with tag prefix "[merlin]_" and `omit_queue_tag=False`.
        - Steps assigned to queues, including multiple steps sharing a queue.

    It checks that:
        - Steps are grouped correctly under their respective tagged queue names.
        - Steps without a `task_queue` are excluded.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_config = SimpleNamespace(celery=SimpleNamespace(omit_queue_tag=False, queue_tag="[merlin]_"))
    mocker.patch("merlin.config.configfile.CONFIG", mock_config)

    steps = [
        make_step("step1", "queue1"),
        make_step("step2", "queue1"),
        make_step("step3", "queue2"),
        make_step("step4"),  # no task_queue
    ]
    mocker.patch.object(spec, "get_study_steps", return_value=steps)

    result = spec.get_queue_step_relationship()

    expected = {
        "[merlin]_queue1": ["step1", "step2"],
        "[merlin]_queue2": ["step3"],
    }
    assert result == expected


def test_get_queue_step_relationship_with_omit_tag(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_queue_step_relationship` when queue tag prefixing is disabled in config.

    This test sets:
        - Config with `omit_queue_tag=True` and a tag "[merlin]_".
        - Steps assigned to multiple queues.

    It verifies that:
        - Steps are grouped correctly by queue.
        - No prefix is added to queue names due to config settings.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mock_config = SimpleNamespace(celery=SimpleNamespace(omit_queue_tag=True, queue_tag="[merlin]_"))
    mocker.patch("merlin.config.configfile.CONFIG", mock_config)

    steps = [
        make_step("step1", "q1"),
        make_step("step2", "q1"),
        make_step("step3", "q2"),
    ]
    mocker.patch.object(spec, "get_study_steps", return_value=steps)

    result = spec.get_queue_step_relationship()

    expected = {
        "q1": ["step1", "step2"],
        "q2": ["step3"],
    }
    assert result == expected


def test_get_queue_list_all_steps(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_queue_list` with "all" steps specified.

    This test patches `get_task_queues` to return three steps,
    with two of them sharing the same queue.

    It verifies that:
        - All unique queues are returned.
        - The result is sorted and deduplicated.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    # Simulate step-to-queue mapping
    mocker.patch.object(
        spec,
        "get_task_queues",
        return_value={
            "step1": "[merlin]_queue1",
            "step2": "[merlin]_queue2",
            "step3": "[merlin]_queue1",  # shared queue
        },
    )

    queues = spec.get_queue_list(["all"])
    assert queues == sorted(set(["[merlin]_queue1", "[merlin]_queue2"]))


def test_get_queue_list_specific_steps(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_queue_list` with a subset of specific steps.

    This test patches `get_task_queues` to return distinct queues for each step.

    It verifies that:
        - Only the queues for the requested steps are returned.
        - Duplicate queues across multiple steps are deduplicated.
        - The output is sorted.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mocker.patch.object(
        spec,
        "get_task_queues",
        return_value={
            "step1": "q1",
            "step2": "q2",
            "step3": "q1",
        },
    )

    queues = spec.get_queue_list(["step1", "step3"])
    assert queues == sorted(set(["q1"]))

    queues = spec.get_queue_list(["step1", "step2"])
    assert queues == sorted(set(["q1", "q2"]))


def test_get_queue_list_single_step_string(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_queue_list` when a single step is passed as a string (not a list).

    This test confirms that:
        - The method handles string input.
        - The correct queue is returned as a one-element list.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mocker.patch.object(
        spec,
        "get_task_queues",
        return_value={
            "step1": "q1",
            "step2": "q2",
        },
    )

    queues = spec.get_queue_list("step2")
    assert queues == ["q2"]


def test_get_queue_list_invalid_step_raises_keyerror(mocker: MockerFixture, spec: MerlinSpec, caplog: CaptureFixture):
    """
    Test `get_queue_list` with an invalid step name.

    This test:
        - Patches `get_task_queues` with two valid steps.
        - Requests a nonexistent step, expecting a `KeyError`.

    It verifies that:
        - A `KeyError` is raised for unknown steps.
        - The error message in logs includes the invalid and valid step names.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
        caplog: PyTest caplog fixture.
    """
    mocker.patch.object(
        spec,
        "get_task_queues",
        return_value={
            "step1": "q1",
            "step2": "q2",
        },
    )

    with pytest.raises(KeyError):
        spec.get_queue_list(["stepX"])

    assert "Invalid steps" in caplog.text
    assert "stepX" in caplog.text
    assert "step1" in caplog.text
    assert "step2" in caplog.text


def test_make_queue_string_returns_sorted_unique_string(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `make_queue_string` returns a properly quoted, sorted, and deduplicated queue string.

    This test patches `get_queue_list` to return duplicate and unordered queues.

    It verifies that:
        - The resulting string is sorted.
        - Duplicates are removed.
        - The string is shell-quoted using `shlex.quote`.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    mocker.patch.object(spec, "get_queue_list", return_value=["q2", "q1", "q1"])

    result = spec.make_queue_string(["step1", "step2"])

    # Accept either ordering of q1,q2 or q2,q1
    expected1 = shlex.quote("q1,q2")
    expected2 = shlex.quote("q2,q1")

    assert result in {expected1, expected2}


def test_get_worker_names_returns_all_worker_keys(spec: MerlinSpec):
    """
    Test `get_worker_names` returns all defined worker keys from the spec.

    This test sets `merlin["resources"]["workers"]` with three worker definitions.

    It verifies that:
        - All keys are extracted.
        - The result is sorted alphabetically.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {
        "resources": {
            "workers": {
                "workerA": {},
                "workerB": {},
                "workerC": {},
            }
        }
    }

    result = spec.get_worker_names()
    assert sorted(result) == ["workerA", "workerB", "workerC"]


def test_get_tasks_per_step_no_samples_no_parameters(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_tasks_per_step` when there are no parameters and no sample file.

    This test sets up:
        - `merlin["samples"]` as None.
        - No parameter labels from `get_parameters`.
        - A step with no tokens requiring expansion.
        - `needs_merlin_expansion` returns False.

    It verifies that the method defaults to returning 1 task for the step.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {
        "samples": None,
    }

    # Patch get_parameters to return empty parameter labels
    mocker.patch.object(spec, "get_parameters", return_value=SimpleNamespace(labels={}, length=0))

    # Mock study steps with no expansion triggers
    step = SimpleNamespace(name="step1", run={"cmd": "echo hello", "restart": ""})
    mocker.patch.object(spec, "get_study_steps", return_value=[step])

    # Patch needs_merlin_expansion to always return False
    mocker.patch("merlin.spec.specification.needs_merlin_expansion", return_value=False)

    result = spec.get_tasks_per_step()
    assert result == {"step1": 1}


def test_get_tasks_per_step_with_parameters_only(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_tasks_per_step` when parameters are present but no samples are defined.

    This test sets up:
        - `merlin["samples"]` as None.
        - Two parameter labels with a total of 2 combinations.
        - A step that references one of the parameters.
        - `needs_merlin_expansion` returns True for the parameter token.

    It verifies that the method returns a task count equal to the number of parameter combinations.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {
        "samples": None,
    }

    mocker.patch.object(spec, "get_parameters", return_value=SimpleNamespace(labels={"param1": 1, "param2": 2}, length=2))

    step = SimpleNamespace(name="step1", run={"cmd": "$(param1)", "restart": ""})
    mocker.patch.object(spec, "get_study_steps", return_value=[step])

    # First call: match param → True
    # Second call: for sample check (which shouldn't happen) → False
    mocker.patch("merlin.spec.specification.needs_merlin_expansion", side_effect=[True, False])

    result = spec.get_tasks_per_step()
    assert result == {"step1": 2}


def test_get_tasks_per_step_with_samples_and_parameters(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_tasks_per_step` when both parameters and samples are defined.

    This test sets up:
        - A sample file with 3 entries and 2 column labels.
        - One parameter with a single value.
        - A step referencing both parameter and sample tokens.
        - `needs_merlin_expansion` returns True for both tokens.

    It verifies that the task count is the product of parameter and sample counts (1 * 3 = 3).

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {"samples": {"file": "fake_samples.npy", "column_labels": ["x", "y"]}}

    # Mock sample file load to return 3 samples
    mocker.patch("merlin.spec.specification.load_array_file", return_value=[[1, 2], [3, 4], [5, 6]])

    mocker.patch.object(spec, "get_parameters", return_value=SimpleNamespace(labels={"param1": 1}, length=1))

    step = SimpleNamespace(name="step1", run={"cmd": "$(x) $(param1)", "restart": ""})
    mocker.patch.object(spec, "get_study_steps", return_value=[step])

    # First call: param match → True
    # Second call: column label match → True
    mocker.patch("merlin.spec.specification.needs_merlin_expansion", side_effect=[True, True])

    result = spec.get_tasks_per_step()
    assert result == {"step1": 3}  # 1 param * 3 samples = 3 tasks


def test_get_tasks_per_step_no_expansion_with_samples(mocker: MockerFixture, spec: MerlinSpec):
    """
    Test `get_tasks_per_step` when samples exist but the step does not reference any expandable tokens.

    This test sets up:
        - A sample file with 1 sample.
        - No parameters.
        - A step with a plain command (no expansion).
        - `needs_merlin_expansion` returns False for both parameter and sample tokens.

    It verifies that the method returns 1 task since no expansion is triggered.

    Args:
        mocker: PyTest mocker fixture.
        spec: A MerlinSpec instance for testing.
    """
    spec.merlin = {"samples": {"file": "fake_samples.npy", "column_labels": ["x", "y"]}}

    mocker.patch("merlin.spec.specification.load_array_file", return_value=[[1, 2]])

    mocker.patch.object(spec, "get_parameters", return_value=SimpleNamespace(labels={}, length=0))

    step = SimpleNamespace(name="step1", run={"cmd": "echo done", "restart": ""})
    mocker.patch.object(spec, "get_study_steps", return_value=[step])

    # Both calls return False
    mocker.patch("merlin.spec.specification.needs_merlin_expansion", side_effect=[False, False])

    result = spec.get_tasks_per_step()
    assert result == {"step1": 1}


def test_create_param_maps_basic(spec: MerlinSpec):
    """
    Test `_create_param_maps` for generating expanded labels and label-to-parameter mappings.

    This test sets up:
        - A parameter generator with two parameters: X and Y.
        - Each parameter maps to one or more values using a `%%` label token format.

    It verifies that:
        - Expanded labels are correctly generated from parameter values.
        - The label-to-parameter mapping is populated accordingly.

    Args:
        spec: A MerlinSpec instance for testing.
    """
    mock_param_gen = SimpleNamespace(
        labels={"X": "x.%%", "Y": "y.%%"},
        parameters={"X": [1, 2], "Y": [10]},
        label_token="%%",
        token="PARAM",
        get_used_parameters=lambda step: ["X", "Y"],
    )

    expanded_labels = {}
    label_param_map = {}

    spec._create_param_maps(mock_param_gen, expanded_labels, label_param_map)

    assert expanded_labels == {"X": ["x.1", "x.2"], "Y": ["y.10"]}

    assert label_param_map == {
        "x.1": {"X": 1},
        "x.2": {"X": 2},
        "y.10": {"Y": 10},
    }


def test_get_step_param_map_basic(mocker: MockerFixture):
    """
    Test `get_step_param_map` for a single step with a command and restart command
    using multiple parameter combinations.

    This test mocks:
        - A parameter generator with token `$PARAM` and parameters `P1`, `P2`.
        - A step containing `$PARAM(foo)` and `$PARAM(bar)` tokens in its commands.
        - Parameter mappings that expand `P1` and `P2` into multiple values.

    It verifies that the method correctly expands step names and constructs
    the corresponding `cmd` and `restart_cmd` parameter maps.

    Args:
        mocker: PyTest mocker fixture.
    """
    spec = mocker.MagicMock()
    spec.token = "$PARAM"

    # Simulate get_parameters()
    param_gen = mocker.MagicMock()
    param_gen.token = "$PARAM"
    param_gen.get_used_parameters.return_value = ["P1", "P2"]
    spec.get_parameters.return_value = param_gen

    # Simulate get_study_steps()
    step = SimpleNamespace(name="step1", run={"cmd": "run $PARAM(foo) and $PARAM(bar)", "restart": "rerun $PARAM(bar) only"})
    spec.get_study_steps.return_value = [step]

    # Simulate _create_param_maps()
    expanded_labels = {"P1": ["foo1", "foo2"], "P2": ["bar1", "bar2"]}
    label_param_map = {"foo1": {"foo": 1}, "bar1": {"bar": 10}, "foo2": {"foo": 2}, "bar2": {"bar": 20}}

    def create_param_maps(p, el, lpm):
        el.update(expanded_labels)
        lpm.update(label_param_map)

    spec._create_param_maps.side_effect = create_param_maps

    spec.get_step_param_map = MerlinSpec.get_step_param_map.__get__(spec)

    result = spec.get_step_param_map()

    assert result == {
        "step1_foo1.bar1": {"cmd": {"foo": 1, "bar": 10}, "restart_cmd": {"bar": 10}},
        "step1_foo2.bar2": {"cmd": {"foo": 2, "bar": 20}, "restart_cmd": {"bar": 20}},
    }


def test_get_step_param_map_no_token_match(mocker: MockerFixture):
    """
    Test `get_step_param_map` for a step whose command does not reference
    any parameter tokens.

    This test mocks:
        - A parameter generator with one parameter `P` that expands to a single label.
        - A step that does not use any `$PARAM(...)` tokens in its commands.

    It verifies that the resulting `cmd` and `restart_cmd` maps are empty,
    but still keyed under the expanded label for completeness.

    Args:
        mocker: PyTest mocker fixture.
    """
    spec = mocker.MagicMock()
    spec.token = "$PARAM"

    # Step that does not use any param tokens
    step = SimpleNamespace(name="stepX", run={"cmd": "echo hello", "restart": "noop"})
    spec.get_study_steps.return_value = [step]

    param_gen = mocker.MagicMock()
    param_gen.token = "$PARAM"
    param_gen.get_used_parameters.return_value = ["P"]
    spec.get_parameters.return_value = param_gen

    def fake_param_maps(pg, expanded_labels, label_param_map):
        expanded_labels.update({"P": ["a"]})
        label_param_map.update({"a": {"x": 42}})

    spec.get_step_param_map = MerlinSpec.get_step_param_map.__get__(spec)

    spec._create_param_maps.side_effect = fake_param_maps

    result = spec.get_step_param_map()

    assert result == {"stepX_a": {"cmd": {}, "restart_cmd": {}}}


def test_get_step_param_map_multiple_steps(mocker: MockerFixture):
    """
    Test `get_step_param_map` with multiple steps where each uses the same
    parameter label in different parts of their run definitions.

    This test mocks:
        - Two steps: one using a `$PARAM(x)` in `cmd`, and one in `restart`.
        - A parameter mapping that expands a single parameter `P` to one label.

    It verifies that each step correctly maps its command fields to the corresponding
    parameter values, depending on where the token appears.

    args:
        mocker: PyTest mocker fixture.
    """
    spec = mocker.MagicMock()
    spec.token = "$PARAM"

    step1 = SimpleNamespace(name="step1", run={"cmd": "$PARAM(x)", "restart": ""})
    step2 = SimpleNamespace(name="step2", run={"cmd": "", "restart": "$PARAM(x)"})
    spec.get_study_steps.return_value = [step1, step2]

    param_gen = mocker.MagicMock()
    param_gen.token = "$PARAM"
    param_gen.get_used_parameters.side_effect = lambda step: ["P"]
    spec.get_parameters.return_value = param_gen

    def fake_param_maps(pg, expanded_labels, label_param_map):
        expanded_labels.update({"P": ["val"]})
        label_param_map.update({"val": {"x": 7}})

    spec._create_param_maps.side_effect = fake_param_maps

    spec.get_step_param_map = MerlinSpec.get_step_param_map.__get__(spec)

    result = spec.get_step_param_map()

    assert result == {
        "step1_val": {"cmd": {"x": 7}, "restart_cmd": {}},
        "step2_val": {"cmd": {}, "restart_cmd": {"x": 7}},
    }
