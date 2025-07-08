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
import yaml
from io import StringIO
from pathlib import Path

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

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


def test_yaml_sections_returns_expected_structure(mocker: MockerFixture):
    spec = MerlinSpec()
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


def test_sections_returns_expected_structure(mocker: MockerFixture):
    spec = MerlinSpec()
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
    bad_path = "/nonexistent/spec.yaml"

    with pytest.raises(Exception):
        MerlinSpec.load_specification(bad_path)


def test_load_spec_from_string_calls_expected_methods(mocker: MockerFixture):
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


def test_get_study_step_names_returns_correct_names():
    spec = MerlinSpec()
    spec.study = [
        {"name": "step1", "run": "echo 1"},
        {"name": "step2", "run": "echo 2"},
        {"name": "step3", "run": "echo 3"},
    ]

    result = spec.get_study_step_names()

    assert result == ["step1", "step2", "step3"]


def test_get_study_step_names_empty():
    spec = MerlinSpec()
    spec.study = []

    result = spec.get_study_step_names()

    assert result == []


def test_verify_loads_schema_and_calls_all_verification_methods(mocker: MockerFixture):
    spec = MerlinSpec()

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


def test_verify_workers_with_valid_steps():
    spec = MerlinSpec()
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


def test_verify_workers_with_invalid_step_raises():
    spec = MerlinSpec()
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


def test_verify_merlin_block_validates_and_verifies_workers(mocker: MockerFixture):
    spec = MerlinSpec()
    spec.merlin = {"resources": {"workers": {}}}
    mock_schema = {"type": "object"}

    validate_mock = mocker.patch("merlin.spec.specification.YAMLSpecification.validate_schema")
    verify_workers_mock = mocker.patch.object(spec, "_verify_workers")

    spec.verify_merlin_block(mock_schema)

    validate_mock.assert_called_once_with("merlin", spec.merlin, mock_schema)
    verify_workers_mock.assert_called_once()


def test_verify_batch_block_with_lsf_walltime_logs_warning(mocker: MockerFixture, caplog: CaptureFixture):
    caplog.set_level(logging.WARNING)

    spec = MerlinSpec()
    spec.batch = {"type": "lsf", "walltime": "01:00:00"}
    mock_schema = {"type": "object"}

    validate_mock = mocker.patch("merlin.spec.specification.YAMLSpecification.validate_schema")

    spec.verify_batch_block(mock_schema)

    validate_mock.assert_called_once_with("batch", spec.batch, mock_schema)
    assert "The walltime argument is not available in lsf." in caplog.text


def test_verify_batch_block_with_slurm_skips_walltime_check(mocker: MockerFixture):
    spec = MerlinSpec()
    spec.batch = {"type": "slurm", "walltime": "02:00:00"}
    mock_schema = {"type": "object"}

    validate_mock = mocker.patch("merlin.spec.specification.YAMLSpecification.validate_schema")
    log_mock = mocker.patch("merlin.spec.specification.LOG")

    spec.verify_batch_block(mock_schema)

    validate_mock.assert_called_once_with("batch", spec.batch, mock_schema)
    log_mock.warning.assert_not_called()


def test_load_merlin_block_with_section_present():
    yaml_content = """
    merlin:
      resources:
        workers:
          worker1:
            steps: ["step1"]
    """
    stream = StringIO(yaml_content)
    result = MerlinSpec.load_merlin_block(stream)

    assert result == {
        "resources": {
            "workers": {
                "worker1": {"steps": ["step1"]}
            }
        }
    }


def test_load_merlin_block_missing_section_logs_warning(caplog: CaptureFixture):
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
    yaml_content = """
    user:
      author: me
      contact: me@example.com
    """
    stream = StringIO(yaml_content)
    result = MerlinSpec.load_user_block(stream)

    assert result == {
        "author": "me",
        "contact": "me@example.com"
    }


def test_load_user_block_missing_section_returns_empty():
    yaml_content = """
    description:
      name: test_study
    """
    stream = StringIO(yaml_content)
    result = MerlinSpec.load_user_block(stream)

    assert result == {}
