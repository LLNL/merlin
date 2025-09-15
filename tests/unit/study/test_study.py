##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the maestroadapter.py module.
"""

import os
import shutil
import tempfile
import unittest

import pytest

from merlin.study.step import Step
from merlin.study.study import MerlinStudy


MERLIN_SPEC = """
description:
    name: unit_test1
    description: Run 100 hello worlds.

batch:
    type: local

env:
    variables:
        OUTPUT_PATH: ./studies
        PATH_VAR: $PATH

    labels:
        SHARED: $(SPECROOT)/../shared
        HELLO: $(SHARED)/hello_world.py

study:
    - name: hello
      description: |
         process a sample with hello world
      run:
        cmd: |
          echo $PATH
          python $(HELLO) -outfile hello_world_output_$(merlin_sample_id).json $(X0) $(X1) $(X2)
        procs: 1
        nodes: 1
        task_queue: hello_queue

    - name: test_special_vars
      description: test the special vars
      run:
        cmd: |
            echo $(MERLIN_SPEC_ORIGINAL_TEMPLATE)
            echo $(MERLIN_SPEC_EXECUTED_RUN)
            echo $(MERLIN_SPEC_ARCHIVED_COPY)
        task_queue: special_var_queue

global.parameters:
    X2:
        values : [0.5]
        label  : X2.%%
    N_NEW:
        values : [100]
        label  : N_NEW.%%

merlin:
    samples:
        generate:
            cmd: python $(SPECROOT)/make_samples.py -n 100 -outfile=$(OUTPUT_PATH)/samples.npy
        file: $(OUTPUT_PATH)/samples.npy
        column_labels: [X0, X1]
"""


MERLIN_SPEC_NO_ENV = """
description:
    name: unit_test2
    description: Run 100 hello worlds.

batch:
    type: local

env:
    variables:
        OUTPUT_PATH: ./studies

    labels:
        SHARED: $(SPECROOT)/../shared
        HELLO: $(SHARED)/hello_world.py

study:
    - name: hello
      description: |
         process a sample with hello world
      run:
        cmd: |
          echo $PATH
          python $(HELLO) -outfile hello_world_output_$(merlin_sample_id).json $(X0) $(X1) $(X2)
        procs: 1
        nodes: 1
        task_queue: hello_queue

global.parameters:
    X2:
        values : [0.5]
        label  : X2.%%
    N_NEW:
        values : [100]
        label  : N_NEW.%%

merlin:
    samples:
        generate:
            cmd: python $(SPECROOT)/make_samples.py -n 100 -outfile=$(OUTPUT_PATH)/samples.npy
        file: $(OUTPUT_PATH)/samples.npy
        column_labels: [X0, X1]
"""

MERLIN_SPEC_CONFLICT = """
description:
    name: unit_test3
    description: Run 100 hello worlds.

batch:
    type: local

env:
    variables:
        OUTPUT_PATH: ./studies

    labels:
        SHARED: $(SPECROOT)/../shared
        HELLO: $(SHARED)/hello_world.py

study:
    - name: hello
      description: |
         process a sample with hello world
      run:
        cmd: |
          echo $PATH
          python $(HELLO) -outfile hello_world_output_$(merlin_sample_id).json $(X0) $(X1) $(X2)
        procs: 1
        nodes: 1
        task_queue: hello_queue

global.parameters:
    X2:
        values : [0.5]
        label  : X2.%%
    N_NEW:
        values : [100]
        label  : N_NEW.%%
    X1:
        values : [0.5]
        label  : X1.%%

merlin:
    samples:
        generate:
            cmd: python $(SPECROOT)/make_samples.py -n 100 -outfile=$(OUTPUT_PATH)/samples.npy
        file: $(OUTPUT_PATH)/samples.npy
        column_labels: [X0, X1]
"""


# TODO many of these more resemble integration tests than unit tests, may want to review unit tests to make it more granular.
def test_get_task_queue_default():
    """
    Given a steps dictionary that sets the task queue to `test_queue` return
    `test_queue` as the queue name.
    """
    steps = {"run": {"task_queue": "test_queue"}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_test_queue"


def test_get_task_queue_task_queue_missing():
    """
    Given a steps dictionary  where the run is set to an empty dictionary
    return `merlin` as the queue name.
    """
    steps = {"run": {}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


def test_get_task_queue_run_missing():
    """
    Given an empty steps dictionary return `merlin` as the queue name.
    """
    steps = {}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


def test_get_task_queue_steps_None():
    """
    Given the value of None return `merlin` as the queue name.
    """
    steps = None
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


def test_get_task_queue_run_None():
    """
    Given a steps dictionary where the run value is set to None, return
    `merlin` as the queue name.
    """
    steps = {"run": None}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


def test_get_task_queue_None():
    """
    Given a steps dictionary where the task_queue is set to None, return
    `merlin` as the queue name.
    """
    steps = {"run": {"task_queue": None}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


def test_mastro_task_queue_None_str():
    """
    Given a steps dictionary where the task_queue is set to the string value
    'None`, return `merlin` as the queue name.
    """
    steps = {"run": {"task_queue": "None"}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


def test_get_task_queue_none_str():
    """
    Given a steps dictionary where the task_queue is set to the string value
    'none', return `merlin` as the queue name.
    """
    steps = {"run": {"task_queue": "none"}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "[merlin]_"


class TestMerlinStudy(unittest.TestCase):
    """Test the logic for parsing the MerlinStudy."""

    @staticmethod
    def file_contains_string(f, string):
        result = False
        with open(f, "r") as infile:
            if string in infile.read():
                result = True
        return result

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.base_name = "basic_ensemble"
        self.merlin_spec_filepath = os.path.join(self.tmpdir, f"{self.base_name}.yaml")

        with open(self.merlin_spec_filepath, "w+") as _file:
            _file.write(MERLIN_SPEC)

        self.study = MerlinStudy(self.merlin_spec_filepath)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_expanded_spec(self):
        """
        Given a Merlin spec with no other configurations to the MerlinSpec
        object, the MerlinStudy should produce a new spec with all instances
        of $(OUTPUT_PATH), $(SPECROOT), and env labels and variables expanded.
        """
        assert TestMerlinStudy.file_contains_string(self.merlin_spec_filepath, "$(SPECROOT)")
        assert TestMerlinStudy.file_contains_string(self.merlin_spec_filepath, "$(OUTPUT_PATH)")
        assert TestMerlinStudy.file_contains_string(self.merlin_spec_filepath, "$PATH")

        assert not TestMerlinStudy.file_contains_string(self.study.expanded_spec.path, "$(SPECROOT)")
        assert not TestMerlinStudy.file_contains_string(self.study.expanded_spec.path, "$(OUTPUT_PATH)")
        assert TestMerlinStudy.file_contains_string(self.study.expanded_spec.path, "$PATH")
        assert not TestMerlinStudy.file_contains_string(self.study.expanded_spec.path, "PATH_VAR: $PATH")

        # Special vars are in the second step of MERLIN_SPEC so grab that step here
        original_special_var_step = self.study.original_spec.study[1]["run"]["cmd"]
        expanded_special_var_step = self.study.expanded_spec.study[1]["run"]["cmd"]

        # Make sure the special filepath variables aren't expanded in the original spec
        assert "$(MERLIN_SPEC_ORIGINAL_TEMPLATE)" in original_special_var_step
        assert "$(MERLIN_SPEC_EXECUTED_RUN)" in original_special_var_step
        assert "$(MERLIN_SPEC_ARCHIVED_COPY)" in original_special_var_step

        # Make sure the special filepath variables aren't left in their variable form in the expanded spec
        assert "$(MERLIN_SPEC_ORIGINAL_TEMPLATE)" not in expanded_special_var_step
        assert "$(MERLIN_SPEC_EXECUTED_RUN)" not in expanded_special_var_step
        assert "$(MERLIN_SPEC_ARCHIVED_COPY)" not in expanded_special_var_step

        # Make sure the special filepath variables we're expanded appropriately in the expanded spec
        assert (
            f"{self.base_name}.orig.yaml" in expanded_special_var_step
            and "unit_test1.orig.yaml" not in expanded_special_var_step
        )
        assert (
            f"{self.base_name}.partial.yaml" in expanded_special_var_step
            and "unit_test1.partial.yaml" not in expanded_special_var_step
        )
        assert (
            f"{self.base_name}.expanded.yaml" in expanded_special_var_step
            and "unit_test1.expanded.yaml" not in expanded_special_var_step
        )

    def test_column_label_conflict(self):
        """
        If there is a common key between Maestro's global.parameters and
        Merlin's sample/column_labels, an error should be raised.
        """
        merlin_spec_conflict: str = os.path.join(self.tmpdir, "basic_ensemble_conflict.yaml")
        with open(merlin_spec_conflict, "w+") as _file:
            _file.write(MERLIN_SPEC_CONFLICT)
        # for some reason flake8 doesn't believe variables instantiated inside the try/with context are assigned
        with pytest.raises(ValueError):
            study_conflict: MerlinStudy = MerlinStudy(merlin_spec_conflict)
            assert not study_conflict, "study_conflict completed construction without raising a ValueError."

    # TODO the pertinent attribute for study_no_env should be examined and asserted to be empty
    def test_no_env(self):
        """
        A MerlinStudy should be able to support a MerlinSpec that does not contain
        the optional `env` section.
        """
        merlin_spec_no_env_filepath: str = os.path.join(self.tmpdir, "basic_ensemble_no_env.yaml")
        with open(merlin_spec_no_env_filepath, "w+") as _file:
            _file.write(MERLIN_SPEC_NO_ENV)
        try:
            study_no_env: MerlinStudy = MerlinStudy(merlin_spec_no_env_filepath)
            bad_type_err: str = f"study_no_env failed construction, is type {type(study_no_env)}."
            assert isinstance(study_no_env, MerlinStudy), bad_type_err
        except Exception as e:
            assert False, f"Encountered unexpected exception, {e}, for viable MerlinSpec without optional 'env' section."


if __name__ == "__main__":
    unittest.main()
