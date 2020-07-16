"""
Tests for the maestroadapter.py module.
"""
import os
import shutil
import tempfile
import unittest

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


def test_get_task_queue_default():
    """
    Given a steps dictionary that sets the task queue to `test_queue` return
    `test_queue` as the queue name.
    """
    steps = {"run": {"task_queue": "test_queue"}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "test_queue"


def test_get_task_queue_task_queue_missing():
    """
    Given a steps dictionary  where the run is set to an empty dictionary
    return `merlin` as the queue name.
    """
    steps = {"run": {}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


def test_get_task_queue_run_missing():
    """
    Given an empty steps dictionary return `merlin` as the queue name.
    """
    steps = {}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


def test_get_task_queue_steps_None():
    """
    Given the value of None return `merlin` as the queue name.
    """
    steps = None
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


def test_get_task_queue_run_None():
    """
    Given a steps dictionary where the run value is set to None, return
    `merlin` as the queue name.
    """
    steps = {"run": None}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


def test_get_task_queue_None():
    """
    Given a steps dictionary where the task_queue is set to None, return
    `merlin` as the queue name.
    """
    steps = {"run": {"task_queue": None}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


def test_mastro_task_queue_None_str():
    """
    Given a steps dictionary where the task_queue is set to the string value
    'None`, return `merlin` as the queue name.
    """
    steps = {"run": {"task_queue": "None"}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


def test_get_task_queue_none_str():
    """
    Given a steps dictionary where the task_queue is set to the string value
    'none', return `merlin` as the queue name.
    """
    steps = {"run": {"task_queue": "none"}}
    queue = Step.get_task_queue_from_dict(steps)
    assert queue == "merlin"


class TestMerlinStudy(unittest.TestCase):
    """Test the logic for parsing the MerlinStudy."""

    @staticmethod
    def file_contains_string(f, string):
        return string in open(f, "r").read()

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.merlin_spec_filepath = os.path.join(self.tmpdir, "basic_ensemble.yaml")

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
        assert TestMerlinStudy.file_contains_string(
            self.merlin_spec_filepath, "$(SPECROOT)"
        )
        assert TestMerlinStudy.file_contains_string(
            self.merlin_spec_filepath, "$(OUTPUT_PATH)"
        )
        assert TestMerlinStudy.file_contains_string(self.merlin_spec_filepath, "$PATH")

        assert not TestMerlinStudy.file_contains_string(
            self.study.expanded_spec.path, "$(SPECROOT)"
        )
        assert not TestMerlinStudy.file_contains_string(
            self.study.expanded_spec.path, "$(OUTPUT_PATH)"
        )
        assert TestMerlinStudy.file_contains_string(
            self.study.expanded_spec.path, "$PATH"
        )
        assert not TestMerlinStudy.file_contains_string(
            self.study.expanded_spec.path, "PATH_VAR: $PATH"
        )

    def test_column_label_conflict(self):
        """
        If there is a common key between Maestro's global.parameters and
        Merlin's sample/column_labels, an error should be raised.
        """
        merlin_spec_conflict = os.path.join(self.tmpdir, "basic_ensemble_conflict.yaml")
        with open(merlin_spec_conflict, "w+") as _file:
            _file.write(MERLIN_SPEC_CONFLICT)
        try:
            study_conflict = MerlinStudy(merlin_spec_conflict)
        except ValueError:
            pass
        else:
            assert False

    def test_no_env(self):
        """
        A MerlinStudy should be able to support a MerlinSpec that does not contain
        the optional `env` section.
        """
        merlin_spec_no_env_filepath = os.path.join(
            self.tmpdir, "basic_ensemble_no_env.yaml"
        )
        with open(merlin_spec_no_env_filepath, "w+") as _file:
            _file.write(MERLIN_SPEC_NO_ENV)
        try:
            study_no_env = MerlinStudy(merlin_spec_no_env_filepath)
        except Exception as e:
            assert False
