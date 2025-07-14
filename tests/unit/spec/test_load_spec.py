##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

import os
import shutil
import tempfile
import unittest
from copy import deepcopy

import yaml
from jsonschema import ValidationError

from merlin.spec import defaults
from merlin.spec.specification import MerlinSpec


MERLIN_SPEC = """
description:
    name: basic_ensemble
    description: Run 100 hello worlds.

batch:
    type: local

env:
    variables:
        OUTPUT_PATH: ./studies
        HELLO: $(SPECROOT)/hello_world.py

study:
    - name: hello
      description: |
         process a sample with hello world
      run:
        cmd: |
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


NO_MERLIN = """
description:
    name: basic_ensemble_no_merlin
    description: Run 100 hello worlds.

batch:
    type: local

env:
    variables:
        OUTPUT_PATH: ./studies
        HELLO: $(SPECROOT)/hello_world.py

study:
    - name: hello
      description: |
         process a sample with hello world
      run:
        cmd: |
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
"""

INVALID_MERLIN = """
description:
    name: basic_ensemble_invalid_merlin
    description: Template yaml to ensure our custom merlin block verification works as intended

batch:
    type: local

study:
    - name: step1
      description: |
         this won't actually run
      run:
        cmd: |
          echo "if this is printed something is bad"

merlin:
    resources:
        task_server: celery
        overlap: false
        workers:
            worker1:
                steps: []
"""


class TestMerlinSpec(unittest.TestCase):
    """Test the logic for parsing the Merlin spec into a MerlinSpec."""

    def setUp(self):
        # Store original defaults to restore later
        self.original_defaults = {
            "WORKER": deepcopy(defaults.WORKER),
            "BATCH": deepcopy(defaults.BATCH),
            "ENV": deepcopy(defaults.ENV),
            "MERLIN": deepcopy(defaults.MERLIN),
            "PARAMETER": deepcopy(defaults.PARAMETER),
            "SAMPLES": deepcopy(defaults.SAMPLES),
            "STUDY_STEP_RUN": deepcopy(defaults.STUDY_STEP_RUN),
        }

        self.tmpdir = tempfile.mkdtemp()
        self.merlin_spec_filepath = os.path.join(self.tmpdir, "basic_ensemble.yaml")

        with open(self.merlin_spec_filepath, "w+") as _file:
            _file.write(MERLIN_SPEC)

        self.spec = MerlinSpec.load_specification(self.merlin_spec_filepath)

    def tearDown(self):
        # Restore original defaults to prevent test contamination
        for key, value in self.original_defaults.items():
            setattr(defaults, key, value)

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_specroot(self):
        """
        Given a Merlin spec with no other configurations to the MerlinSpec
        object, then the default spec root path should be set to the base
        path of the file.
        """
        self.assertEqual(self.spec.specroot, self.tmpdir)

    def test_filepath(self):
        """
        Given a Merlin spec with no other configurations to the MerlinSpec
        object, then the filepath should be set to the path of the yaml file
        itself.
        """
        self.assertEqual(self.spec.path, self.merlin_spec_filepath)

    # TODO consider moving default behavior like this to MerlinStudy
    #    def test_task_server(self):
    #        """
    #        Given a Merlin spec with no Celery configuration specified in the spec,
    #        then the MerlinSpec object should default the task server to `celery`.
    #        """
    #        self.assertEqual(self.spec.merlin["resources"]["task_server, 'celery')

    def test_sample_files(self):
        """
        Given a Merlin spec with a sample file defined with a `SPECROOT`
        variable, when `spec.sample_files` is called, then the MerlinSpec
        object should return the expanded path (replaced SPECROOT) path to the
        sample files.
        """
        expected = "$(OUTPUT_PATH)/samples.npy"
        self.assertEqual(self.spec.merlin["samples"]["file"], expected)

    def test_sample_labels(self):
        """
        Given a Merlin spec with colume labels defined, when
        'spec.sample_labels' is called then the MerlinSpec object should
        return the values set in the column_labels.
        """
        self.assertEqual(self.spec.merlin["samples"]["column_labels"], ["X0", "X1"])

    def test_spec_sample_generator(self):
        """
        Given a Merlin spec with the generate section defined when
        'spec.sample_generate` is called then the MerlinSpec object should
        return the values set in the generate cmd section.
        """
        expected = "python $(SPECROOT)/make_samples.py -n 100 -outfile=$(OUTPUT_PATH)/samples.npy"
        self.assertEqual(self.spec.merlin["samples"]["generate"]["cmd"], expected)


class TestSpecNoMerlin(unittest.TestCase):
    """Test the logic for parsing a spec with no Merlin block into a MerlinSpec."""

    def setUp(self):
        # Store original defaults to restore later
        self.original_defaults = {
            "WORKER": deepcopy(defaults.WORKER),
            "BATCH": deepcopy(defaults.BATCH),
            "ENV": deepcopy(defaults.ENV),
            "MERLIN": deepcopy(defaults.MERLIN),
            "PARAMETER": deepcopy(defaults.PARAMETER),
            "SAMPLES": deepcopy(defaults.SAMPLES),
            "STUDY_STEP_RUN": deepcopy(defaults.STUDY_STEP_RUN),
        }

        self.tmpdir = tempfile.mkdtemp()
        self.merlin_spec_filepath = os.path.join(self.tmpdir, "no_merlin.yaml")

        with open(self.merlin_spec_filepath, "w+") as _file:
            _file.write(NO_MERLIN)

        self.spec = MerlinSpec.load_specification(self.merlin_spec_filepath)

    def tearDown(self):
        # Restore original defaults to prevent test contamination
        for key, value in self.original_defaults.items():
            setattr(defaults, key, value)

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_default_merlin_block(self):
        self.assertEqual(self.spec.merlin["resources"]["task_server"], "celery")
        self.assertEqual(self.spec.merlin["resources"]["overlap"], False)
        self.assertEqual(self.spec.merlin["resources"]["workers"]["default_worker"]["steps"], ["all"])
        self.assertEqual(self.spec.merlin["resources"]["workers"]["default_worker"]["batch"], None)
        self.assertEqual(self.spec.merlin["resources"]["workers"]["default_worker"]["nodes"], None)
        self.assertEqual(self.spec.merlin["samples"], None)


class TestCustomVerification(unittest.TestCase):
    """
    Tests to make sure our custom verification on merlin specific parts of our
    spec files is working as intended. Verification happens in
    merlin/spec/specification.py

    NOTE: reset_spec() should be called at the end of each test to make sure the
          test file is reset.

    CREATING A NEW VERIFICATION TEST:
        1. Read in the spec with self.read_spec()
        2. Modify the spec with an invalid value to test for (e.g. a bad step, a bad walltime, etc.)
        3. Update the spec file with self.update_spec(spec)
        4. Assert that the correct error is thrown
        5. Reset the spec file with self.reset_spec()
    """

    def setUp(self):
        # Store original defaults to restore later
        self.original_defaults = {
            "WORKER": deepcopy(defaults.WORKER),
            "BATCH": deepcopy(defaults.BATCH),
            "ENV": deepcopy(defaults.ENV),
            "MERLIN": deepcopy(defaults.MERLIN),
            "PARAMETER": deepcopy(defaults.PARAMETER),
            "SAMPLES": deepcopy(defaults.SAMPLES),
            "STUDY_STEP_RUN": deepcopy(defaults.STUDY_STEP_RUN),
        }

        self.tmpdir = tempfile.mkdtemp()
        self.merlin_spec_filepath = os.path.join(self.tmpdir, "merlin_verification.yaml")
        self.write_spec(INVALID_MERLIN)

    def tearDown(self):
        # Restore original defaults to prevent test contamination
        for key, value in self.original_defaults.items():
            setattr(defaults, key, value)

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def reset_spec(self):
        self.write_spec(INVALID_MERLIN)

    def write_spec(self, spec):
        with open(self.merlin_spec_filepath, "w+") as _file:
            _file.write(spec)

    def read_spec(self):
        with open(self.merlin_spec_filepath, "r") as yamfile:
            spec = yaml.load(yamfile, yaml.Loader)
        return spec

    def update_spec(self, spec):
        with open(self.merlin_spec_filepath, "w") as yamfile:
            yaml.dump(spec, yamfile, yaml.Dumper)

    def test_invalid_step(self):
        # Read in the existing spec and update it with our bad step
        spec = self.read_spec()
        spec["merlin"]["resources"]["workers"]["worker1"]["steps"].append("bad_step")
        self.update_spec(spec)

        # Assert that the invalid format was caught
        with self.assertRaises(ValueError):
            MerlinSpec.load_specification(self.merlin_spec_filepath)

        # Reset the spec to the default value
        self.reset_spec()

    def test_invalid_walltime(self):
        # Read in INVALID_MERLIN spec
        spec = self.read_spec()

        invalid_walltimes = ["", -1]

        # Loop through the invalid walltimes and make sure they're all caught
        for time in invalid_walltimes:
            spec["batch"]["walltime"] = time
            self.update_spec(spec)

            with self.assertRaises((ValidationError, ValueError)):
                MerlinSpec.load_specification(self.merlin_spec_filepath)

        # Reset the spec
        self.reset_spec()
