import os
import shutil
import tempfile
import unittest

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


class TestMerlinSpec(unittest.TestCase):
    """Test the logic for parsing the Merlin spec into a MerlinSpec."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.merlin_spec_filepath = os.path.join(self.tmpdir, "basic_ensemble.yaml")

        with open(self.merlin_spec_filepath, "w+") as _file:
            _file.write(MERLIN_SPEC)

        self.spec = MerlinSpec.load_specification(self.merlin_spec_filepath)

    def tearDown(self):
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
        self.tmpdir = tempfile.mkdtemp()
        self.merlin_spec_filepath = os.path.join(self.tmpdir, "no_merlin.yaml")

        with open(self.merlin_spec_filepath, "w+") as _file:
            _file.write(NO_MERLIN)

        self.spec = MerlinSpec.load_specification(self.merlin_spec_filepath)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_default_merlin_block(self):
        self.assertEqual(self.spec.merlin["resources"]["task_server"], "celery")
        self.assertEqual(self.spec.merlin["resources"]["overlap"], False)
        self.assertEqual(
            self.spec.merlin["resources"]["workers"]["default_worker"]["steps"], ["all"]
        )
        self.assertEqual(
            self.spec.merlin["resources"]["workers"]["default_worker"]["batch"], None
        )
        self.assertEqual(
            self.spec.merlin["resources"]["workers"]["default_worker"]["nodes"], None
        )
        self.assertEqual(self.spec.merlin["samples"], None)
