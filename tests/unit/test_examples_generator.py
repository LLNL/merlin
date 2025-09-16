##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/examples/generator.py` module.
"""

import os
from typing import List

import pytest
from tabulate import tabulate

from merlin.examples.generator import (
    EXAMPLES_DIR,
    gather_all_examples,
    gather_example_dirs,
    list_examples,
    setup_example,
    write_example,
)
from tests.utils import create_dir


EXAMPLES_GENERATOR_DIR = "{temp_output_dir}/examples_generator"


def test_gather_example_dirs():
    """Test the `gather_example_dirs` function."""
    example_workflows = [
        "feature_demo",
        "flux",
        "hello",
        "hpc_demo",
        "iterative_demo",
        "lsf",
        "null_spec",
        "openfoam_wf",
        "openfoam_wf_no_docker",
        "openfoam_wf_singularity",
        "optimization",
        "remote_feature_demo",
        "restart",
        "restart_delay",
        "simple_chain",
        "slurm",
    ]
    expected = {}
    for wf_dir in example_workflows:
        expected[wf_dir] = wf_dir
    actual = gather_example_dirs()
    assert actual == expected


def test_gather_all_examples():
    """Test the `gather_all_examples` function."""
    expected = [
        f"{EXAMPLES_DIR}/feature_demo/feature_demo.yaml",
        f"{EXAMPLES_DIR}/flux/flux_local.yaml",
        f"{EXAMPLES_DIR}/flux/flux_par_restart.yaml",
        f"{EXAMPLES_DIR}/flux/flux_par.yaml",
        f"{EXAMPLES_DIR}/flux/paper.yaml",
        f"{EXAMPLES_DIR}/hello/hello_samples.yaml",
        f"{EXAMPLES_DIR}/hello/hello.yaml",
        f"{EXAMPLES_DIR}/hello/my_hello.yaml",
        f"{EXAMPLES_DIR}/hpc_demo/hpc_demo.yaml",
        f"{EXAMPLES_DIR}/iterative_demo/iterative_demo.yaml",
        f"{EXAMPLES_DIR}/lsf/lsf_par_srun.yaml",
        f"{EXAMPLES_DIR}/lsf/lsf_par.yaml",
        f"{EXAMPLES_DIR}/null_spec/null_chain.yaml",
        f"{EXAMPLES_DIR}/null_spec/null_spec.yaml",
        f"{EXAMPLES_DIR}/openfoam_wf/openfoam_wf_docker_template.yaml",
        f"{EXAMPLES_DIR}/openfoam_wf/openfoam_wf.yaml",
        f"{EXAMPLES_DIR}/openfoam_wf_no_docker/openfoam_wf_no_docker_template.yaml",
        f"{EXAMPLES_DIR}/openfoam_wf_no_docker/openfoam_wf_no_docker.yaml",
        f"{EXAMPLES_DIR}/openfoam_wf_singularity/openfoam_wf_singularity.yaml",
        f"{EXAMPLES_DIR}/openfoam_wf_singularity/openfoam_wf_singularity_template.yaml",
        f"{EXAMPLES_DIR}/optimization/optimization_basic.yaml",
        f"{EXAMPLES_DIR}/remote_feature_demo/remote_feature_demo.yaml",
        f"{EXAMPLES_DIR}/restart/restart.yaml",
        f"{EXAMPLES_DIR}/restart_delay/restart_delay.yaml",
        f"{EXAMPLES_DIR}/simple_chain/simple_chain.yaml",
        f"{EXAMPLES_DIR}/slurm/slurm_par_restart.yaml",
        f"{EXAMPLES_DIR}/slurm/slurm_par.yaml",
    ]
    actual = gather_all_examples()
    assert sorted(actual) == sorted(expected)


def test_write_example_dir(examples_testing_dir: str):
    """
    Test the `write_example` function with the src_path as a directory.

    :param examples_testing_dir: The path to the the temp output directory for examples tests
    """
    dir_to_copy = f"{EXAMPLES_DIR}/feature_demo/"
    dst_dir = f"{examples_testing_dir}/write_example_dir"
    write_example(dir_to_copy, dst_dir)
    assert sorted(os.listdir(dir_to_copy)) == sorted(os.listdir(dst_dir))


def test_write_example_file(examples_testing_dir: str):
    """
    Test the `write_example` function with the src_path as a file.

    :param examples_testing_dir: The path to the the temp output directory for examples tests
    """
    file_to_copy = f"{EXAMPLES_DIR}/flux/flux_par.yaml"
    dst_path = f"{examples_testing_dir}/flux_par.yaml"
    write_example(file_to_copy, dst_path)
    assert os.path.exists(dst_path)


def test_list_examples():
    """Test the `list_examples` function to see if it gives us all of the examples that we want."""
    expected_headers = ["name", "description"]
    expected_rows = [
        ["feature_demo", "Run 10 hello worlds."],
        ["flux_local", "Run a scan through Merlin/Maestro"],
        ["flux_par", "A simple ensemble of parallel MPI jobs run by flux."],
        ["flux_par_restart", "A simple ensemble of parallel MPI jobs run by flux."],
        ["paper_flux", "Use flux to run single core MPI jobs and record timings."],
        ["hello", "a very simple merlin workflow"],
        ["hello_samples", "a very simple merlin workflow, with samples"],
        ["hpc_demo", "Demo running a workflow on HPC machines"],
        ["iterative_demo", "Demo of a workflow with self driven iteration/looping"],
        ["lsf_par", "A simple ensemble of parallel MPI jobs run by lsf (jsrun)."],
        ["lsf_par_srun", "A simple ensemble of parallel MPI jobs run by lsf using the srun wrapper (srun)."],
        [
            "null_chain",
            "Run N_SAMPLES steps of TIME seconds each at CONC concurrency.\n"
            "May be used to measure overhead in merlin.\n"
            "Iterates thru a chain of workflows.",
        ],
        [
            "null_spec",
            "run N_SAMPLES null steps at CONC concurrency for TIME seconds each. May be used to measure overhead in merlin.",
        ],
        [
            "openfoam_wf",
            "A parameter study that includes initializing, running,\n"
            "post-processing, collecting, learning and visualizing OpenFOAM runs\n"
            "using docker.",
        ],
        [
            "openfoam_wf_no_docker",
            "A parameter study that includes initializing, running,\n"
            "post-processing, collecting, learning and vizualizing OpenFOAM runs\n"
            "without using docker.",
        ],
        [
            "openfoam_wf_singularity",
            "A parameter study that includes initializing, running,\n"
            "post-processing, collecting, learning and visualizing OpenFOAM runs\n"
            "using singularity.",
        ],
        [
            "optimization_basic",
            "Design Optimization Template\n"
            "To use,\n"
            "1. Specify the first three variables here (N_DIMS, TEST_FUNCTION, DEBUG)\n"
            "2. Run the template_config file in current directory using `python template_config.py`\n"
            "3. Merlin run as usual (merlin run optimization.yaml)\n"
            "* MAX_ITER and the N_SAMPLES options use default values unless using DEBUG mode\n"
            "* BOUNDS_X and UNCERTS_X are configured using the template_config.py scripts",
        ],
        ["remote_feature_demo", "Run 10 hello worlds."],
        ["restart", "A simple ensemble of with restarts."],
        ["restart_delay", "A simple ensemble of with restart delay times."],
        ["simple_chain", "test to see that chains are not run in parallel"],
        ["slurm_par", "A simple ensemble of parallel MPI jobs run by slurm (srun)."],
        ["slurm_par_restart", "A simple ensemble of parallel MPI jobs run by slurm (srun)."],
    ]
    expected = "\n" + tabulate(expected_rows, expected_headers) + "\n"
    actual = list_examples()
    print(f"actual:\n{actual}")
    print(f"expected:\n{expected}")
    assert actual == expected


def test_setup_example_invalid_name():
    """
    Test the `setup_example` function with an invalid example name.
    This should just return None.
    """
    assert setup_example("invalid_example_name", None) is None


def test_setup_example_no_outdir(examples_testing_dir: str):
    """
    Test the `setup_example` function with an invalid example name.
    This should create a directory with the example name (in this case hello)
    and copy all of the example contents to this folder.
    We'll create a directory specifically for this test and move into it so that
    the `setup_example` function creates the hello/ subdirectory in a directory with
    the name of this test (setup_no_outdir).

    :param examples_testing_dir: The path to the the temp output directory for examples tests
    """
    cwd = os.getcwd()

    # Create the temp path to store this setup and move into that directory
    setup_example_dir = os.path.join(examples_testing_dir, "setup_no_outdir")
    create_dir(setup_example_dir)
    os.chdir(setup_example_dir)

    # This should still work and return to us the name of the example
    try:
        assert setup_example("hello", None) == "hello"
    except AssertionError as exc:
        os.chdir(cwd)
        raise AssertionError from exc

    # All files from this example should be written to a directory with the example name
    full_output_path = os.path.join(setup_example_dir, "hello")
    expected_files = [
        os.path.join(full_output_path, "hello_samples.yaml"),
        os.path.join(full_output_path, "hello.yaml"),
        os.path.join(full_output_path, "my_hello.yaml"),
        os.path.join(full_output_path, "requirements.txt"),
        os.path.join(full_output_path, "make_samples.py"),
    ]
    try:
        for file in expected_files:
            assert os.path.exists(file)
    except AssertionError as exc:
        os.chdir(cwd)
        raise AssertionError from exc
    finally:
        os.chdir(cwd)


def test_setup_example_outdir_exists(examples_testing_dir: str):
    """
    Test the `setup_example` function with an output directory that already exists.
    This should just return None.

    :param examples_testing_dir: The path to the the temp output directory for examples tests
    """
    assert setup_example("hello", examples_testing_dir) is None


@pytest.mark.parametrize(
    "example_name, example_files, expected_return",
    [
        (
            "feature_demo",
            [
                ".gitignore",
                "feature_demo.yaml",
                "requirements.txt",
                "scripts/features.json",
                "scripts/hello_world.py",
                "scripts/pgen.py",
            ],
            "feature_demo",
        ),
        (
            "flux_local",
            [
                "flux_local.yaml",
                "flux_par_restart.yaml",
                "flux_par.yaml",
                "paper.yaml",
                "requirements.txt",
                "scripts/flux_info.py",
                "scripts/hello_sleep.c",
                "scripts/hello.c",
                "scripts/make_samples.py",
                "scripts/paper_workers.sbatch",
                "scripts/test_workers.sbatch",
                "scripts/workers.sbatch",
                "scripts/workers.bsub",
            ],
            "flux",
        ),
        (
            "lsf_par",
            [
                "lsf_par_srun.yaml",
                "lsf_par.yaml",
                "scripts/hello.c",
                "scripts/make_samples.py",
            ],
            "lsf",
        ),
        (
            "slurm_par",
            [
                "slurm_par.yaml",
                "slurm_par_restart.yaml",
                "requirements.txt",
                "scripts/hello.c",
                "scripts/make_samples.py",
                "scripts/test_workers.sbatch",
                "scripts/workers.sbatch",
            ],
            "slurm",
        ),
        (
            "hello",
            [
                "hello_samples.yaml",
                "hello.yaml",
                "my_hello.yaml",
                "requirements.txt",
                "make_samples.py",
            ],
            "hello",
        ),
        (
            "hpc_demo",
            [
                "hpc_demo.yaml",
                "cumulative_sample_processor.py",
                "faker_sample.py",
                "sample_collector.py",
                "sample_processor.py",
                "requirements.txt",
            ],
            "hpc_demo",
        ),
        (
            "iterative_demo",
            [
                "iterative_demo.yaml",
                "cumulative_sample_processor.py",
                "faker_sample.py",
                "sample_collector.py",
                "sample_processor.py",
                "requirements.txt",
            ],
            "iterative_demo",
        ),
        (
            "null_spec",
            [
                "null_spec.yaml",
                "null_chain.yaml",
                ".gitignore",
                "Makefile",
                "requirements.txt",
                "scripts/aggregate_chain_output.sh",
                "scripts/aggregate_output.sh",
                "scripts/check_completion.sh",
                "scripts/kill_all.sh",
                "scripts/launch_chain_job.py",
                "scripts/launch_jobs.py",
                "scripts/make_samples.py",
                "scripts/read_output_chain.py",
                "scripts/read_output.py",
                "scripts/search.sh",
                "scripts/submit_chain.sbatch",
                "scripts/submit.sbatch",
            ],
            "null_spec",
        ),
        (
            "openfoam_wf",
            [
                "openfoam_wf.yaml",
                "openfoam_wf_docker_template.yaml",
                "README.md",
                "requirements.txt",
                "scripts/make_samples.py",
                "scripts/blockMesh_template.txt",
                "scripts/cavity_setup.sh",
                "scripts/combine_outputs.py",
                "scripts/learn.py",
                "scripts/mesh_param_script.py",
                "scripts/run_openfoam",
            ],
            "openfoam_wf",
        ),
        (
            "openfoam_wf_no_docker",
            [
                "openfoam_wf_no_docker.yaml",
                "openfoam_wf_no_docker_template.yaml",
                "requirements.txt",
                "scripts/make_samples.py",
                "scripts/blockMesh_template.txt",
                "scripts/cavity_setup.sh",
                "scripts/combine_outputs.py",
                "scripts/learn.py",
                "scripts/mesh_param_script.py",
                "scripts/run_openfoam",
            ],
            "openfoam_wf_no_docker",
        ),
        (
            "openfoam_wf_singularity",
            [
                "openfoam_wf_singularity.yaml",
                "openfoam_wf_singularity_template.yaml",
                "requirements.txt",
                "scripts/make_samples.py",
                "scripts/blockMesh_template.txt",
                "scripts/cavity_setup.sh",
                "scripts/combine_outputs.py",
                "scripts/learn.py",
                "scripts/mesh_param_script.py",
                "scripts/run_openfoam",
            ],
            "openfoam_wf_singularity",
        ),
        (
            "optimization_basic",
            [
                "optimization_basic.yaml",
                "requirements.txt",
                "template_config.py",
                "template_optimization.temp",
                "scripts/collector.py",
                "scripts/optimizer.py",
                "scripts/test_functions.py",
                "scripts/visualizer.py",
            ],
            "optimization",
        ),
        (
            "remote_feature_demo",
            [
                ".gitignore",
                "remote_feature_demo.yaml",
                "requirements.txt",
                "scripts/features.json",
                "scripts/hello_world.py",
                "scripts/pgen.py",
            ],
            "remote_feature_demo",
        ),
        ("restart", ["restart.yaml", "scripts/make_samples.py"], "restart"),
        ("restart_delay", ["restart_delay.yaml", "scripts/make_samples.py"], "restart_delay"),
    ],
)
def test_setup_example(examples_testing_dir: str, example_name: str, example_files: List[str], expected_return: str):
    """
    Run tests for the `setup_example` function.
    Each test will consist of:
    1. The name of the example to setup
    2. A list of files that we're expecting to be setup
    3. The expected return value
    Each test is a tuple in the parametrize decorator above this test function.

    :param examples_testing_dir: The path to the the temp output directory for examples tests
    :param example_name: The name of the example to setup
    :param example_files: A list of filenames that should be copied by setup_example
    :param expected_return: The expected return value from `setup_example`
    """
    # Create the temp path to store this setup
    setup_example_dir = os.path.join(examples_testing_dir, f"setup_{example_name}")

    # Ensure that the example name is returned
    actual = setup_example(example_name, setup_example_dir)
    assert actual == expected_return

    # Ensure all of the files that should've been copied were copied
    expected_files = [os.path.join(setup_example_dir, expected_file) for expected_file in example_files]
    for file in expected_files:
        assert os.path.exists(file)


def test_setup_example_simple_chain(examples_testing_dir: str):
    """
    Test the `setup_example` function for the simple_chain example.
    This example just writes a single file so we can't run it in the `test_setup_example` test.

    :param examples_testing_dir: The path to the the temp output directory for examples tests
    """

    # Create the temp path to store this setup
    output_file = os.path.join(examples_testing_dir, "simple_chain.yaml")

    # Ensure that the example name is returned
    actual = setup_example("simple_chain", output_file)
    assert actual == "simple_chain"
    assert os.path.exists(output_file)
