"""
This module will contain the testing logic
for the `merlin run` command.
"""
import csv
import os
import re
import subprocess
from typing import Tuple

from redis import Redis

from merlin.spec.specification import MerlinSpec
from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd


class TestRunCommand:
    """
    Base class for testing the `merlin run` command.
    """

    demo_workflow = os.path.join("examples", "workflows", "feature_demo", "feature_demo.yaml")

    def setup_test_environment(self, merlin_server_dir: str, run_command_testing_dir: str):
        """
        Setup the test environment for these tests by:
        1. Moving into the temporary output directory created specifically for these tests.
        2. Copying the app.yaml file created by the `redis_server` fixture to the cwd so that
           Merlin can connect to the test server.

        Args:
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            run_command_testing_dir:
                The path to the the temp output directory for `merlin run` tests.
        """
        os.chdir(run_command_testing_dir)
        copy_app_yaml_to_cwd(merlin_server_dir)

    def run_merlin_command(self, command: str) -> subprocess.CompletedProcess:
        """
        Open a subprocess and run the command specified by the `command` parameter.
        Ensure this command runs successfully and return the process results.

        Args:
            command: The command to execute in a subprocess.

        Returns:
            The results from executing the command in a subprocess.

        Raises:
            AssertionError: If the command fails (non-zero return code).
        """
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        assert result.returncode == 0, f"Command failed with return code {result.returncode}. Output: {result.stdout} Error: {result.stderr}"
        return result

    def get_output_workspace_from_logs(self, stdout_logs: str, stderr_logs: str) -> str:
        """
        Extracts the workspace path from the provided standard output and error logs.

        This method searches for a specific message indicating the study workspace 
        in the combined logs (both stdout and stderr). The expected message format 
        is: "Study workspace is '<workspace_path>'". If the message is found, 
        the method returns the extracted workspace path. If the message is not 
        found, an assertion error is raised.

        Args:
            stdout_logs: The standard output logs as a string.
            stderr_logs: The standard error logs as a string.

        Returns:
            The extracted workspace path from the logs.

        Raises:
            AssertionError: If the expected message is not found in the combined logs.
        """
        workspace_pattern = re.compile(r"Study workspace is '(\S+)'")
        combined_output = stdout_logs + stderr_logs
        match = workspace_pattern.search(combined_output)
        assert match, "No 'Study workspace is...' message found in command output."
        return match.group(1)

    def validate_workspace(self, result: subprocess.CompletedProcess) -> str:
        """
        Validate the workspace path extracted from the command output logs.

        This method retrieves the expected workspace path from the standard output and error logs
        of the executed command. It checks if the workspace path exists in the filesystem.

        Args:
            result: The result of the executed command, containing stdout and stderr.

        Returns:
            The path to the expected workspace.

        Raises:
            AssertionError: If the expected workspace path is not found in the filesystem.
        """
        expected_workspace_path = self.get_output_workspace_from_logs(result.stdout, result.stderr)
        assert os.path.exists(expected_workspace_path), f"Expected workspace not found: {expected_workspace_path}"
        return expected_workspace_path


class TestRunCommandDistributed(TestRunCommand):
    """
    Tests for the `merlin run` command that are run in a distributed manner
    rather than being run locally.
    """

    def test_distributed_run(
        self,
        redis_client: Redis,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_merlin_codebase: str,
        merlin_server_dir: str,
        run_command_testing_dir: str,
    ):
        """
        This test verifies that tasks can be successfully sent to a Redis server
        using the `merlin run` command with no flags.

        Args:
            redis_client:
                A fixture that connects us to a redis client that we can interact with.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            run_command_testing_dir:
                The path to the the temp output directory for `merlin run` tests.
        """
        from merlin.celery import app as celery_app

        # Setup the testing environment
        feature_demo = os.path.join(path_to_merlin_codebase, self.demo_workflow)
        self.setup_test_environment(merlin_server_dir, run_command_testing_dir)

        with CeleryTaskManager(celery_app, redis_client) as CTM:
            # Send tasks to the server
            self.run_merlin_command(f"merlin run {feature_demo} --vars NAME=run_command_test_distributed_run")

            # Get the queues we need to query
            spec = MerlinSpec.load_specification(feature_demo)
            queues_in_spec = spec.get_task_queues()

            for queue in queues_in_spec.values():
                # Brackets are special chars in regex so we have to add \ to make them literal
                queue = queue.replace("[", "\\[").replace("]", "\\]")
                matching_queues_on_server = redis_client.keys(pattern=f"{queue}*")

                # Make sure any queues that exist on the server have tasks in them
                for matching_queue in matching_queues_on_server:
                    tasks = redis_client.lrange(matching_queue, 0, -1)
                    assert len(tasks) > 0

    def test_samplesfile_option(
        self,
        redis_client: Redis,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_merlin_codebase: str,
        merlin_server_dir: str,
        run_command_testing_dir: str,
    ):
        """
        This test verifies that passing in a samples filepath from the command line will
        substitute in the file properly. It should copy the samples file that's passed
        in to the merlin_info subdirectory.

        Args:
            redis_client:
                A fixture that connects us to a redis client that we can interact with.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            run_command_testing_dir:
                The path to the the temp output directory for `merlin run` tests.
        """
        from merlin.celery import app as celery_app

        # Setup the testing environment
        feature_demo = os.path.join(path_to_merlin_codebase, self.demo_workflow)
        self.setup_test_environment(merlin_server_dir, run_command_testing_dir)

        # Create a new samples file to pass into our test workflow
        data = [
            ["X1, Value 1", "X2, Value 1"],
            ["X1, Value 2", "X2, Value 2"],
            ["X1, Value 3", "X2, Value 3"],
        ]
        sample_filename = "test_samplesfile.csv"
        new_samples_file = os.path.join(run_command_testing_dir, sample_filename)
        with open(new_samples_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)

        with CeleryTaskManager(celery_app, redis_client) as CTM:
            # Send tasks to the server
            result = self.run_merlin_command(f"merlin run {feature_demo} --vars NAME=run_command_test_samplesfile_option --samplesfile {new_samples_file}")

            # Check that the new samples file is written to the merlin_info directory
            expected_workspace_path = self.validate_workspace(result)
            assert os.path.exists(os.path.join(expected_workspace_path, "merlin_info", sample_filename))

    def test_pgen_and_pargs_options(
        self,
        redis_client: Redis,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_merlin_codebase: str,
        merlin_server_dir: str,
        run_command_testing_dir: str,
    ):
        """
        Test the `--pgen` and `--pargs` options with the `merlin run` command.
        This should update the parameter block of the expanded yaml file to have
        2 entries for both `X2` and `N_NEW`. The `X2` parameter should be between
        `X2_MIN` and `X2_MAX`, and the `N_NEW` parameter should be between `N_NEW_MIN`
        and `N_NEW_MAX`.

        Args:
            redis_client:
                A fixture that connects us to a redis client that we can interact with.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            run_command_testing_dir:
                The path to the the temp output directory for `merlin run` tests.
        """
        from merlin.celery import app as celery_app

        # Setup test vars and the testing environment
        feature_demo = os.path.join(path_to_merlin_codebase, self.demo_workflow)
        new_x2_min, new_x2_max = 1, 2
        new_n_new_min, new_n_new_max = 5, 15
        pgen_filepath = os.path.join(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.dirname(__file__)))), "pgen.py")
        self.setup_test_environment(merlin_server_dir, run_command_testing_dir)
        
        with CeleryTaskManager(celery_app, redis_client) as CTM:
            # Send tasks to the server
            result = self.run_merlin_command(
                f'merlin run {feature_demo} --vars NAME=run_command_test_pgen_and_pargs_options --pgen {pgen_filepath} --parg "X2_MIN:{new_x2_min}" --parg "X2_MAX:{new_x2_max}" --parg "N_NAME_MIN:{new_n_new_min}" --parg "N_NAME_MAX:{new_n_new_max}"'
            )

            # Check that an expanded yaml file exists
            expected_workspace_path = self.validate_workspace(result)
            merlin_info_dir = os.path.join(expected_workspace_path, "merlin_info")
            expanded_yaml = os.path.join(merlin_info_dir, "feature_demo.expanded.yaml")
            assert os.path.exists(expanded_yaml), f"Expected YAML file not found: {expanded_yaml}"

            # Read in the parameters from the expanded yaml and ensure they're within the new bounds we provided
            expanded_spec = MerlinSpec.load_specification(expanded_yaml)
            params = expanded_spec.get_parameters()
            for x2_param in params.parameters["X2"]:
                assert new_x2_min <= x2_param <= new_x2_max
            for n_new_param in params.parameters["N_NEW"]:
                assert new_n_new_min <= n_new_param <= new_n_new_max


class TestRunCommandLocal(TestRunCommand):
    """
    Tests for the `merlin run` command that are run in a locally rather
    than in a distributed manner.
    """

    def test_dry_run(
        self,
        redis_client: Redis,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_merlin_codebase: str,
        merlin_server_dir: str,
        run_command_testing_dir: str,
    ):
        """
        Test the `merlin run` command's `--dry` option. This should create all the output
        subdirectories for each step but it shouldn't execute anything for the steps. In
        other words, the only file in each step subdirectory should be the .sh file.

        Note:
            This test will run locally so that we don't have to worry about starting
            & stopping workers.

        Args:
            redis_client:
                A fixture that connects us to a redis client that we can interact with.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            run_command_testing_dir:
                The path to the the temp output directory for `merlin run` tests.
        """
        # Setup the test environment
        feature_demo = os.path.join(path_to_merlin_codebase, self.demo_workflow)
        self.setup_test_environment(merlin_server_dir, run_command_testing_dir)

        # Run the test and grab the output workspace generated from it
        result = self.run_merlin_command(f"merlin run {feature_demo} --vars NAME=run_command_test_dry_run --local --dry")
        expected_workspace_path = self.validate_workspace(result)

        # Check that every step was ran by looking for an existing output workspace
        spec = MerlinSpec.load_specification(feature_demo)
        for step in spec.get_study_steps():
            step_directory = os.path.join(expected_workspace_path, step.name)
            assert os.path.exists(step_directory), f"Output directory for step '{step.name}' not found: {step_directory}"

            allowed_dry_run_files = {"MERLIN_STATUS.json", "status.lock"}            
            for dirpath, dirnames, filenames in os.walk(step_directory):
                # Check if the current directory has no subdirectories (leaf directory)
                if not dirnames:
                    # Check for unexpected files
                    unexpected_files = [file for file in filenames if file not in allowed_dry_run_files and not file.endswith(".sh")]
                    assert not unexpected_files, f"Unexpected files found in {dirpath}: {unexpected_files}. Expected only .sh files or {allowed_dry_run_files}."

                    # Check that there is exactly one .sh file
                    sh_file_count = sum(1 for file in filenames if file.endswith(".sh"))
                    assert sh_file_count == 1, f"Expected exactly one .sh file in {dirpath} but found {sh_file_count} .sh files."

    def test_local_run(
        self,
        redis_client: Redis,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_merlin_codebase: str,
        merlin_server_dir: str,
        run_command_testing_dir: str,
    ):
        """
        This test verifies that tasks can be successfully executed locally using
        the `merlin run` command with the `--local` flag.

        Args:
            redis_client:
                A fixture that connects us to a redis client that we can interact with.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_merlin_codebase:
                A fixture to provide the path to the directory containing Merlin's core
                functionality.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            run_command_testing_dir:
                The path to the the temp output directory for `merlin run` tests.
        """
        # Setup the test environment
        feature_demo = os.path.join(path_to_merlin_codebase, self.demo_workflow)
        self.setup_test_environment(merlin_server_dir, run_command_testing_dir)

        # Run the test and grab the output workspace generated from it
        result = self.run_merlin_command(f"merlin run {feature_demo} --vars NAME=run_command_test_local_run --local")
        expected_workspace_path = self.validate_workspace(result)

        # Check that every step was ran by looking for an existing output workspace and MERLIN_FINISHED files
        spec = MerlinSpec.load_specification(feature_demo)
        for step in spec.get_study_steps():
            step_directory = os.path.join(expected_workspace_path, step.name)
            assert os.path.exists(step_directory), f"Output directory for step '{step.name}' not found: {step_directory}"
            for dirpath, dirnames, filenames in os.walk(step_directory):
                # Check if the current directory has no subdirectories (leaf directory)
                if not dirnames:
                    # Check for the existence of the MERLIN_FINISHED file
                    assert "MERLIN_FINISHED" in filenames, f"Expected a MERLIN_FINISHED file in list of files for {dirpath} but did not find one"

# TODO
# - commit these changes
# - do something similar for `merlin purge` tests