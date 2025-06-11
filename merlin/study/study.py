##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module represents all of the logic for a study"""

import logging
import os
import shutil
import subprocess
import time
from contextlib import suppress
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
from cached_property import cached_property
from maestrowf.datastructures.core import Study
from maestrowf.datastructures.core.studyenvironment import StudyEnvironment
from maestrowf.maestro import load_parameter_generator
from maestrowf.utils import create_dictionary

from merlin.common.enums import ReturnCode
from merlin.spec import defaults
from merlin.spec.expansion import determine_user_variables, expand_by_line, expand_env_vars, expand_line
from merlin.spec.override import error_override_vars, replace_override_vars
from merlin.spec.specification import MerlinSpec
from merlin.study.dag import DAG
from merlin.utils import contains_shell_ref, contains_token, get_flux_cmd, load_array_file


LOG = logging.getLogger(__name__)


# TODO: see if there's any way to split this class up
#       (pylint doesn't like how many attributes and public methods there are)
# - Might be able to create an object to store files and handle file modifications
# - If we don't want to create entirely new classes we could try grouping args into dicts
class MerlinStudy:  # pylint: disable=R0902,R0904
    """
    Represents a Merlin study run on a specification. Used for 'merlin run'.

    This class manages the execution of a study based on a provided specification file,
    handling sample data, output paths, workspace management, and the generation of a
    Directed Acyclic Graph (DAG) for execution.

    Attributes:
        dag (study.dag.DAG): Directed acyclic graph representing the execution flow of the study.
        dry_run (bool): Flag indicating whether to perform a dry run of the workflow.
        expanded_spec (spec.specification.MerlinSpec): The expanded specification after applying overrides.
        filepath (str): Path to the desired specification file.
        flux_command (str): Command for running flux jobs, if applicable.
        info (str): Path to the 'merlin_info' directory within the workspace.
        level_max_dirs (int): The number of directories at each level of the sample hierarchy.
        no_errors (bool): Flag to ignore some errors for testing purposes.
        original_spec (spec.specification.MerlinSpec): The original specification loaded
            from the filepath.
        output_path (str): Path to the output directory for the study.
        override_vars (Dict[str, Union[str, int]]): Dictionary of variables to override in the specification.
        parameter_labels (List[str]): List of parameter labels used in the study.
        pargs (List[str]): Arguments for the parameter generator.
        pgen_file (str): Filepath for the parameter generator, if applicable.
        restart_dir (str): Filepath to restart the study, if applicable.
        sample_labels (List[str]): The column labels of the samples.
        samples (np.ndarray): The samples in the study.
        samples_file (str): File to load samples from, if specified.
        special_vars (Dict[str, str]): Dictionary of special variables used in the study.
        timestamp (str): Timestamp representing the start time of the study.
        user_vars (Dict[str, str]): The user-defined variables in the study.
        workspace (str): Path to the workspace directory for the study.

    Methods:
        generate_samples: Executes a command to generate sample data if the sample file is missing.
        get_adapter_config: Builds and returns the adapter configuration dictionary.
        get_expanded_spec: Returns a new YAML spec file with defaults, CLI overrides, and variable expansions.
        get_sample_labels: Retrieves the column labels for the samples.
        get_user_vars: Returns a dictionary of expanded user-defined variables from the specification.
        label_clash_error: Checks for clashes between sample and parameter names.
        load_dag: Generates a Directed Acyclic Graph (DAG) for the study's execution.
        load_pgen: Executes a parameter generator script.
        load_samples: Loads samples from disk or generates them if the file does not exist.
        write_original_spec: Copies the original specification to the 'merlin_info' directory.
    """

    def __init__(  # pylint: disable=R0913
        self,
        filepath: str,
        override_vars: Dict[str, Union[str, int]] = None,
        restart_dir: str = None,
        samples_file: str = None,
        dry_run: bool = False,
        no_errors: bool = False,
        pgen_file: str = None,
        pargs: List[str] = None,
    ):
        """
        Initializes a MerlinStudy object, which represents a study run based on a specification file.

        Args:
            filepath: Path to the specification file for the study.
            override_vars: Dictionary of variables to override in the specification.
            restart_dir: Path to the directory for restarting the study.
            samples_file: Path to a file containing sample data. If specified, the samples
                will be loaded from this file.
            dry_run: Flag indicating whether to perform a dry run of the workflow
                without executing tasks.
            no_errors: Flag to suppress certain errors for testing purposes.
            pgen_file: Path to a parameter generator file.
            pargs: Arguments for the parameter generator.
        """
        self.filepath: str = filepath
        self.original_spec: MerlinSpec = MerlinSpec.load_specification(filepath)
        self.override_vars: Dict = override_vars
        error_override_vars(self.override_vars, self.original_spec.path)

        self.samples_file: str = samples_file
        self.label_clash_error()
        self.dry_run: bool = dry_run
        self.no_errors: bool = no_errors

        # If we load from a file, record that in the object for provenance
        # downstream
        if self.samples_file is not None:
            self.original_spec.merlin["samples"]["file"] = self.samples_file
            self.original_spec.merlin["samples"]["generate"]["cmd"] = ""

        self.restart_dir: str = restart_dir

        self.special_vars: Dict[str, str] = {
            "SPECROOT": self.original_spec.specroot,
            "MERLIN_TIMESTAMP": self.timestamp,
            "MERLIN_INFO": self.info,
            "MERLIN_WORKSPACE": self.workspace,
            "OUTPUT_PATH": self.output_path,
            "MERLIN_SUCCESS": str(int(ReturnCode.OK)),
            "MERLIN_RESTART": str(int(ReturnCode.RESTART)),
            "MERLIN_SOFT_FAIL": str(int(ReturnCode.SOFT_FAIL)),
            "MERLIN_HARD_FAIL": str(int(ReturnCode.HARD_FAIL)),
            "MERLIN_RETRY": str(int(ReturnCode.RETRY)),
            # below will be substituted for sample values on execution
            "MERLIN_SAMPLE_VECTOR": " ".join([f"$({k})" for k in self.get_sample_labels(from_spec=self.original_spec)]),
            "MERLIN_SAMPLE_NAMES": " ".join(self.get_sample_labels(from_spec=self.original_spec)),
        }
        self._set_special_file_vars()

        self.pgen_file: str = pgen_file
        self.pargs: List[str] = pargs

        self.dag: DAG = None
        self.load_dag()

    def _set_special_file_vars(self):
        """
        Sets the original, partial, and expanded file paths for a study.

        This method constructs file paths for three special variables
        related to the study's specifications. It generates paths for
        the original template, the executed run, and the archived copy
        of the specification files based on the study's base file name.
        """
        shortened_filepath = self.filepath.replace(".out", "").replace(".partial", "").replace(".expanded", "")
        base_name = Path(shortened_filepath).stem
        self.special_vars["MERLIN_SPEC_ORIGINAL_TEMPLATE"] = os.path.join(
            self.info,
            base_name + ".orig.yaml",
        )
        self.special_vars["MERLIN_SPEC_EXECUTED_RUN"] = os.path.join(
            self.info,
            base_name + ".partial.yaml",
        )
        self.special_vars["MERLIN_SPEC_ARCHIVED_COPY"] = os.path.join(
            self.info,
            base_name + ".expanded.yaml",
        )

    def write_original_spec(self):
        """
        Copies the original specification file to the designated directory.

        This method copies the original specification file from its
        current location to the `merlin_info/` directory, renaming it
        to '<base_file_name>.orig.yaml'. The base file name is derived
        from the original specification's path.
        """
        shutil.copyfile(self.original_spec.path, self.special_vars["MERLIN_SPEC_ORIGINAL_TEMPLATE"])

    def label_clash_error(self):
        """
        Detects illegal clashes between Merlin's sample column labels and
        [Maestro's global parameters](https://maestrowf.readthedocs.io/en/latest/Maestro/specification.html#parameters-globalparameters).

        This method checks for any conflicts between the column labels
        defined in the `merlin` section of the original specification and
        the global parameters defined in the same specification. If a
        column label is found to also exist in the global parameters,
        a ValueError is raised to indicate the clash.

        Raises:
            ValueError: If any column label in `merlin.samples.column_labels`
                is also found in `merlin.globals`, indicating an illegal clash.
        """
        if self.original_spec.merlin["samples"]:
            for label in self.original_spec.merlin["samples"]["column_labels"]:
                if label in self.original_spec.globals:
                    raise ValueError(f"column_label {label} cannot also be in global.parameters!")

    # There's similar code inside expansion.py but the whole point of the function inside that file is
    # to not use the MerlinStudy object so we disable this pylint error
    # pylint: disable=duplicate-code
    @staticmethod
    def get_user_vars(spec: MerlinSpec) -> Dict[str, str]:
        """
        Retrieves and expands user-defined variables from the specification environment.

        This static method examines the provided specification's environment
        to collect user-defined variables and labels. It constructs a list
        of these variables and passes them to the `determine_user_variables`
        function to obtain a dictionary of expanded variables.

        Args:
            spec (spec.specification.MerlinSpec): The specification object containing the environment from which
                to extract user-defined variables. The environment should have keys
                "variables" and/or "labels" that contain the relevant data.

        Returns:
            A dictionary of expanded user-defined variables, where the keys
                are variable names and the values are their corresponding
                expanded values.
        """
        uvars = []
        if "variables" in spec.environment:
            uvars.append(spec.environment["variables"])
        if "labels" in spec.environment:
            uvars.append(spec.environment["labels"])
        return determine_user_variables(*uvars)

    # pylint: enable=duplicate-code

    @property
    def user_vars(self) -> Dict[str, str]:
        """
        Retrieves the user-defined variables for the study.

        This property accesses the original specification of the study and
        retrieves the user-defined variables using the `get_user_vars`
        method from this class.

        Returns:
            A dictionary containing the user-defined variables
                associated with the study.
        """
        return MerlinStudy.get_user_vars(self.original_spec)

    def get_expanded_spec(self) -> MerlinSpec:
        """
        Generates a new YAML specification file with applied defaults,
        command-line interface (CLI) overrides, and variable expansions.

        This method creates a modified version of the original specification
        by incorporating default values and user-defined overrides from the
        command line. It also expands user-defined variables and reserved
        words to produce a fully resolved specification. This is particularly
        useful for tracking provenance and ensuring that the specification
        accurately reflects all applied configurations.

        Returns:
            (spec.specification.MerlinSpec): A new instance of the [`MerlinSpec`][spec.specification.MerlinSpec]
                class that contains the fully expanded specification.
        """
        # get specification including defaults and cli-overridden user variables
        new_env = replace_override_vars(self.original_spec.environment, self.override_vars)
        new_spec = deepcopy(self.original_spec)
        new_spec.environment = new_env

        # expand user variables
        new_spec_text = expand_by_line(new_spec.dump(), MerlinStudy.get_user_vars(new_spec))

        # expand reserved words
        new_spec_text = expand_by_line(new_spec_text, self.special_vars)

        result = MerlinSpec.load_spec_from_string(new_spec_text)
        return expand_env_vars(result)

    @property
    def samples(self) -> np.ndarray:
        """
        Retrieves the samples associated with this study.

        This property checks if there are any samples defined in the
        expanded specification of the study. If samples are present,
        it loads and returns them; otherwise, it returns an empty list.

        Returns:
            A numpy array of samples corresponding to the study.
                If no samples are defined, an empty list is returned.
        """

        if self.expanded_spec.merlin["samples"]:
            return self.load_samples()
        return []

    def get_sample_labels(self, from_spec: MerlinSpec) -> List[str]:
        """
        Retrieves the column labels of the samples from the provided specification.

        This method checks the specified [`MerlinSpec`][spec.specification.MerlinSpec]
        object for sample information and returns the associated column labels if they
        exist. If no sample labels are found, an empty list is returned.

        Args:
            from_spec (spec.specification.MerlinSpec): The specification object
                from which to extract sample column labels. It is expected to contain
                a "samples" key within its "merlin" dictionary.

        Returns:
            A list of column labels for the samples. If no sample labels are
                present, an empty list is returned.
        """
        if from_spec.merlin["samples"]:
            return from_spec.merlin["samples"]["column_labels"]
        return []

    @property
    def sample_labels(self) -> List[str]:
        """
        Retrieves the labels of the samples associated with this study.

        This property extracts the sample labels from the study's
        expanded specification. It returns a list of labels that
        correspond to the samples defined in the specification.

        Returns:
            A list of sample labels. If no labels are defined, an empty list is returned.

        Example:
            Given the following contents in a specification file:

            ```yaml
            merlin:
                samples:
                    column_labels: [X0, X1]
            ```

            This property would return: `["X0", "X1"]`
        """
        return self.get_sample_labels(from_spec=self.expanded_spec)

    def load_samples(self) -> np.ndarray:
        """
        Loads the study's samples from disk, generating them if the file
        does not exist and is defined in the YAML specification.

        This method checks if a sample file is specified in the expanded
        specification. If the file does not exist, it will invoke the
        generation command defined in the 'generate' section of the
        specification to create the sample file. Once the file is available,
        it loads the samples into a NumPy array and assigns them to the
        variables specified in 'column_labels'.

        Returns:
            A NumPy array containing the loaded samples. The shape of the
                array will be (n_samples, n_features), where n_samples is
                the number of samples loaded and n_features is the number
                of features corresponding to the column labels.

        Example:
            The spec file contents will look something like:

            ```yaml
            merlin:
                samples:
                    generate:
                        cmd: python make_samples.py -outfile=samples.npy
                    file: samples.npy
                    column_labels: [X0, X1]
            ```
        """
        if self.samples_file is None:
            if self.expanded_spec.merlin["samples"]:
                self.samples_file = self.expanded_spec.merlin["samples"]["file"]
                # generates the samples if the file does not exist
                self.generate_samples()

        LOG.info(f"Loading samples from '{os.path.basename(self.samples_file)}'...")
        samples = load_array_file(self.samples_file, ndmin=2)
        nsamples = samples.shape[0]
        nfeatures = samples.shape[1]
        if nfeatures != len(self.sample_labels):
            LOG.warning(
                (
                    f"Number of columns in '{self.samples_file}' ({nfeatures}) "
                    f"doesn't match the number of column labels "
                    f"in spec file ({len(self.sample_labels)})"
                )
            )
        if nsamples == 1:
            LOG.info(f"{nsamples} sample loaded.")
        else:
            LOG.info(f"{nsamples} samples loaded.")
        return samples

    @property
    def level_max_dirs(self) -> int:
        """
        Retrieves the maximum number of directory levels for sample organization.

        This property checks the expanded specification for the maximum
        number of directory levels defined under the 'merlin' section.
        If the value is not found, it falls back to a default value
        specified in the `defaults.SAMPLES` dictionary.

        Returns:
            The maximum number of directory levels. If the value is
                not specified in the expanded specification, the default
                value from `defaults.SAMPLES["level_max_dirs"]` is returned.
        """
        with suppress(TypeError, KeyError):
            return self.expanded_spec.merlin["samples"]["level_max_dirs"]
        return defaults.SAMPLES["level_max_dirs"]

    @cached_property
    def output_path(self) -> str:
        """
        Determines and creates an output directory for this study.

        This property checks if a restart directory is specified. If so, it validates
        the existence of the directory and returns its absolute path. If no restart
        directory is provided, it constructs the output path based on the original
        specification and any override variables. The output path is expanded to
        include user-defined variables and environment variables. If the directory
        does not exist, it is created.

        Returns:
            The absolute path to the output directory for the study.

        Raises:
            ValueError: If the specified restart directory does not exist.
        """
        if self.restart_dir is not None:
            output_path = self.restart_dir
            if not os.path.isdir(output_path):
                raise ValueError(f"Restart dir '{self.restart_dir}' does not exist!")
            return os.path.abspath(output_path)

        output_path = str(self.original_spec.output_path)

        # If there are override vars we need to check that the output path doesn't need changed
        if self.override_vars is not None:
            # Case where output path is directly modified
            if "OUTPUT_PATH" in self.override_vars:
                output_path = str(self.override_vars["OUTPUT_PATH"])
            else:
                for var_name, var_val in self.override_vars.items():
                    token = f"$({var_name})"
                    # Case where output path contains a variable that was overridden
                    if token in output_path:
                        output_path = output_path.replace(token, str(var_val))

        output_path = expand_line(output_path, self.user_vars, env_vars=True)
        output_path = os.path.abspath(output_path)
        if not os.path.isdir(output_path):
            os.makedirs(output_path)
            LOG.info(f"Made dir(s) to output path '{output_path}'.")

        LOG.info(f"OUTPUT_PATH: {os.path.basename(output_path)}")

        return output_path

    @cached_property
    def timestamp(self) -> str:
        """
        Returns a timestamp string representing the time this study began.

        This property generates a unique identifier based on the current time
        when the study is initiated. If a restart directory is specified, it
        extracts a substring from the directory name as the timestamp. Otherwise,
        it formats the current time in the 'YYYYMMDD-HHMMSS' format.

        Returns:
            A string representing the timestamp of the study's initiation,
                which can be used as an identifier or unique key.
        """
        if self.restart_dir is not None:
            return self.restart_dir.strip("/")[-15:]
        return time.strftime("%Y%m%d-%H%M%S")

    # TODO look into why pylint complains that this method is hidden
    # - might be because we reset self.workspace's value in the expanded_spec method
    @cached_property
    def workspace(self) -> str:  # pylint: disable=E0202
        """
        Determines, creates, and returns the path to this study's workspace directory.

        This property generates a unique workspace directory for the study, which
        contains subdirectories for each step of the study and a 'merlin_info/'
        directory. The name of the workspace directory is derived from the original
        specification name and includes a timestamp to ensure uniqueness. If a
        restart directory is specified, it validates the existence of the directory
        and returns its absolute path.

        Returns:
            The absolute path to the workspace directory for the study.

        Raises:
            ValueError: If the specified restart directory does not exist.
        """
        if self.restart_dir is not None:
            if not os.path.isdir(self.restart_dir):
                raise ValueError(f"Restart directory '{self.restart_dir}' does not exist!")
            return os.path.abspath(self.restart_dir)

        workspace_name = f'{self.original_spec.name.replace(" ", "_")}_{self.timestamp}'
        workspace = os.path.join(self.output_path, workspace_name)
        with suppress(FileNotFoundError):
            shutil.rmtree(workspace)
        os.mkdir(workspace)

        return workspace

    # TODO look into why pylint complains that this method is hidden
    # - might be because we reset self.info's value in the expanded_spec method
    @cached_property
    def info(self) -> str:  # pylint: disable=E0202
        """
        Creates and returns the path to the 'merlin_info' directory within the study's workspace.

        This property checks if a restart directory is specified. If not, it creates
        the 'merlin_info' directory inside the study's workspace directory. This
        directory is intended to store metadata and other relevant information related
        to the study.

        Returns:
            The absolute path to the 'merlin_info' directory.
        """
        info_name = os.path.join(self.workspace, "merlin_info")
        if self.restart_dir is None:
            os.mkdir(info_name)
        return info_name

    @cached_property
    def expanded_spec(self) -> MerlinSpec:
        """
        Determines, writes to YAML, and loads into memory an expanded specification.

        This property handles the expansion of the study's specification based on
        the original specification and any provided environment variables. If the
        study is being restarted, it retrieves the previously expanded specification
        without re-expanding it. Otherwise, it processes the original specification,
        expands any tokens or shell references, and updates paths accordingly.

        Returns:
            (spec.specification.MerlinSpec): The expanded specification object.

        Raises:
            ValueError: If the expanded name for the workspace contains invalid
                characters for a filename.
        """
        # If we are restarting, we don't need to re-expand, just need to read
        # in the previously expanded spec
        if self.restart_dir is not None:
            return self.get_expanded_spec()

        result = self.get_expanded_spec()

        # expand provenance spec filename
        if contains_token(self.original_spec.name) or contains_shell_ref(self.original_spec.name):
            name = f"{result.description['name'].replace(' ', '_')}_{self.timestamp}"
            name = expand_line(name, {}, env_vars=True)
            if "/" in name:
                raise ValueError(f"Expanded value '{name}' for field 'name' in section 'description' is not a valid filename.")
            expanded_workspace = os.path.join(self.output_path, name)

            if result.merlin["samples"]:
                sample_file = result.merlin["samples"]["file"]
                if sample_file.startswith(self.workspace):
                    new_samples_file = sample_file.replace(self.workspace, expanded_workspace)
                    result.merlin["samples"]["generate"]["cmd"] = result.merlin["samples"]["generate"]["cmd"].replace(
                        self.workspace, expanded_workspace
                    )
                    result.merlin["samples"]["file"] = new_samples_file

            shutil.move(self.workspace, expanded_workspace)
            self.workspace = expanded_workspace
            self.info = os.path.join(self.workspace, "merlin_info")
            self.special_vars["MERLIN_INFO"] = self.info
            self._set_special_file_vars()

            new_spec_text = expand_by_line(result.dump(), MerlinStudy.get_user_vars(result))
            result = MerlinSpec.load_spec_from_string(new_spec_text)
            result = expand_env_vars(result)

        # pgen
        if self.pgen_file:
            env = result.get_study_environment()
            result.globals = self.load_pgen(self.pgen_file, self.pargs, env)

        # copy the --samplesfile (if any) into merlin_info
        if self.samples_file:
            shutil.copyfile(
                self.samples_file,
                os.path.join(self.info, os.path.basename(self.samples_file)),
            )

        # write expanded spec for provenance and set the path (necessary for testing)
        with open(self.special_vars["MERLIN_SPEC_ARCHIVED_COPY"], "w") as f:  # pylint: disable=C0103
            f.write(result.dump())
        result.path = self.special_vars["MERLIN_SPEC_ARCHIVED_COPY"]

        # write original spec for provenance
        self.write_original_spec()

        # write partially-expanded spec for provenance
        partial_spec = deepcopy(self.original_spec)
        if "variables" in result.environment:
            partial_spec.environment["variables"] = result.environment["variables"]
        if "labels" in result.environment:
            partial_spec.environment["labels"] = result.environment["labels"]
        with open(self.special_vars["MERLIN_SPEC_EXECUTED_RUN"], "w") as f:  # pylint: disable=C0103
            f.write(partial_spec.dump())

        LOG.info(f"Study workspace is '{self.workspace}'.")
        return result

    @cached_property
    def flux_command(self) -> str:
        """
        Returns the full path to the flux command based on the specified workflow configuration.

        This property constructs the command to execute the flux binary. If a
        `flux_path` is provided in the expanded specification's batch configuration,
        it will use that path to create the full command. Otherwise, it defaults
        to the standard 'flux' command.

        Returns:
            The complete command string for executing flux.
        """
        flux_bin = "flux"
        if "flux_path" in self.expanded_spec.batch.keys():
            flux_bin = os.path.join(self.expanded_spec.batch["flux_path"], "flux")
        return get_flux_cmd(flux_bin, no_errors=self.no_errors)

    def generate_samples(self):
        """
        Generates sample data by executing the command defined in the
        'generate' section of the specification if the sample file does
        not already exist.

        This method checks if the specified sample file exists. If it
        does not, it retrieves the command from the YAML specification
        and executes it using a subprocess. The output and error logs
        from the command execution are saved to files for later review.

        Example:
            Here's an example sample generation command:

            ```yaml
            merlin:
                samples:
                    generate:
                        cmd: python make_samples.py -outfile=samples.npy
            ```
        """
        try:
            if not os.path.exists(self.samples_file):
                sample_generate = self.expanded_spec.merlin["samples"]["generate"]["cmd"]
                LOG.info("Generating samples...")
                sample_process = subprocess.Popen(  # pylint: disable=R1732
                    sample_generate,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    shell=True,
                )
                stdout, stderr = sample_process.communicate()
                with open(os.path.join(self.info, "cmd.sh"), "w") as f:  # pylint: disable=C0103
                    f.write(sample_generate)
                with open(os.path.join(self.info, "cmd.out"), "wb") as f:  # pylint: disable=C0103
                    f.write(stdout)
                with open(os.path.join(self.info, "cmd.err"), "wb") as f:  # pylint: disable=C0103
                    f.write(stderr)
                LOG.info("Generating samples complete!")
            return
        except (IndexError, TypeError) as e:  # pylint: disable=C0103
            LOG.error(f"Could not generate samples:\n{e}")
            return

    def load_pgen(self, filepath: str, pargs: List[str], env: StudyEnvironment) -> Dict[str, Dict[str, str]]:
        """
        Loads a parameter generator script and creates a dictionary of
        variable names and their corresponding values.

        This method reads a parameter generator script from the specified
        file path and extracts variable names and values defined within
        the script. It constructs a dictionary where each key is a
        variable name, and the value is another dictionary containing
        the variable's label and its associated values.

        Args:
            filepath: The path to the parameter generator script to be loaded.
            pargs: A list of additional arguments to be passed to the parameter
                generator. If None, an empty list will be used.
            env: A Maestro
                [`StudyEnvironment`](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/datastructures/core/studyenvironment.html)
                object containing custom information.

        Returns:
            A dictionary where each key is a variable name and each value
                is a dictionary containing:\n
                - `values`: The values associated with the variable,
                    or None if not defined.
                - `label`: The label of the variable as defined in the
                    parameter generator script.
        """
        if filepath:
            if pargs is None:
                pargs = []
            kwargs = create_dictionary(pargs)
            params = load_parameter_generator(filepath, env, kwargs)
            result = {}
            for key, val in params.labels.items():
                result[key] = {"values": None, "label": val}
            for key, val in params.parameters.items():
                result[key]["values"] = val
            return result
        return None

    def load_dag(self):
        """
        Generates a Directed Acyclic Graph (DAG) for the execution of
        the study and assigns it to the `self.dag` attribute.

        This method constructs a DAG based on the specifications defined
        in the expanded study specification. It retrieves the study
        environment, steps, and parameters, and initializes a Maestro
        [`Study`](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/datastructures/core/study.html)
        object. The method then sets up the workspace and environment
        for the study, configures it, and generates the DAG using the
        Maestro framework.

        The generated DAG contains the execution flow of the study,
        ensuring that all steps are executed in the correct order
        without cycles.
        """
        environment = self.expanded_spec.get_study_environment()
        steps = self.expanded_spec.get_study_steps()

        parameters = self.expanded_spec.get_parameters()

        # Setup the study.
        study = Study(
            self.expanded_spec.name,
            self.expanded_spec.description,
            studyenv=environment,
            parameters=parameters,
            steps=steps,
            out_path=self.workspace,
        )

        # Prepare the maestro study
        if self.restart_dir is None:
            study.setup_workspace()

        study.setup_environment()
        study.configure_study(
            throttle=0,
            submission_attempts=1,
            restart_limit=0,
            use_tmp=None,
            hash_ws=None,
        )

        # Generate the DAG
        _, maestro_dag = study.stage()
        column_labels = []
        if self.expanded_spec.merlin["samples"]:
            column_labels = self.expanded_spec.merlin["samples"]["column_labels"]
        parameter_info = {
            "labels": self.parameter_labels,
            "step_param_map": self.expanded_spec.get_step_param_map(),
        }
        # To avoid pickling issues with _pass_detect_cycle from maestro, we unpack the dag here
        self.dag = DAG(maestro_dag.adjacency_table, maestro_dag.values, column_labels, study.name, parameter_info)

    def get_adapter_config(self, override_type: str = None) -> Dict[str, str]:
        """
        Builds and returns the adapter configuration dictionary.

        This method constructs a configuration dictionary for the adapter
        based on the specifications defined in `self.expanded_spec.batch`.
        It ensures that the configuration includes a type, which can be
        overridden if specified. The method also checks for a dry run
        flag and adds relevant commands if the batch type is set to
        "flux".

        Args:
            override_type: An optional string to override the default adapter
                type. If not provided, the type from the expanded specification
                will be used.

        Returns:
            A dictionary containing the adapter configuration.
        """
        adapter_config = dict(self.expanded_spec.batch)

        if "type" not in adapter_config.keys():
            adapter_config["type"] = "local"

        # The type may be overriden, preserve the batch type
        adapter_config["batch_type"] = adapter_config["type"]

        if override_type is not None:
            adapter_config["type"] = override_type

        # if a dry run was ordered by the yaml spec OR the cli flag, do a dry run.
        adapter_config["dry_run"] = self.dry_run or adapter_config["dry_run"]

        # Add the version if using flux to switch the command in the step
        if adapter_config["batch_type"] == "flux":
            adapter_config["flux_command"] = self.flux_command

        LOG.debug(f"Adapter config = {adapter_config}")
        return adapter_config

    @property
    def parameter_labels(self) -> List[str]:
        """
        Retrieves the parameter labels associated with this study.

        This property extracts parameter labels from the expanded specification
        of the study. It accesses the parameters and their associated metadata,
        collecting all labels defined for each parameter.

        Returns:
            A list of parameter labels used in this study.
        """
        parameters = self.expanded_spec.get_parameters()
        metadata = parameters.get_metadata()

        param_labels = []
        for parameter_info in metadata.values():
            for parameter_label in parameter_info["labels"].values():
                param_labels.append(parameter_label)

        return param_labels
