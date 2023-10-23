###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.1.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""This module represents all of the logic for a study"""

import logging
import os
import shutil
import subprocess
import time
from contextlib import suppress
from copy import deepcopy
from pathlib import Path

from cached_property import cached_property
from maestrowf.datastructures.core import Study
from maestrowf.maestro import load_parameter_generator
from maestrowf.utils import create_dictionary

from merlin.common.abstracts.enums import ReturnCode
from merlin.spec import defaults
from merlin.spec.expansion import determine_user_variables, expand_by_line, expand_env_vars, expand_line
from merlin.spec.override import error_override_vars, replace_override_vars
from merlin.spec.specification import MerlinSpec
from merlin.study.dag import DAG
from merlin.utils import contains_shell_ref, contains_token, get_flux_cmd, load_array_file


LOG = logging.getLogger(__name__)


# TODO: see if there's any way to split this class up (pylint doesn't like how many attributes there are)
# - Might be able to create an object to store files and handle file modifications
# - If we don't want to create entirely new classes we could try grouping args into dicts
class MerlinStudy:  # pylint: disable=R0902
    """
    Represents a Merlin study run on a specification. Used for 'merlin run'.

    :param `filepath`: path to the desired specification file.
    :param `override_vars`: Dictionary (keyword-variable name, value-variable
        value) to override in the spec.
    :param `restart_dir`: Filepath to restart study. If None, study runs
        normally.
    :param `samples_file`: File to load samples from. Ignores sample lookup
        and generation in the spec if set.
    :param `dry_run`: Flag to dry-run a workflow, which sets up the workspace but does not launch tasks.
    :param `no_errors`: Flag to ignore some errors for testing.
    """

    def __init__(  # pylint: disable=R0913
        self,
        filepath,
        override_vars=None,
        restart_dir=None,
        samples_file=None,
        dry_run=False,
        no_errors=False,
        pgen_file=None,
        pargs=None,
    ):
        self.filepath = filepath
        self.original_spec = MerlinSpec.load_specification(filepath)
        self.override_vars = override_vars
        error_override_vars(self.override_vars, self.original_spec.path)

        self.samples_file = samples_file
        self.label_clash_error()
        self.dry_run = dry_run
        self.no_errors = no_errors

        # If we load from a file, record that in the object for provenance
        # downstream
        if self.samples_file is not None:
            self.original_spec.merlin["samples"]["file"] = self.samples_file
            self.original_spec.merlin["samples"]["generate"]["cmd"] = ""

        self.restart_dir = restart_dir

        self.special_vars = {
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

        self.pgen_file = pgen_file
        self.pargs = pargs

        self.dag = None
        self.load_dag()

    def _set_special_file_vars(self):
        """Setter for the orig, partial, and expanded file paths of a study."""
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
        Copy the original spec into merlin_info/ as '<base_file_name>.orig.yaml'.
        """
        shutil.copyfile(self.original_spec.path, self.special_vars["MERLIN_SPEC_ORIGINAL_TEMPLATE"])

    def label_clash_error(self):
        """
        Detect any illegal clashes between merlin's
        merlin -> samples -> column_labels and Maestro's
        global.parameters. Raises an error if any such
        clash exists.
        """
        if self.original_spec.merlin["samples"]:
            for label in self.original_spec.merlin["samples"]["column_labels"]:
                if label in self.original_spec.globals:
                    raise ValueError(f"column_label {label} cannot also be in global.parameters!")

    # There's similar code inside expansion.py but the whole point of the function inside that file is
    # to not use the MerlinStudy object so we disable this pylint error
    # pylint: disable=duplicate-code
    @staticmethod
    def get_user_vars(spec):
        """
        Using the spec environment, return a dictionary
        of expanded user-defined variables.
        """
        uvars = []
        if "variables" in spec.environment:
            uvars.append(spec.environment["variables"])
        if "labels" in spec.environment:
            uvars.append(spec.environment["labels"])
        return determine_user_variables(*uvars)

    # pylint: enable=duplicate-code

    @property
    def user_vars(self):
        """Get the user defined variables"""
        return MerlinStudy.get_user_vars(self.original_spec)

    def get_expanded_spec(self):
        """
        Get a new yaml spec file with defaults, cli overrides, and variable expansions.
        Useful for provenance.
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
    def samples(self):
        """
        Return this study's corresponding samples.

        :return: list of samples
        """

        if self.expanded_spec.merlin["samples"]:
            return self.load_samples()
        return []

    def get_sample_labels(self, from_spec):
        """Return the column labels of the samples (if any)"""
        if from_spec.merlin["samples"]:
            return from_spec.merlin["samples"]["column_labels"]
        return []

    @property
    def sample_labels(self):
        """
        Return this study's corresponding sample labels

        Example spec_file contents:

        --spec_file.yaml--
        ...
        merlin:
        samples:
            column_labels: [X0, X1]

        :return: list of labels (e.g. ["X0", "X1"] )
        """
        return self.get_sample_labels(from_spec=self.expanded_spec)

    def load_samples(self):
        """
        load this study's samples from disk, generating if the file does
        not yet exist and the file is defined in the YAML file.
        (no generation will occur if file is defined via __init__)

        Runs the function defined in 'generate' and then loads up
        the sample files defined in 'file', assigning them to the
        variables in 'column_labels'

        Example spec_file contents:

        --spec_file.yaml--
        ...
        merlin:
        samples:
            generate:
                cmd: python make_samples.py -outfile=samples.npy
            file: samples.npy
            column_labels: [X0, X1]

        :return: numpy samples
        :return: the samples loaded
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
    def level_max_dirs(self):
        """
        Returns the maximum number of directory levels.
        """
        with suppress(TypeError, KeyError):
            return self.expanded_spec.merlin["samples"]["level_max_dirs"]
        return defaults.SAMPLES["level_max_dirs"]

    @cached_property
    def output_path(self):
        """
        Determines and creates an output directory for this study.
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
    def timestamp(self):
        """
        Returns a timestamp string, representing the time this
        study began. May be used as an id or unique identifier.
        """
        if self.restart_dir is not None:
            return self.restart_dir.strip("/")[-15:]
        return time.strftime("%Y%m%d-%H%M%S")

    # TODO look into why pylint complains that this method is hidden
    # - might be because we reset self.workspace's value in the expanded_spec method
    @cached_property
    def workspace(self):  # pylint: disable=E0202
        """
        Determines, makes, and returns the path to this study's
        workspace directory. This directory holds workspace directories
        for each step in the study, as well as 'merlin_info/'. The
        name of this directory ends in a timestamp.
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
    def info(self):  # pylint: disable=E0202
        """
        Creates the 'merlin_info' directory inside this study's workspace directory.
        """
        info_name = os.path.join(self.workspace, "merlin_info")
        if self.restart_dir is None:
            os.mkdir(info_name)
        return info_name

    @cached_property
    def expanded_spec(self):
        """
        Determines, writes to yaml, and loads into memory an expanded
        specification.
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
    def flux_command(self):
        """
        Returns the flux command, this will include the full path, if flux_path given in the workflow.
        """
        flux_bin = "flux"
        if "flux_path" in self.expanded_spec.batch.keys():
            flux_bin = os.path.join(self.expanded_spec.batch["flux_path"], "flux")
        return get_flux_cmd(flux_bin, no_errors=self.no_errors)

    def generate_samples(self):
        """
        Runs the function defined in 'generate' if self.samples_file is not
        yet a file.

        Example spec_file contents:

        --spec_file.yaml--
        ...
        merlin:
        samples:
            generate:
                cmd: python make_samples.py -outfile=samples.npy

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

    def load_pgen(self, filepath, pargs, env):
        """Creates a dict of variable names and values defined in a pgen script"""
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
        Generates a dag (a directed acyclic execution graph).
        Assigns it to `self.dag`.
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
        labels = []
        if self.expanded_spec.merlin["samples"]:
            labels = self.expanded_spec.merlin["samples"]["column_labels"]
        # To avoid pickling issues with _pass_detect_cycle from maestro, we unpack the dag here
        self.dag = DAG(maestro_dag.adjacency_table, maestro_dag.values, labels)

    def get_adapter_config(self, override_type=None):
        """Builds and returns the adapter configuration dictionary"""
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
