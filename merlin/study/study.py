###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.5.3.
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

import logging
import os
import shutil
import subprocess
import time
from contextlib import suppress
from fileinput import FileInput

from cached_property import cached_property
from maestrowf.datastructures.core import Study

from merlin.common.abstracts.enums import ReturnCode
from merlin.spec import defaults
from merlin.spec.expansion import (
    determine_user_variables,
    expand_line,
)
from merlin.spec.override import (
    dump_with_overrides,
    error_override_vars,
)
from merlin.spec.specification import MerlinSpec
from merlin.study.dag import DAG
from merlin.utils import (
    get_flux_cmd,
    load_array_file,
)


LOG = logging.getLogger(__name__)


class MerlinStudy:
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

    def __init__(
        self,
        filepath,
        override_vars=None,
        restart_dir=None,
        samples_file=None,
        dry_run=False,
        no_errors=False,
    ):
        self.spec = MerlinSpec.load_specification(filepath)
        self.override_vars = override_vars
        error_override_vars(self.override_vars, self.spec.path)

        self.samples_file = samples_file
        self.label_clash_error()
        self.dry_run = dry_run
        self.no_errors = no_errors

        # If we load from a file, record that in the object for provenance
        # downstream
        if self.samples_file is not None:
            self.spec.merlin["samples"]["file"] = self.samples_file
            self.spec.merlin["samples"]["generate"]["cmd"] = ""

        self.restart_dir = restart_dir

        self.special_vars = {
            "SPECROOT": self.spec.specroot,
            "MERLIN_TIMESTAMP": self.timestamp,
            "MERLIN_INFO": self.info,
            "MERLIN_WORKSPACE": self.workspace,
            "OUTPUT_PATH": self.output_path,
            "MERLIN_SUCCESS": str(int(ReturnCode.OK)),
            "MERLIN_RESTART": str(int(ReturnCode.RESTART)),
            "MERLIN_SOFT_FAIL": str(int(ReturnCode.SOFT_FAIL)),
            "MERLIN_HARD_FAIL": str(int(ReturnCode.HARD_FAIL)),
            "MERLIN_RETRY": str(int(ReturnCode.RETRY)),
        }
        self.dag = None
        self.load_dag()

    def label_clash_error(self):
        """
        Detect any illegal clashes between merlin's
        merlin -> samples -> column_labels and Maestro's
        global.parameters. Raises an error if any such
        clash exists.
        """
        if self.spec.merlin["samples"]:
            for label in self.spec.merlin["samples"]["column_labels"]:
                if label in self.spec.globals:
                    raise ValueError(
                        f"column_label {label} cannot also be " "in global.parameters!"
                    )

    @property
    def user_vars(self):
        """
        Using the spec environment, return a dictionary
        of expanded user-defined variables.
        """
        uvars = []
        if "variables" in self.spec.environment:
            uvars.append(self.spec.environment["variables"])
        if "labels" in self.spec.environment:
            uvars.append(self.spec.environment["labels"])
        return determine_user_variables(*uvars)

    def write_expand_by_line(self, filepath, keywords):
        """
        Given a destination and keyword dictionary, expand each
        line of the destination file in-place.
        """
        with FileInput(filepath, inplace=True) as _file:
            for line in _file:
                expanded_line = expand_line(line, keywords)
                print(expanded_line, end="")

    def write_expanded_spec(self, dest):
        """
        Write a new yaml spec file with defaults and variable expansions.
        Useful for provenance.

        :param `dest`: destination for fully expanded yaml file
        """
        # specification text including defaults and overridden user variables
        full_spec = dump_with_overrides(self.spec, self.override_vars)

        with open(dest, "w") as dumped_file:
            dumped_file.write(full_spec)

        # update spec so that user_vars update will be accurate
        self.spec = MerlinSpec.load_specification(dest)

        # expand user variables
        self.write_expand_by_line(dest, self.user_vars)
        # expand reserved words
        self.write_expand_by_line(dest, self.special_vars)

    @property
    def samples(self):
        """
        Return this study's corresponding samples.

        :return: list of samples
        """

        if self.expanded_spec.merlin["samples"]:
            return self.load_samples()
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
        if self.expanded_spec.merlin["samples"]:
            return self.expanded_spec.merlin["samples"]["column_labels"]
        return []

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

        else:
            output_path = str(self.spec.output_path)

            if (self.override_vars is not None) and (
                "OUTPUT_PATH" in self.override_vars
            ):
                output_path = str(self.override_vars["OUTPUT_PATH"])

            output_path = expand_line(output_path, self.user_vars)
            output_path = os.path.abspath(output_path)
            if not os.path.isdir(output_path):
                os.makedirs(output_path)
                LOG.info(f"Made dir(s) to output path '{output_path}'.")

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

    @cached_property
    def workspace(self):
        """
        Determines, makes, and returns the path to this study's
        workspace directory. This directory holds workspace directories
        for each step in the study, as well as 'merlin_info/'. The
        name of this directory ends in a timestamp.
        """
        if self.restart_dir is not None:
            if not os.path.isdir(self.restart_dir):
                raise ValueError(
                    f"Restart directory '{self.restart_dir}' does not exist!"
                )
            return os.path.abspath(self.restart_dir)

        workspace_name = f'{self.spec.name.replace(" ", "_")}_{self.timestamp}'
        workspace = os.path.join(self.output_path, workspace_name)
        with suppress(FileNotFoundError):
            shutil.rmtree(workspace)
        os.mkdir(workspace)

        LOG.info(f"Study workspace is '{workspace}'.")
        return workspace

    @cached_property
    def info(self):
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
        # Write expanded yaml spec
        self.expanded_filepath = os.path.join(
            self.info, self.spec.name.replace(" ", "_") + ".yaml"
        )

        # If we are restarting, we don't need to re-expand, just need to read
        # in the previously expanded spec
        if self.restart_dir is None:
            self.write_expanded_spec(self.expanded_filepath)

        return MerlinSpec.load_specification(
            self.expanded_filepath, suppress_warning=False
        )

    @cached_property
    def flux_command(self):
        """
        Returns a the flux version
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
                sample_generate = self.expanded_spec.merlin["samples"]["generate"][
                    "cmd"
                ]
                LOG.info("Generating samples...")
                subprocess.call(sample_generate, shell=True)
                LOG.info("Generating samples complete!")
            return
        except (IndexError, TypeError) as e:
            LOG.error(f"Could not generate samples:\n{e}")
            return

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
        self.dag = DAG(maestro_dag, labels)

    def get_adapter_config(self, override_type=None):
        spec = MerlinSpec.load_specification(self.spec.path)
        adapter_config = dict(spec.batch)

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
