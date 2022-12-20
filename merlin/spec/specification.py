###############################################################################
# Copyright (c) 2022, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.9.1.
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

"""
This module contains a class, MerlinSpec, which holds the unchanged
data from the Merlin specification file.
To see examples of yaml specifications, run `merlin example`.
"""
import json
import logging
import os
import shlex
from io import StringIO

import yaml
from maestrowf.specification import YAMLSpecification

from merlin.spec import all_keys, defaults


LOG = logging.getLogger(__name__)


class MerlinSpec(YAMLSpecification):
    """
    This class represents the logic for parsing the Merlin yaml
    specification.

    Example spec_file contents:

    --spec_file.yaml--
    ...
    merlin:
        resources:
            task_server: celery
        samples:
            generate:
                cmd: python make_samples.py -outfile=$(OUTPUT_PATH)/merlin_info/samples.npy
            file: $(OUTPUT_PATH)/merlin_info/samples.npy
            column_labels: [X0, X1]
    """

    def __init__(self):
        super(MerlinSpec, self).__init__()

    @property
    def yaml_sections(self):
        """
        Returns a nested dictionary of all sections of the specification
        as used in a yaml spec.
        """
        return {
            "description": self.description,
            "batch": self.batch,
            "env": self.environment,
            "study": self.study,
            "global.parameters": self.globals,
            "merlin": self.merlin,
            "user": self.user,
        }

    @property
    def sections(self):
        """
        Returns a nested dictionary of all sections of the specification
        as referenced by Maestro's YAMLSpecification class.
        """
        return {
            "description": self.description,
            "batch": self.batch,
            "environment": self.environment,
            "study": self.study,
            "globals": self.globals,
            "merlin": self.merlin,
            "user": self.user,
        }

    def __str__(self):
        """Magic method to print an instance of our MerlinSpec class."""
        env = ""
        globs = ""
        merlin = ""
        user = ""
        if self.environment:
            env = f"\n\tenvironment: \n\t\t{self.environment}"
        if self.globals:
            globs = f"\n\tglobals:\n\t\t{self.globals}"
        if self.merlin:
            merlin = f"\n\tmerlin:\n\t\t{self.merlin}"
        if self.user is not None:
            user = f"\n\tuser:\n\t\t{self.user}"
        result = f"""MERLIN SPEC OBJECT:\n\tdescription:\n\t\t{self.description}
               \n\tbatch:\n\t\t{self.batch}\n\tstudy:\n\t\t{self.study}
               {env}{globs}{merlin}{user}"""

        return result

    @classmethod
    def load_specification(cls, filepath, suppress_warning=True):
        LOG.info("Loading specification from path: %s", filepath)
        try:
            # Load the YAML spec from the filepath
            with open(filepath, "r") as data:
                spec = cls.load_spec_from_string(data, needs_IO=False, needs_verification=True)
        except Exception as e:
            LOG.exception(e.args)
            raise e

        # Path not set in _populate_spec because loading spec with string
        # does not have a path so we set it here
        spec.path = filepath
        spec.specroot = os.path.dirname(spec.path)

        if not suppress_warning:
            spec.warn_unrecognized_keys()
        return spec

    @classmethod
    def load_spec_from_string(cls, string, needs_IO=True, needs_verification=False):
        LOG.debug("Creating Merlin spec object...")
        # Create and populate the MerlinSpec object
        data = StringIO(string) if needs_IO else string
        spec = cls._populate_spec(data)
        spec.specroot = None
        spec.process_spec_defaults()
        LOG.debug("Merlin spec object created.")

        # Verify the spec object
        if needs_verification:
            LOG.debug("Verifying Merlin spec...")
            spec.verify()
            LOG.debug("Merlin spec verified.")

        return spec

    @classmethod
    def _populate_spec(cls, data):
        """
        Helper method to load a study spec and populate it's fields.

        NOTE: This is basically a direct copy of YAMLSpecification's
        load_specification method from Maestro just without the call to verify.
        The verify method was breaking our code since we have no way of modifying
        Maestro's schema that they use to verify yaml files. The work around
        is to load the yaml file ourselves and create our own schema to verify
        against.

        :param data: Raw text stream to study YAML spec data
        :returns: A MerlinSpec object containing information from the path
        """
        # Read in the spec file
        try:
            spec = yaml.load(data, yaml.FullLoader)
        except AttributeError:
            LOG.warn(
                "PyYAML is using an unsafe version with a known "
                "load vulnerability. Please upgrade your installation "
                "to a more recent version!"
            )
            spec = yaml.load(data)
        LOG.debug("Successfully loaded specification: \n%s", spec["description"])

        # Load in the parts of the yaml that are the same as Maestro's
        merlin_spec = cls()
        merlin_spec.path = None
        merlin_spec.description = spec.pop("description", {})
        merlin_spec.environment = spec.pop("env", {"variables": {}, "sources": [], "labels": {}, "dependencies": {}})
        merlin_spec.batch = spec.pop("batch", {})
        merlin_spec.study = spec.pop("study", [])
        merlin_spec.globals = spec.pop("global.parameters", {})

        # Reset the file pointer and load the merlin block
        data.seek(0)
        merlin_spec.merlin = MerlinSpec.load_merlin_block(data)

        # Reset the file pointer and load the user block
        data.seek(0)
        merlin_spec.user = MerlinSpec.load_user_block(data)

        return merlin_spec

    def verify(self):
        """
        Verify the spec against a valid schema. Similar to YAMLSpecification's verify
        method from Maestro but specific for Merlin yaml specs.

        NOTE: Maestro v2.0 may add the ability to customize the schema files it
        compares against. If that's the case then we can convert this file back to
        using Maestro's verification.
        """
        # Load the MerlinSpec schema file
        dir_path = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(dir_path, "merlinspec.json")
        with open(schema_path, "r") as json_file:
            schema = json.load(json_file)

        # Use Maestro's verification methods for shared sections
        self.verify_description(schema["DESCRIPTION"])
        self.verify_environment(schema["ENV"])
        self.verify_study(schema["STUDY_STEP"])
        self.verify_parameters(schema["PARAM"])

        # Merlin specific verification
        self.verify_merlin_block(schema["MERLIN"])
        self.verify_batch_block(schema["BATCH"])

    def get_study_step_names(self):
        """
        Get a list of the names of steps in our study.

        :returns: an unsorted list of study step names
        """
        names = []
        for step in self.study:
            names.append(step["name"])
        return names

    def _verify_workers(self):
        """
        Helper method to verify the workers section located within the Merlin block
        of our spec file.
        """
        # Retrieve the names of the steps in our study
        actual_steps = self.get_study_step_names()

        try:
            # Verify that the steps in merlin block's worker section actually exist
            for worker, worker_vals in self.merlin["resources"]["workers"].items():
                error_prefix = f"Problem in Merlin block with worker {worker} --"
                for step in worker_vals["steps"]:
                    if step != "all" and step not in actual_steps:
                        error_msg = (
                            f"{error_prefix} Step with the name {step}"
                            " is not defined in the study block of the yaml specification file"
                        )
                        raise ValueError(error_msg)

        except Exception:
            raise

    def verify_merlin_block(self, schema):
        """
        Method to verify the merlin section of our spec file.

        :param schema: The section of the predefined schema (merlinspec.json) to check
                       our spec file against.
        """
        # Validate merlin block against the json schema
        YAMLSpecification.validate_schema("merlin", self.merlin, schema)
        # Verify the workers section within merlin block
        self._verify_workers()

    def verify_batch_block(self, schema):
        """
        Method to verify the batch section of our spec file.

        :param schema: The section of the predefined schema (merlinspec.json) to check
                       our spec file against.
        """
        # Validate batch block against the json schema
        YAMLSpecification.validate_schema("batch", self.batch, schema)

        # Additional Walltime checks in case the regex from the schema bypasses an error
        if "walltime" in self.batch:
            if self.batch["type"] == "lsf":
                LOG.warning("The walltime argument is not available in lsf.")
            else:
                try:
                    err_msg = "Walltime must be of the form SS, MM:SS, or HH:MM:SS."
                    walltime = self.batch["walltime"]
                    if len(walltime) > 2:
                        # Walltime must have : if it's not of the form SS
                        if ":" not in walltime:
                            raise ValueError(err_msg)
                        else:
                            # Walltime must have exactly 2 chars between :
                            time = walltime.split(":")
                            for section in time:
                                if len(section) != 2:
                                    raise ValueError(err_msg)
                except Exception:
                    raise

    @staticmethod
    def load_merlin_block(stream):
        try:
            merlin_block = yaml.safe_load(stream)["merlin"]
        except KeyError:
            merlin_block = {}
            warning_msg: str = (
                "Workflow specification missing \n "
                "encouraged 'merlin' section! Run 'merlin example' for examples.\n"
                "Using default configuration with no sampling."
            )
            LOG.warning(warning_msg)
        return merlin_block

    @staticmethod
    def load_user_block(stream):
        try:
            user_block = yaml.safe_load(stream)["user"]
        except KeyError:
            user_block = {}
        return user_block

    def process_spec_defaults(self):
        for name, section in self.sections.items():
            if section is None:
                setattr(self, name, {})

        # fill in missing batch section defaults
        MerlinSpec.fill_missing_defaults(self.batch, defaults.BATCH["batch"])

        # fill in missing env section defaults
        MerlinSpec.fill_missing_defaults(self.environment, defaults.ENV["env"])

        # fill in missing global parameter section defaults
        MerlinSpec.fill_missing_defaults(self.globals, defaults.PARAMETER["global.parameters"])

        # fill in missing step section defaults within 'run'
        defaults.STUDY_STEP_RUN["shell"] = self.batch["shell"]
        for step in self.study:
            MerlinSpec.fill_missing_defaults(step["run"], defaults.STUDY_STEP_RUN)

        # fill in missing merlin section defaults
        MerlinSpec.fill_missing_defaults(self.merlin, defaults.MERLIN["merlin"])
        if self.merlin["resources"]["workers"] is None:
            self.merlin["resources"]["workers"] = {"default_worker": defaults.WORKER}
        else:
            for worker, vals in self.merlin["resources"]["workers"].items():
                MerlinSpec.fill_missing_defaults(vals, defaults.WORKER)
        if self.merlin["samples"] is not None:
            MerlinSpec.fill_missing_defaults(self.merlin["samples"], defaults.SAMPLES)

        # no defaults for user block

    @staticmethod
    def fill_missing_defaults(object_to_update, default_dict):
        """
        Merge keys and values from a dictionary of defaults
        into a parallel object that may be missing attributes.
        Only adds missing attributes to object; does not overwrite
        existing ones.
        """

        def recurse(result, defaults):
            if not isinstance(defaults, dict):
                return
            for key, val in defaults.items():
                # fmt: off
                if (key not in result) or (
                    (result[key] is None) and (defaults[key] is not None)
                ):
                    result[key] = val
                else:
                    recurse(result[key], val)
                # fmt: on

        recurse(object_to_update, default_dict)

    # ***Unsure if this method is still needed after adding json schema verification***
    def warn_unrecognized_keys(self):
        # check description
        MerlinSpec.check_section("description", self.description, all_keys.DESCRIPTION)

        # check batch
        MerlinSpec.check_section("batch", self.batch, all_keys.BATCH)

        # check env
        MerlinSpec.check_section("env", self.environment, all_keys.ENV)

        # check parameters
        for param, contents in self.globals.items():
            MerlinSpec.check_section("global.parameters", contents, all_keys.PARAMETER)

        # check steps
        for step in self.study:
            MerlinSpec.check_section(step["name"], step, all_keys.STUDY_STEP)
            MerlinSpec.check_section(step["name"] + ".run", step["run"], all_keys.STUDY_STEP_RUN)

        # check merlin
        MerlinSpec.check_section("merlin", self.merlin, all_keys.MERLIN)
        MerlinSpec.check_section("merlin.resources", self.merlin["resources"], all_keys.MERLIN_RESOURCES)
        for worker, contents in self.merlin["resources"]["workers"].items():
            MerlinSpec.check_section("merlin.resources.workers " + worker, contents, all_keys.WORKER)
        if self.merlin["samples"]:
            MerlinSpec.check_section("merlin.samples", self.merlin["samples"], all_keys.SAMPLES)

        # user block is not checked

    @staticmethod
    def check_section(section_name, section, all_keys):
        diff = set(section.keys()).difference(all_keys)

        # TODO: Maybe add a check here for required keys

        for extra in diff:
            LOG.warn(f"Unrecognized key '{extra}' found in spec section '{section_name}'.")

    def dump(self):
        """
        Dump this MerlinSpec to a pretty yaml string.
        """
        tab = 3 * " "
        result = self._dict_to_yaml(self.yaml_sections, "", [], tab)
        while "\n\n\n" in result:
            result = result.replace("\n\n\n", "\n\n")
        try:
            yaml.safe_load(result)
        except Exception as e:
            raise ValueError(f"Error parsing provenance spec:\n{e}")
        return result

    def _dict_to_yaml(self, obj, string, key_stack, tab, newline=True):
        """
        The if-else ladder for sorting the yaml string prettification of dump().
        """
        if obj is None:
            return ""

        lvl = len(key_stack) - 1

        if isinstance(obj, str):
            return self._process_string(obj, lvl, tab)
        elif isinstance(obj, bool):
            return str(obj).lower()
        elif not (isinstance(obj, list) or isinstance(obj, dict)):
            return obj
        else:
            return self._process_dict_or_list(obj, string, key_stack, lvl, tab)

    def _process_string(self, obj, lvl, tab):
        """
        Processes strings for _dict_to_yaml() in the dump() method.
        """
        split = obj.splitlines()
        if len(split) > 1:
            obj = "|\n" + tab * (lvl + 1) + ("\n" + tab * (lvl + 1)).join(split)
        return obj

    def _process_dict_or_list(self, obj, string, key_stack, lvl, tab):
        """
        Processes lists and dicts for _dict_to_yaml() in the dump() method.
        """
        from copy import deepcopy

        list_offset = 2 * " "
        if isinstance(obj, list):
            n = len(obj)
            use_hyphens = key_stack[-1] in ["paths", "sources", "git", "study"] or key_stack[0] in ["user"]
            if not use_hyphens:
                string += "["
            else:
                string += "\n"
            for i, elem in enumerate(obj):
                key_stack = deepcopy(key_stack)
                key_stack.append("elem")
                if use_hyphens:
                    string += (lvl + 1) * tab + "- " + str(self._dict_to_yaml(elem, "", key_stack, tab)) + "\n"
                else:
                    string += str(self._dict_to_yaml(elem, "", key_stack, tab, newline=(i != 0)))
                    if n > 1 and i != len(obj) - 1:
                        string += ", "
                key_stack.pop()
            if not use_hyphens:
                string += "]"
        # must be dict
        else:
            if len(key_stack) > 0 and key_stack[-1] != "elem":
                string += "\n"
            i = 0
            for k, v in obj.items():
                key_stack = deepcopy(key_stack)
                key_stack.append(k)
                if len(key_stack) > 1 and key_stack[-2] == "elem" and i == 0:
                    # string += (tab * (lvl - 1))
                    string += ""
                elif "elem" in key_stack:
                    string += list_offset + (tab * lvl)
                else:
                    string += tab * (lvl + 1)
                string += str(k) + ": " + str(self._dict_to_yaml(v, "", key_stack, tab)) + "\n"
                key_stack.pop()
                i += 1
        return string

    def get_task_queues(self):
        """Returns a dictionary of steps and their corresponding task queues."""
        from merlin.config.configfile import CONFIG

        steps = self.get_study_steps()
        queues = {}
        for step in steps:
            if "task_queue" in step.run and CONFIG.celery.omit_queue_tag:
                queues[step.name] = step.run["task_queue"]
            elif "task_queue" in step.run:
                queues[step.name] = CONFIG.celery.queue_tag + step.run["task_queue"]
        return queues

    def get_queue_list(self, steps):
        """
        Return a sorted list of queues corresponding to spec steps

        param steps: a list of step names or 'all'
        """
        queues = self.get_task_queues()
        if steps[0] == "all":
            task_queues = queues.values()
        else:
            try:
                if isinstance(steps, list):
                    task_queues = [queues[step] for step in steps]
                else:
                    task_queues = [queues[steps]]
            except KeyError:
                nl = "\n"
                LOG.error(f"Invalid steps '{steps}'! Try one of these (or 'all'):\n{nl.join(queues.keys())}")
                raise
        return sorted(set(task_queues))

    def make_queue_string(self, steps):
        """
        Return a unique queue string for the steps

        param steps: a list of step names
        """
        queues = ",".join(set(self.get_queue_list(steps)))
        return shlex.quote(queues)

    def get_worker_names(self):
        result = []
        for worker in self.merlin["resources"]["workers"]:
            result.append(worker)
        return result
