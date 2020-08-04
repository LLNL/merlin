###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.3.
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
import logging
import os
from io import StringIO

import yaml
from maestrowf.datastructures import YAMLSpecification

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
        }

    @classmethod
    def load_specification(cls, filepath, suppress_warning=True):
        spec = super(MerlinSpec, cls).load_specification(filepath)
        with open(filepath, "r") as f:
            spec.merlin = MerlinSpec.load_merlin_block(f)
        spec.specroot = os.path.dirname(spec.path)
        spec.process_spec_defaults()
        if not suppress_warning:
            spec.warn_unrecognized_keys()
        return spec

    @classmethod
    def load_spec_from_string(cls, string):
        spec = super(MerlinSpec, cls).load_specification_from_stream(StringIO(string))
        spec.merlin = MerlinSpec.load_merlin_block(StringIO(string))
        spec.specroot = None
        spec.process_spec_defaults()
        return spec

    @staticmethod
    def load_merlin_block(stream):
        try:
            merlin_block = yaml.safe_load(stream)["merlin"]
        except KeyError:
            merlin_block = {}
            LOG.warning(
                f"Workflow specification missing \n "
                f"encouraged 'merlin' section! Run 'merlin example' for examples.\n"
                f"Using default configuration with no sampling."
            )
        return merlin_block

    def process_spec_defaults(self):
        # fill in missing batch section defaults
        MerlinSpec.fill_missing_defaults(self.batch, defaults.BATCH["batch"])

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
                if key not in result:
                    result[key] = val
                else:
                    recurse(result[key], val)

        recurse(object_to_update, default_dict)

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
            MerlinSpec.check_section(
                step["name"] + ".run", step["run"], all_keys.STUDY_STEP_RUN
            )

        # check merlin
        MerlinSpec.check_section("merlin", self.merlin, all_keys.MERLIN)
        MerlinSpec.check_section(
            "merlin.resources", self.merlin["resources"], all_keys.MERLIN_RESOURCES
        )
        for worker, contents in self.merlin["resources"]["workers"].items():
            MerlinSpec.check_section(
                "merlin.resources.workers " + worker, contents, all_keys.WORKER
            )
        if self.merlin["samples"]:
            MerlinSpec.check_section(
                "merlin.samples", self.merlin["samples"], all_keys.SAMPLES
            )

    @staticmethod
    def check_section(section_name, section, all_keys):
        diff = set(section.keys()).difference(all_keys)
        for extra in diff:
            LOG.warn(
                f"Unrecognized key '{extra}' found in spec section '{section_name}'."
            )

    def dump(self):
        """
        Dump this MerlinSpec to a pretty yaml string.
        """
        tab = "   "
        list_offset = "  "
        from copy import deepcopy

        def dict_to_yaml(obj, string, key_stack, newline=True):
            if obj is None:
                return ""
            lvl = len(key_stack) - 1
            if isinstance(obj, str):
                split = obj.splitlines()
                if len(split) > 1:
                    obj = "|\n" + tab * (lvl + 1) + ("\n" + tab * (lvl + 1)).join(split)
                return obj
            if isinstance(obj, bool):
                return str(obj).lower()
            if (not isinstance(obj, list)) and (not isinstance(obj, dict)):
                return obj
            if isinstance(obj, list):
                n = len(obj)
                use_hyphens = key_stack[-1] in ["paths", "sources", "git", "study"]
                if not use_hyphens:
                    string += "["
                else:
                    string += "\n"
                for i, elem in enumerate(obj):
                    key_stack = deepcopy(key_stack)
                    key_stack.append("elem")
                    if use_hyphens:
                        string += (
                            (lvl + 1) * tab
                            + "- "
                            + str(dict_to_yaml(elem, "", key_stack))
                            + "\n"
                        )
                    else:
                        string += str(
                            dict_to_yaml(elem, "", key_stack, newline=(i != 0))
                        )
                        if n > 1 and i != len(obj) - 1:
                            string += ", "
                    key_stack.pop()
                if not use_hyphens:
                    string += "]"
            if isinstance(obj, dict):
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
                    string += str(k) + ": " + str(dict_to_yaml(v, "", key_stack)) + "\n"
                    key_stack.pop()
                    i += 1
            return string

        result = dict_to_yaml(self.yaml_sections, "", [])
        while "\n\n\n" in result:
            result = result.replace("\n\n\n", "\n\n")
        try:
            yaml.safe_load(result)
        except BaseException as e:
            raise ValueError(f"Error parsing provenance spec:\n{e}")
        return result

    def get_task_queues(self):
        """Returns a dictionary of steps and their corresponding task queues."""
        steps = self.get_study_steps()
        queues = {}
        for step in steps:
            if "task_queue" in step.run:
                queues[step.name] = step.run["task_queue"]
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
                LOG.error(
                    f"Invalid steps '{steps}'! Try one of these (or 'all'):\n{nl.join(queues.keys())}"
                )
                raise
        return sorted(set(task_queues))

    def make_queue_string(self, steps):
        """
        Return a unique queue string for the steps

        param steps: a list of step names
        """
        return ",".join(set(self.get_queue_list(steps)))

    def get_worker_names(self):
        result = []
        for worker in self.merlin["resources"]["workers"]:
            result.append(worker)
        return result
