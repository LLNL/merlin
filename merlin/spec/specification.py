##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains a class, `MerlinSpec`, which holds the unchanged
data from the Merlin specification file.

To see examples of yaml specifications, run `merlin example`.
"""
import json
import logging
import os
import shlex
from copy import deepcopy
from datetime import timedelta
from io import StringIO
from typing import Any, Dict, List, Set, TextIO, Union

import yaml
from maestrowf.datastructures.core.parameters import ParameterGenerator
from maestrowf.specification import YAMLSpecification

from merlin.spec import all_keys, defaults
from merlin.utils import find_vlaunch_var, load_array_file, needs_merlin_expansion, repr_timedelta


LOG = logging.getLogger(__name__)


# Pylint complains we have too many instance attributes but it's fine
class MerlinSpec(YAMLSpecification):  # pylint: disable=R0902
    """
    A class to represent and manage the specifications for a Merlin workflow.

    This class provides methods to verify, load, and process various sections of a
    workflow specification file, including the merlin block, batch block, and user block.
    It also handles default values and parameter mapping.

    Attributes:
        batch (Dict): A dictionary representing the batch section of the spec file.
        description (Dict): A dictionary representing the description section of the spec file.
        environment (Dict): A dictionary representing the environment section of the spec file.
        globals (Dict): A dictionary representing global parameters in the spec file.
        merlin (Dict): A dictionary representing the merlin section of the spec file.
        sections (Dict): A dictionary of all sections in the spec file.
        study (Dict): A dictionary representing the study section of the spec file.
        user (Dict): A dictionary representing the user section of the spec file.
        yaml_sections (Dict): A dictionary for YAML representation of the sections.

    Methods:
        check_section: Checks sections of the spec file for unrecognized keys.
        dump: Dumps the current spec to a pretty YAML string.
        fill_missing_defaults: Merges default values into an object.
        get_queue_list: Returns a sorted set of queues for specified steps.
        get_queue_step_relationship: Maps task queues to their associated steps.
        get_step_param_map: Creates a mapping of parameters used for each step.
        get_step_worker_map: Maps step names to associated workers.
        get_study_step_names: Returns a list of the names of the steps in the spec file.
        get_task_queues: Maps steps to their corresponding task queues.
        get_tasks_per_step: Returns the number of tasks needed for each step.
        get_worker_names: Returns a list of worker names.
        get_worker_step_map: Maps worker names to associated steps.
        load_merlin_block: Loads the merlin block from a YAML stream.
        load_spec_from_string: Creates a `MerlinSpec` object from a string (or stream) representing
            a spec file.
        load_specification: Creates a `MerlinSpec` object based on the contents of a spec file.
        load_user_block: Loads the user block from a YAML stream.
        make_queue_string: Returns a unique queue string for specified steps.
        process_spec_defaults: Fills in default values for missing sections.
        verify: Verify the spec against a valid schema.
        verify_batch_block: Validates the batch block against a predefined schema.
        verify_merlin_block: Validates the merlin block against a predefined schema.
        warn_unrecognized_keys: Checks for unrecognized keys in the spec file.
    """

    # Pylint says this call to super is useless but we'll leave it in case we want to add to __init__ in the future
    def __init__(self):  # pylint: disable=W0246
        """Initializes a MerlinSpec object."""
        super().__init__()

    @property
    def yaml_sections(self) -> Dict:
        """
        Returns a nested dictionary of all sections of the specification as used in a YAML
        specification. The structure is tailored for YAML representation.

        Returns:
            A dictionary containing the sections of the specification formatted for YAML.
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
    def sections(self) -> Dict:
        """
        Returns a nested dictionary of all sections of the specification as referenced by
        [Maestro's `YAMLSpecification` class](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/specification/yamlspecification.html).
        The structure is aligned with the expectations of Maestro's `YAMLSpecification` class.

        Returns:
            A dictionary containing the sections of the specification formatted for Maestro.
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
    def load_specification(cls, path: str, suppress_warning: bool = True) -> "MerlinSpec":
        """
        Load a specification file and create a `MerlinSpec` object based on its contents.

        This method reads a YAML specification file from the provided path,
        processes its contents, and returns a `MerlinSpec` object. It can also
        suppress warnings about unrecognized keys in the specification.

        Args:
            path: The path to the specification file to be loaded.
            suppress_warning: Whether to suppress warnings about unrecognized keys.

        Returns:
            A `MerlinSpec` object created from the contents of the specification file.

        Raises:
            Exception: If there is an error loading the specification file.
        """
        LOG.info("Loading specification from path: %s", path)
        try:
            # Load the YAML spec from the path
            with open(path, "r") as data:
                spec = cls.load_spec_from_string(data, needs_IO=False, needs_verification=True)
        except Exception as e:  # pylint: disable=C0103
            LOG.exception(e.args)
            raise e

        # Path not set in _populate_spec because loading spec with string
        # does not have a path so we set it here
        spec.path = path
        spec.specroot = os.path.dirname(spec.path)  # pylint: disable=W0201

        if not suppress_warning:
            spec.warn_unrecognized_keys()
        return spec

    @classmethod
    def load_spec_from_string(
        cls, string: Union[str, TextIO], needs_IO: bool = True, needs_verification: bool = False
    ) -> "MerlinSpec":  # pylint: disable=C0103
        """
        Read a specification from a string (or stream) and create a `MerlinSpec` object from it.

        This method processes a string or stream containing the specification
        and returns a `MerlinSpec` object. It can also verify the specification
        if required.

        Args:
            string: A string or stream of the specification content.
            needs_IO: Whether to treat the string as a file object.
            needs_verification: Whether to verify the specification after loading.

        Returns:
            A `MerlinSpec` object created from the provided specification content.
        """
        LOG.debug("Creating Merlin spec object...")
        # Create and populate the MerlinSpec object
        data = StringIO(string) if needs_IO else string
        spec = cls._populate_spec(data)
        spec.specroot = None  # pylint: disable=W0201
        spec.process_spec_defaults()
        LOG.debug("Merlin spec object created.")

        # Verify the spec object
        if needs_verification:
            LOG.debug("Verifying Merlin spec...")
            spec.verify()
            LOG.debug("Merlin spec verified.")

        # Convert the walltime value back to HMS if PyYAML messed with it
        for _, section in spec.yaml_sections.items():
            # Section is a list for the study block
            if isinstance(section, list):
                for step in section:
                    if "walltime" in step and isinstance(step["walltime"], int):
                        step["walltime"] = repr_timedelta(timedelta(seconds=step["walltime"]))
            # Section is a dict for all other blocks
            if isinstance(section, dict):
                if "walltime" in section and isinstance(section["walltime"], int):
                    section["walltime"] = repr_timedelta(timedelta(seconds=section["walltime"]))

        return spec

    @classmethod
    def _populate_spec(cls, data: TextIO) -> "MerlinSpec":
        """
        Helper method to load a study specification and populate its fields.

        This method reads a YAML specification from a raw text stream and
        populates the fields of a `MerlinSpec` object. It is a modified version
        of the `load_specification` method from Maestro's YAMLSpecification class,
        excluding the verification step due to compatibility issues with Maestro's schema.

        Note:
            This is basically a direct copy of YAMLSpecification's
            load_specification method from Maestro just without the call to verify.
            The verify method was breaking our code since we have no way of modifying
            Maestro's schema that they use to verify yaml files. The work around
            is to load the yaml file ourselves and create our own schema to verify
            against.

        Args:
            data: A raw text stream containing the study YAML specification data.

        Returns:
            A `MerlinSpec` object populated with information extracted from the YAML specification.
        """
        # Read in the spec file
        try:
            spec = yaml.load(data, yaml.FullLoader)
        except AttributeError:
            LOG.warning(
                "PyYAML is using an unsafe version with a known "
                "load vulnerability. Please upgrade your installation "
                "to a more recent version!"
            )
            spec = yaml.load(data, yaml.Loader)
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
        merlin_spec.merlin = MerlinSpec.load_merlin_block(data)  # pylint: disable=W0201

        # Reset the file pointer and load the user block
        data.seek(0)
        merlin_spec.user = MerlinSpec.load_user_block(data)  # pylint: disable=W0201

        return merlin_spec

    def verify(self):
        """
        Verify the specification against a valid schema.

        This method checks the current `MerlinSpec` object against a predefined
        schema to ensure that it adheres to the expected structure and
        constraints. It is similar to the verify method from Maestro's
        YAMLSpecification class but is tailored specifically for Merlin YAML
        specifications.

        Note:
            Maestro v2.0 may introduce the ability to customize the schema files
            used for verification. If that feature becomes available, then we can
            convert this file back to using Maestro's verification.

        Raises:
            Exception: If the specification does not conform to the schema,
                appropriate exceptions will be raised during the verification process.
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

    def get_study_step_names(self) -> List[str]:
        """
        Retrieve the names of steps in the study.

        This method iterates through the study steps and collects their names
        into a list. The returned list is unsorted.

        Returns:
            An unsorted list of strings representing the names of the
                study steps.
        """
        names = []
        for step in self.study:
            names.append(step["name"])
        return names

    def _verify_workers(self):
        """
        Verify the workers section in the Merlin block of the specification.

        This helper method checks that the steps referenced in the workers
        section of the Merlin block exist in the study steps. It raises a
        ValueError if any step specified for a worker does not match the
        defined study steps.

        Raises:
            ValueError: If a step specified in the workers section does not
                exist in the list of study step names.
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

        except Exception:  # pylint: disable=W0706
            raise

    def verify_merlin_block(self, schema: Dict):
        """

        Verify the Merlin section of the specification file against a schema.

        This method validates the Merlin block of the specification file
        against a predefined JSON schema and verifies the workers section
        to ensure that all specified steps are defined in the study.

        Args:
            schema: The section of the predefined schema (merlinspec.json) to
                check the Merlin block against.
        """
        # Validate merlin block against the json schema
        YAMLSpecification.validate_schema("merlin", self.merlin, schema)
        # Verify the workers section within merlin block
        self._verify_workers()

    def verify_batch_block(self, schema: Dict):
        """
        Verify the batch section of the specification file against a schema.

        This method validates the batch block of the specification file
        against a predefined JSON schema and performs additional checks
        related to the walltime parameter for the LSF batch type.

        Args:
            schema: The section of the predefined schema (merlinspec.json) to
                check the batch block against.
        """
        # Validate batch block against the json schema
        YAMLSpecification.validate_schema("batch", self.batch, schema)

        # Additional Walltime checks in case the regex from the schema bypasses an error
        if self.batch["type"] == "lsf" and "walltime" in self.batch:
            LOG.warning("The walltime argument is not available in lsf.")

    @staticmethod
    def load_merlin_block(stream: TextIO) -> Dict:
        """
        Load the Merlin block from a specification file stream.

        This static method reads a YAML stream and attempts to extract
        the 'merlin' section. If the 'merlin' section is missing, it
        logs a warning and returns an empty dictionary, indicating that
        the default configuration will be used without sampling.

        Args:
            stream: A file-like object or string stream containing the
                YAML specification.

        Returns:
            The Merlin block extracted from the YAML stream. If the 'merlin'
                section is not found, an empty dictionary is returned.
        """
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
    def load_user_block(stream: TextIO) -> Dict:
        """
        Load the user block from a specification file stream.

        This static method reads a YAML stream and attempts to extract
        the 'user' section. If the 'user' section is not present, it
        returns an empty dictionary.

        Args:
            stream: A file-like object or string stream containing the
                YAML specification.

        Returns:
            The user block extracted from the YAML stream. If the 'user'
                section is not found, an empty dictionary is returned.
        """
        try:
            user_block = yaml.safe_load(stream)["user"]
        except KeyError:
            user_block = {}
        return user_block

    def process_spec_defaults(self):
        """
        Fill in default values for specification sections if they are missing.

        This method iterates through the sections of the specification and
        populates any that are `None` with empty dictionaries. It then fills
        in default values for various sections, including batch, environment,
        global parameters, and step sections within the study.

        The method also handles specific cases for the VLAUNCHER variables
        in the command of each step, ensuring that default values are set
        if they are not defined by the user. Additionally, it ensures that
        workers are assigned to steps appropriately, filling in defaults
        where necessary.

        The method modifies the instance's attributes directly, ensuring that
        the specification is complete and ready for further processing.
        """
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
            # Insert VLAUNCHER specific variables if necessary
            if "$(VLAUNCHER)" in step["run"]["cmd"]:
                SHSET = ""
                if "csh" in step["run"]["shell"]:
                    SHSET = "set "
                # We need to set default values for VLAUNCHER variables if they're not defined by the user
                for vlaunch_var, vlaunch_val in defaults.VLAUNCHER_VARS.items():
                    if not find_vlaunch_var(vlaunch_var.replace("MERLIN_", ""), step["run"]["cmd"], accept_no_matches=True):
                        # Look for predefined nodes/procs/cores/gpus values in the step and default to those
                        vlaunch_val = step["run"][vlaunch_val[0]] if vlaunch_val[0] in step["run"] else vlaunch_val[1]
                        step["run"]["cmd"] = f"{SHSET}{vlaunch_var}={vlaunch_val}\n" + step["run"]["cmd"]

        # fill in missing merlin section defaults
        MerlinSpec.fill_missing_defaults(self.merlin, defaults.MERLIN["merlin"])
        if self.merlin["resources"]["workers"] is None:
            self.merlin["resources"]["workers"] = {"default_worker": defaults.WORKER}
        else:
            # Gather a list of step names defined in the study
            all_workflow_steps = self.get_study_step_names()
            # Create a variable to track the steps assigned to workers
            worker_steps = []

            # Loop through each worker and fill in the defaults
            for _, worker_settings in self.merlin["resources"]["workers"].items():
                MerlinSpec.fill_missing_defaults(worker_settings, defaults.WORKER)
                worker_steps.extend(worker_settings["steps"])

            if "all" in worker_steps:
                steps_that_need_workers = []
            else:
                # Figure out which steps still need workers
                steps_that_need_workers = list(set(all_workflow_steps) - set(worker_steps))

            # If there are still steps remaining that haven't been assigned a worker yet,
            # assign the remaining steps to the default worker. If all the steps still need workers
            # (i.e. no workers were assigned) then default workers' steps should be "all" so we skip this
            if steps_that_need_workers and (steps_that_need_workers != all_workflow_steps):
                self.merlin["resources"]["workers"]["default_worker"] = defaults.WORKER
                self.merlin["resources"]["workers"]["default_worker"]["steps"] = steps_that_need_workers
        if self.merlin["samples"] is not None:
            MerlinSpec.fill_missing_defaults(self.merlin["samples"], defaults.SAMPLES)

        # no defaults for user block

    @staticmethod
    def fill_missing_defaults(object_to_update: Dict, default_dict: Dict):
        """
        Merge default values into an object, filling in missing attributes.

        This static method takes an object and a dictionary of default values,
        and merges the defaults into the object. It only adds missing attributes
        to the object and does not overwrite any existing attributes. If an
        attribute is present in the object but its value is `None`, it will be
        updated with the corresponding value from the defaults.

        The method works recursively, allowing for nested dictionaries.

        The method modifies the `object_to_update` in place.

        Args:
            object_to_update: The object (as a dictionary) that needs to be
                updated with default values.
            default_dict: A dictionary containing default values to merge into
                the object.

        Example:
            ```python
            >>> obj = {'a': 1, 'b': None}
            >>> defaults = {'a': 2, 'b': 3, 'c': 4}
            >>> fill_missing_defaults(obj, defaults)
            >>> print(obj)
            {'a': 1, 'b': 3, 'c': 4}
            ```
        """

        def recurse(result: Dict, recurse_defaults: Dict):
            """
            Recursively merge default values into the result object.

            This helper function checks if the current level of the `recurse_defaults`
            dictionary is a dictionary itself. If it is, it iterates through each key-value
            pair. If a key is not present in the `result` or its value is `None`, it
            assigns the value from `recurse_defaults`. If the key exists and has a value,
            it recursively calls itself to handle nested dictionaries.

            The function modifies the `result` in place.

            Args:
                result: The current state of the object being updated.
                recurse_defaults: The current level of defaults to merge.
            """
            if not isinstance(recurse_defaults, dict):
                return
            for key, val in recurse_defaults.items():
                # fmt: off
                if (key not in result) or (
                    (result[key] is None) and (recurse_defaults[key] is not None)
                ):
                    result[key] = val
                else:
                    recurse(result[key], val)
                # fmt: on

        recurse(object_to_update, default_dict)

    # ***Unsure if this method is still needed after adding json schema verification***
    def warn_unrecognized_keys(self):
        """
        Check for unrecognized keys in the specification file.

        This method verifies that all keys present in the specification file
        conform to the expected structure defined by the `MerlinSpec` class.
        It checks various sections of the specification, including "description",
        "batch", "env", "global parameters", "steps", and "merlin". For each
        section, it calls the `check_section` method to ensure that the keys
        are recognized and valid according to predefined criteria.
        """
        # check description
        MerlinSpec.check_section("description", self.description, all_keys.DESCRIPTION)

        # check batch
        MerlinSpec.check_section("batch", self.batch, all_keys.BATCH)

        # check env
        MerlinSpec.check_section("env", self.environment, all_keys.ENV)

        # check parameters
        for _, contents in self.globals.items():
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
    def check_section(section_name: str, section: Dict, known_keys: Set[str]):
        """
        Check a section of the specification file for unrecognized keys.

        This static method compares the keys present in a specified section
        of the specification file against a set of known keys. If any keys
        are found that are not recognized, a warning is logged indicating
        the unrecognized key and the section in which it was found.

        Args:
            section_name: The name of the section being checked.
            section: The section of the specification file to validate.
            known_keys: A set of keys that are recognized as valid for
                the specified section.
        """
        diff = set(section.keys()).difference(known_keys)

        # TODO: Maybe add a check here for required keys

        for extra in diff:
            LOG.warning(f"Unrecognized key '{extra}' found in spec section '{section_name}'.")

    def dump(self) -> str:
        """
        Dump the `MerlinSpec` instance to a formatted YAML string.

        This method converts the current state of the `MerlinSpec` instance
        into a YAML formatted string. It utilizes the `_dict_to_yaml`
        method to handle the conversion and prettification of the data.
        Additionally, it ensures that the resulting YAML string is valid
        by attempting to parse it with `yaml.safe_load`. If parsing fails,
        a ValueError is raised with details about the error.

        Returns:
            A pretty formatted YAML string representation of the
                `MerlinSpec` instance.

        Raises:
            ValueError: If there is an error while parsing the YAML string.
        """
        tab = 3 * " "
        result = self._dict_to_yaml(self.yaml_sections, "", [], tab)
        while "\n\n\n" in result:
            result = result.replace("\n\n\n", "\n\n")
        try:
            yaml.safe_load(result)
        except Exception as e:  # pylint: disable=C0103
            raise ValueError(f"Error parsing provenance spec:\n{e}") from e
        return result

    def _dict_to_yaml(self, obj: Any, string: str, key_stack: List[str], tab: int) -> str:
        """
        Convert a Python object to a formatted YAML string.

        This private method handles the conversion of various Python data
        types (strings, booleans, lists, and dictionaries) into a
        formatted YAML string. It uses an if-else structure to determine
        the type of the input object and calls the appropriate processing
        methods for each type. The method also manages indentation based
        on the current level of nesting.

        Args:
            obj: The object to convert to YAML format.
            string: The current string representation being built.
            key_stack: A stack of keys representing the current level of
                nesting in the YAML structure.
            tab: The number of spaces to use for indentation.

        Returns:
            A formatted YAML string representation of the input object.
        """
        if obj is None:
            return ""

        lvl = len(key_stack) - 1

        if isinstance(obj, str):
            return self._process_string(obj, lvl, tab)
        if isinstance(obj, bool):
            return str(obj).lower()
        if isinstance(obj, list):
            return self._process_list(obj, string, key_stack, lvl, tab)
        if isinstance(obj, dict):
            return self._process_dict(obj, string, key_stack, lvl, tab)
        return obj

    def _process_string(self, obj: str, lvl: int, tab: int) -> str:
        """
        Process a string for YAML formatting in the dump method.

        This private method takes a string and formats it for inclusion
        in a YAML output. If the string contains multiple lines, it
        transforms the string into a block scalar format using the pipe
        (`|`) character, which is suitable for YAML representation.
        The indentation is adjusted based on the current level of
        nesting.

        Args:
            obj: The string to be processed.
            lvl: The current level of indentation for the YAML output.
            tab: The number of spaces to use for indentation.

        Returns:
            The formatted string ready for YAML output.
        """
        split = obj.splitlines()
        if len(split) > 1:
            obj = "|\n" + tab * (lvl + 1) + ("\n" + tab * (lvl + 1)).join(split)
        return obj

    def _process_list(
        self,
        obj: List[Any],
        string: str,
        key_stack: List[str],
        lvl: int,
        tab: int,
    ) -> str:
        """
        Process a list for YAML formatting in the dump method.

        This private method handles the conversion of a list into a
        YAML formatted string. It determines whether to use hyphens
        for list items based on the context provided by the key stack.
        The method recursively processes each element in the list and
        manages indentation based on the current level of nesting.

        Args:
            obj: The list to be processed.
            string: The current string representation being built.
            key_stack: A stack of keys representing the current
                level of nesting in the YAML structure.
            lvl: The current level of indentation for the YAML output.
            tab: The number of spaces to use for indentation.

        Returns:
            A formatted YAML string representation of the input list.
        """
        num_entries = len(obj)
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
                string += str(self._dict_to_yaml(elem, "", key_stack, tab))
                if num_entries > 1 and i != len(obj) - 1:
                    string += ", "
            key_stack.pop()
        if not use_hyphens:
            string += "]"
        return string

    def _process_dict(
        self,
        obj: Dict,
        string: str,
        key_stack: List[str],
        lvl: int,
        tab: int,
    ) -> str:  # pylint: disable=R0913
        """
        Process a dictionary for YAML formatting in the dump method.

        This private method converts a dictionary into a YAML formatted
        string. It iterates over the dictionary's key-value pairs,
        formatting each pair according to YAML syntax. The method
        handles indentation and manages the key stack to maintain the
        correct nesting level in the output.

        Args:
            obj: The dictionary to be processed.
            string: The current string representation being built.
            key_stack: A stack of keys representing the current
                level of nesting in the YAML structure.
            lvl: The current level of indentation for the YAML output.
            tab: The number of spaces to use for indentation.

        Returns:
            A formatted YAML string representation of the input dictionary.
        """
        list_offset = 2 * " "
        if len(key_stack) > 0 and key_stack[-1] != "elem":
            string += "\n"
        i = 0
        for key, val in obj.items():
            key_stack = deepcopy(key_stack)
            key_stack.append(key)
            if len(key_stack) > 1 and key_stack[-2] == "elem" and i == 0:
                # string += (tab * (lvl - 1))
                string += ""
            elif "elem" in key_stack:
                string += list_offset + (tab * lvl)
            else:
                string += tab * (lvl + 1)
            string += str(key) + ": " + str(self._dict_to_yaml(val, "", key_stack, tab)) + "\n"
            key_stack.pop()
            i += 1
        return string

    def get_step_worker_map(self) -> Dict[str, List[str]]:
        """
        Create a mapping of step names to associated workers.

        This method constructs a dictionary where each key is a step name
        and the corresponding value is a list of workers assigned to that
        step. Workers can either be associated with all steps or with
        specific steps. This method serves as the inverse of the
        [`get_worker_step_map`][spec.specification.MerlinSpec.get_worker_step_map]
        method.

        Returns:
            A dictionary mapping step names to lists of worker names
                associated with each step.
        """
        steps = self.get_study_step_names()
        step_worker_map = {step_name: [] for step_name in steps}
        for worker_name, worker_val in self.merlin["resources"]["workers"].items():
            # Case 1: worker doesn't have specific steps
            if "all" in worker_val["steps"]:
                for step_name in step_worker_map:
                    step_worker_map[step_name].append(worker_name)
            # Case 2: worker has specific steps
            else:
                for step in worker_val["steps"]:
                    step_worker_map[step].append(worker_name)
        return step_worker_map

    def get_worker_step_map(self) -> Dict[str, List[str]]:
        """
        Create a mapping of worker names to associated steps.

        This method constructs a dictionary where each key is a worker name
        and the corresponding value is a list of steps that the worker is
        assigned to monitor. Workers can either be assigned to all steps or
        to specific steps. It serves as the inverse of the
        [`get_step_worker_map`][spec.specification.MerlinSpec.get_step_worker_map]
        method.

        Returns:
            A dictionary mapping worker names to lists of step names that each
                worker monitors.
        """
        worker_step_map = {}
        steps = self.get_study_step_names()
        for worker_name, worker_val in self.merlin["resources"]["workers"].items():
            # Case 1: worker doesn't have specific steps
            if "all" in worker_val["steps"]:
                worker_step_map[worker_name] = steps
            # Case 2: worker has specific steps
            else:
                worker_step_map[worker_name] = []
                for step in worker_val["steps"]:
                    worker_step_map[worker_name].append(step)
        return worker_step_map

    def get_task_queues(self, omit_tag: bool = False) -> Dict[str, str]:
        """
        Create a mapping of steps to their corresponding task queues.

        This method constructs a dictionary where each key is a step name
        and the corresponding value is the associated task queue. The
        `omit_tag` parameter allows for the optional exclusion of the Celery
        queue tag from the queue names. It serves as the inverse of the
        [`get_queue_step_relationship`][spec.specification.MerlinSpec.get_queue_step_relationship]
        method.

        Args:
            omit_tag: If True, the Celery queue tag will be omitted
                from the task queue names. Default is False.

        Returns:
            A dictionary mapping step names to their corresponding task queues.
        """
        from merlin.config.configfile import CONFIG  # pylint: disable=C0415

        steps = self.get_study_steps()
        queues = {}
        for step in steps:
            if "task_queue" in step.run and (omit_tag or CONFIG.celery.omit_queue_tag):
                queues[step.name] = step.run["task_queue"]
            elif "task_queue" in step.run:
                queues[step.name] = CONFIG.celery.queue_tag + step.run["task_queue"]
        return queues

    def get_queue_step_relationship(self) -> Dict[str, List[str]]:
        """
        Build a mapping of task queues to their associated steps.

        This method constructs a dictionary where each key is a task queue
        name and the corresponding value is a list of steps that are
        associated with that queue. It serves as the inverse of the
        [`get_task_queues`][spec.specification.MerlinSpec.get_task_queues]
        method.

        Returns:
            A dictionary mapping task queue names to lists of step names
                associated with each queue.
        """
        from merlin.config.configfile import CONFIG  # pylint: disable=C0415

        steps = self.get_study_steps()
        relationship_tracker = {}

        for step in steps:
            if "task_queue" in step.run:
                queue_name = (
                    step.run["task_queue"]
                    if CONFIG.celery.omit_queue_tag
                    else f"{CONFIG.celery.queue_tag}{step.run['task_queue']}"
                )

                if queue_name in relationship_tracker:
                    relationship_tracker[queue_name].append(step.name)
                else:
                    relationship_tracker[queue_name] = [step.name]

        return relationship_tracker

    def get_queue_list(self, steps: Union[List[str], str], omit_tag: bool = False) -> Set[str]:
        """
        Return a sorted set of queues corresponding to specified steps.

        This method retrieves a list of task queues associated with the
        given steps. If the `steps` parameter is set to ['all'], it will
        return all available queues. The `omit_tag` parameter allows for
        the optional exclusion of the Celery queue tag from the queue names.

        Args:
            steps: A list of step names or a list containing the string 'all'
                to represent all steps, or the name of a single step.
            omit_tag: If True, the Celery queue tag will be omitted from the
                task queue names.

        Returns:
            A sorted set of unique task queues corresponding to the specified
                steps.

        Raises:
            KeyError: If any of the specified steps do not exist in the
                task queues.
        """
        queues = self.get_task_queues(omit_tag=omit_tag)
        if steps[0] == "all":
            task_queues = queues.values()
        else:
            try:
                if isinstance(steps, list):
                    task_queues = [queues[step] for step in steps]
                else:
                    task_queues = [queues[steps]]
            except KeyError:
                newline = "\n"
                LOG.error(f"Invalid steps '{steps}'! Try one of these (or 'all'):\n{newline.join(queues.keys())}")
                raise
        return sorted(set(task_queues))

    def make_queue_string(self, steps: List[str]) -> str:
        """
        Return a unique queue string for the specified steps.

        This method constructs a comma-separated string of unique task
        queues associated with the provided steps. The resulting string
        is suitable for use in command-line contexts.

        Args:
            steps: A list of step names for which to generate the
                queue string.

        Returns:
            A quoted string of unique task queues, separated by commas.
        """
        queues = ",".join(set(self.get_queue_list(steps)))
        return shlex.quote(queues)

    def get_worker_names(self) -> List[str]:
        """
        Build a list of worker names.

        This method retrieves the names of all workers defined in the
        Merlin resources and returns them as a list.

        Returns:
            A list of worker names.
        """
        result = []
        for worker in self.merlin["resources"]["workers"]:
            result.append(worker)
        return result

    def get_tasks_per_step(self) -> Dict[str, int]:
        """
        Get the number of tasks needed to complete each step.

        This method calculates the number of tasks required for each
        step in the study based on the number of samples and parameters.
        It returns a dictionary where the keys are the step names and
        the values are the corresponding number of tasks required for
        that step.

        Returns:
            A dictionary mapping step names to the number of tasks
                required for each step.
        """
        # Get the number of samples used
        samples = []
        if self.merlin["samples"] and self.merlin["samples"]["file"]:
            samples = load_array_file(self.merlin["samples"]["file"])
        num_samples = len(samples)

        # Get the column labels, the parameter labels, the number of parameters, and the steps in the study
        if num_samples > 0:
            column_labels = self.merlin["samples"]["column_labels"]
        parameter_labels = list(self.get_parameters().labels.keys())
        num_params = self.get_parameters().length
        study_steps = self.get_study_steps()

        tasks_per_step = {}
        for step in study_steps:
            cmd = step.__dict__["run"]["cmd"]
            restart_cmd = step.__dict__["run"]["restart"]

            # Default number of tasks for a step is 1
            tasks_per_step[step.name] = 1

            # If this step uses parameters, we'll at least have a num_params number of tasks to complete
            if needs_merlin_expansion(cmd, restart_cmd, parameter_labels, include_sample_keywords=False):
                tasks_per_step[step.name] = num_params

            # If merlin expansion is needed with column labels, this step uses samples
            if num_samples > 0 and needs_merlin_expansion(cmd, restart_cmd, column_labels):
                tasks_per_step[step.name] *= num_samples

        return tasks_per_step

    def _create_param_maps(self, param_gen: ParameterGenerator, expanded_labels: Dict, label_param_map: Dict):
        """
        Create mappings of tokens to expanded labels and labels to parameter values.

        This private method processes a parameter generator to create two mappings:

        1. `expanded_labels`: Maps tokens to their expanded labels based on the
            provided parameter values.
        2. `label_param_map`: Maps expanded labels to their corresponding parameter
            values.

        The expected structure for the parameter block is:

        ```
        global.parameters:
            TOKEN:
                values: [param_val_1, param_val_2]
                label: label.%%
        ```

        Args:
            param_gen: A `ParameterGenerator` object from Maestro containing the
                parameter definitions.
            expanded_labels: A dictionary to store the mapping from tokens to their
                expanded labels.
            label_param_map: A dictionary to store the mapping from labels to their
                corresponding parameter values.
        """
        for token, orig_label in param_gen.labels.items():
            for param in param_gen.parameters[token]:
                expanded_label = orig_label.replace(param_gen.label_token, str(param))
                if token in expanded_labels:
                    expanded_labels[token].append(expanded_label)
                else:
                    expanded_labels[token] = [expanded_label]
                label_param_map[expanded_label] = {token: param}

    def get_step_param_map(self) -> Dict:  # pylint: disable=R0914
        """
        Create a mapping of parameters used for each step in the study.

        This method generates a mapping of parameters for each step, where each
        step may have a command (`cmd`) and a restart command (`restart_cmd`).
        The resulting mapping has a structure similar to the following:

        ```python
        step_name_with_parameters: {
            "cmd": {
                TOKEN_1: param_1_value_1,
                TOKEN_2: param_2_value_1,
            },
            "restart_cmd": {
                TOKEN_1: param_1_value_1,
                TOKEN_3: param_3_value_1,
            }
        }
        ```

        Returns:
            A dictionary mapping step names (with parameters) to their
                respective command and restart command parameter mappings.
        """
        # Get the steps and the parameters in the study
        study_steps = self.get_study_steps()
        param_gen = self.get_parameters()

        # Create maps between tokens and expanded labels, and between labels and parameter values
        expanded_labels = {}
        label_param_map = {}
        self._create_param_maps(param_gen, expanded_labels, label_param_map)

        step_param_map = {}
        for step in study_steps:
            # Get the cmd and restart cmd for the step
            cmd = step.__dict__["run"]["cmd"]
            restart_cmd = step.__dict__["run"]["restart"]

            # Get the parameters used in this step and the labels used with those parameters
            all_params_in_step = param_gen.get_used_parameters(step)
            labels_used = [expanded_labels[param] for param in sorted(all_params_in_step)]

            # Zip all labels used for the step together (since this is how steps are named in Maestro)
            for labels in zip(*labels_used):
                # Initialize the entry in the step param map
                param_str = ".".join(labels)
                step_name_with_params = f"{step.name}_{param_str}"
                step_param_map[step_name_with_params] = {"cmd": {}, "restart_cmd": {}}

                # Populate the entry in the step param map based on which token is found in which command (cmd or restart)
                for label in labels:
                    for token, param_value in label_param_map[label].items():
                        full_token = f"{param_gen.token}({token})"
                        if full_token in cmd:
                            step_param_map[step_name_with_params]["cmd"][token] = param_value
                        if full_token in restart_cmd:
                            step_param_map[step_name_with_params]["restart_cmd"][token] = param_value

        return step_param_map
