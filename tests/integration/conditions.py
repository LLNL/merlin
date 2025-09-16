##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module defines the different conditions to test against."""
import os
from abc import ABC, abstractmethod
from glob import glob
from re import search


class Condition(ABC):
    """Abstract Condition class that other conditions will inherit from"""

    def ingest_info(self, info):
        """
        This function allows child classes of Condition
        to take in data AFTER a test is run.
        """
        for key, val in info.items():
            setattr(self, key, val)

    @property
    @abstractmethod
    def passes(self):
        """The method that will check if the test passes or not"""
        raise NotImplementedError("The 'passes' property should be defined in all Condition subclasses.")


# pylint: disable=no-member
class HasReturnCode(Condition):
    """
    A condition that some process must return 0
    as its return code.
    """

    def __init__(self, expected_code=0):
        """
        :param `expected_code`: the expected return code
        """
        self.expected_code = expected_code

    def __str__(self):
        return f"{__class__.__name__} expected {self.expected_code} but got {self.return_code}"

    @property
    def passes(self):
        return self.return_code == self.expected_code


class HasNoStdErr(Condition):
    """
    A condition that some process have an empty
    stderr string.
    """

    def __str__(self):
        return f"{__class__.__name__} expected empty stderr but stderr was non-empty (see --verbose)"

    @property
    def passes(self):
        return self.stderr == ""


class HasRegex(Condition):
    """
    A condition that some body of text MUST match a
    given regular expression. Defaults to stdout.
    """

    def __init__(self, regex, negate=False):
        """
        :param `regex`: a string regex pattern
        """
        self.regex = regex
        self.negate = negate

    def __str__(self):
        if self.negate:
            return f"{__class__.__name__} expected no '{self.regex}' regex match, but match was found."
        return f"{__class__.__name__} expected '{self.regex}' regex match, but match was not found."

    def is_within(self, text):
        """
        :param `text`: text in which to search for a regex match
        """
        return search(self.regex, text) is not None

    @property
    def passes(self):
        if self.negate:
            return not self.is_within(self.stdout) and not self.is_within(self.stderr)
        return self.is_within(self.stdout) or self.is_within(self.stderr)


class StudyOutputAware(Condition):
    """
    An abstract condition that is aware of a study's name and output path.
    """

    def __init__(self, study_name, output_path):
        """
        :param `study_name`: the name of a study
        :param `output_path`: the $(OUTPUT_PATH) of a study
        """
        self.study_name = study_name
        self.output_path = output_path
        self.dirpath_glob = os.path.join(self.output_path, f"{self.study_name}_[0-9]*-[0-9]*")

    def glob(self, glob_string):
        """
        Returns a regex string for the glob library to recursively find files with.
        """
        candidates = glob(glob_string)
        if isinstance(candidates, list):
            return sorted(candidates)[-1]
        return candidates

    @property
    @abstractmethod
    def passes(self):
        """The method that will check if the test passes or not"""
        raise NotImplementedError("The 'passes' property should be defined in all StudyOutputAware subclasses.")


class StepFileExists(StudyOutputAware):
    """
    A StudyOutputAware that checks for a particular file's existence.
    """

    def __init__(self, step, filename, study_name, output_path, params=False, samples=False):  # pylint: disable=R0913
        """
        :param `step`: the name of a step
        :param `filename`: name of file to search for in step's workspace directory
        :param `study_name`: the name of a study
        :param `output_path`: the $(OUTPUT_PATH) of a study
        """
        super().__init__(study_name, output_path)
        self.step = step
        self.filename = filename
        self.params = params
        self.samples = samples

    def __str__(self):
        return f"{__class__.__name__} expected to find file '{self.glob_string}', but file did not exist"

    @property
    def glob_string(self):
        """
        Returns a regex string for the glob library to recursively find files with.
        """
        param_glob = "*" if self.params else ""
        samples_glob = "**" if self.samples else ""
        return os.path.join(self.dirpath_glob, self.step, param_glob, samples_glob, self.filename)

    def file_exists(self):
        """Check if the file path created by glob_string exists"""
        glob_string = self.glob_string
        try:
            filename = self.glob(glob_string)
        except IndexError:
            return False
        return os.path.isfile(filename)

    @property
    def passes(self):
        return self.file_exists()


class StepFileHasRegex(StudyOutputAware):
    """
    A StudyOutputAware that checks that a particular file contains a regex.
    """

    def __init__(self, step, filename, study_name, output_path, regex):  # pylint: disable=R0913
        """
        :param `step`: the name of a step
        :param `filename`: name of file to search for in step's workspace directory
        :param `study_name`: the name of a study
        :param `output_path`: the $(OUTPUT_PATH) of a study
        """
        super().__init__(study_name, output_path)
        self.step = step
        self.filename = filename
        self.regex = regex

    def __str__(self):
        return f"""{__class__.__name__} expected to find '{self.regex}'
        regex match in file '{self.glob_string}', but match was not found"""

    @property
    def glob_string(self):
        """
        Returns a regex string for the glob library to recursively find files with.
        """
        return f"{self.dirpath_glob}/{self.step}/{self.filename}"

    def contains(self):
        """See if the regex is within the filetext"""
        glob_string = self.glob_string
        try:
            filename = self.glob(glob_string)
            with open(filename, "r") as textfile:
                filetext = textfile.read()
            return self.is_within(filetext)
        except Exception:  # pylint: disable=broad-except
            return False

    def is_within(self, text):
        """
        :param `text`: text in which to search for a regex match
        """
        return search(self.regex, text) is not None

    @property
    def passes(self):
        return self.contains()


# TODO when writing API docs for tests make sure this looks correct and has functioning links
# - Do we want to list expected_count, glob_string, and passes as methods since they're already attributes?
class StepFinishedFilesCount(StudyOutputAware):
    """
    A [`StudyOutputAware`][integration.conditions.StudyOutputAware] that checks for the
    exact number of `MERLIN_FINISHED` files in a specified step's output directory based
    on the number of parameters and samples.

    Attributes:
        step: The name of the step to check.
        study_name: The name of the study.
        output_path: The output path of the study.
        num_parameters: The number of parameters for the step.
        num_samples: The number of samples for the step.
        expected_count: The expected number of `MERLIN_FINISHED` files based on parameters and samples or explicitly set.
        glob_string: The glob pattern to find `MERLIN_FINISHED` files in the specified step's output directory.
        passes: Checks if the count of `MERLIN_FINISHED` files matches the expected count.

    Methods:
        expected_count: Calculates the expected number of `MERLIN_FINISHED` files.
        glob_string: Constructs the glob pattern for searching `MERLIN_FINISHED` files.
        count_finished_files: Counts the number of `MERLIN_FINISHED` files found.
        passes: Checks if the count of `MERLIN_FINISHED` files matches the expected count.
    """

    # All of these parameters are necessary for this Condition so we'll ignore pylint
    def __init__(
        self,
        step: str,
        study_name: str,
        output_path: str,
        num_parameters: int = 0,
        num_samples: int = 0,
        expected_count: int = None,
    ):  # pylint: disable=too-many-arguments
        super().__init__(study_name, output_path)
        self.step = step
        self.num_parameters = num_parameters
        self.num_samples = num_samples
        self._expected_count = expected_count

    @property
    def expected_count(self) -> int:
        """
        Calculate the expected number of `MERLIN_FINISHED` files.

        Returns:
            The expected number of `MERLIN_FINISHED` files.
        """
        # Return the explicitly set expected count if given
        if self._expected_count is not None:
            return self._expected_count

        # Otherwise calculate the correct number of MERLIN_FINISHED files to expect
        if self.num_parameters > 0 and self.num_samples > 0:
            return self.num_parameters * self.num_samples
        if self.num_parameters > 0:
            return self.num_parameters
        if self.num_samples > 0:
            return self.num_samples

        return 1  # Default case when there are no parameters or samples

    @property
    def glob_string(self) -> str:
        """
        Glob pattern to find `MERLIN_FINISHED` files in the specified step's output directory.

        Returns:
            A glob pattern to find `MERLIN_FINISHED` files.
        """
        param_glob = "*" if self.num_parameters > 0 else ""
        samples_glob = "**" if self.num_samples > 0 else ""
        return os.path.join(self.dirpath_glob, self.step, param_glob, samples_glob, "MERLIN_FINISHED")

    def count_finished_files(self) -> int:
        """
        Count the number of `MERLIN_FINISHED` files found.

        Returns:
            The actual number of `MERLIN_FINISHED` files that exist in the step's output directory.
        """
        finished_files = glob(self.glob_string)  # Adjust the glob pattern as needed
        return len(finished_files)

    @property
    def passes(self) -> bool:
        """
        Check if the count of `MERLIN_FINISHED` files matches the expected count.

        Returns:
            True if the expected count matches the actual count. False otherwise.
        """
        return self.count_finished_files() == self.expected_count

    def __str__(self) -> str:
        return (
            f"{__class__.__name__} expected {self.expected_count} `MERLIN_FINISHED` "
            f"files, but found {self.count_finished_files()}"
        )


class ProvenanceYAMLFileHasRegex(HasRegex):
    """
    A condition that a Merlin provenance yaml spec in the 'merlin_info' directory
    MUST contain a given regular expression.
    """

    def __init__(self, regex, spec_file_name, study_name, output_path, provenance_type, negate=False):  # pylint: disable=R0913
        """
        :param `regex`: a string regex pattern
        :param `spec_file_name`: the name of the spec file
        :param `study_name`: the name of a study
        :param `output_path`: the $(OUTPUT_PATH) of a study
        """
        super().__init__(regex, negate=negate)
        self.spec_file_name = spec_file_name
        self.study_name = study_name
        self.output_path = output_path
        provenance_types = ["orig", "partial", "expanded"]
        if provenance_type not in provenance_types:
            raise ValueError(
                f"Invalid provenance_type '{provenance_type}' in ProvenanceYAMLFileHasRegex! Options: {provenance_types}"
            )
        self.prov_type = provenance_type

    def __str__(self):
        if self.negate:
            return f"""{__class__.__name__} expected to find no '{self.regex}'
            regex match in provenance spec '{self.glob_string}', but match was found"""
        return f"""{__class__.__name__} expected to find '{self.regex}'
        regex match in provenance spec '{self.glob_string}', but match was not found"""

    @property
    def glob_string(self):
        """
        Returns a regex string for the glob library to recursively find files with.
        """
        return (
            f"{self.output_path}/{self.study_name}" f"_[0-9]*-[0-9]*/merlin_info/{self.spec_file_name}.{self.prov_type}.yaml"
        )

    def is_within(self):  # pylint: disable=W0221
        """
        Uses glob to find the correct provenance yaml spec.
        Returns True if that file contains a match to this
        object's self.regex string.
        """
        filepath = self.glob_string
        filename = sorted(glob(filepath))[-1]
        with open(filename, "r") as _file:
            text = _file.read()
            return super().is_within(text)

    @property
    def passes(self):
        if self.negate:
            return not self.is_within()
        return self.is_within()


class PathExists(Condition):
    """
    A condition for checking if a path to a file or directory exists
    """

    def __init__(self, pathname, negate=False) -> None:
        self.pathname = pathname
        self.negate = negate

    def path_exists(self) -> bool:
        """Check if a path exists"""
        return os.path.exists(self.pathname)

    def __str__(self) -> str:
        return f"{__class__.__name__} expected to find file or directory at {self.pathname}"

    @property
    def passes(self):
        return not self.path_exists() if self.negate else self.path_exists()


class FileHasRegex(Condition):
    """
    A condition that some body of text within a file
    MUST match a given regular expression.
    """

    def __init__(self, filename, regex) -> None:
        self.filename = filename
        self.regex = regex

    def contains(self) -> bool:
        """Checks if the regex matches anywhere in the filetext"""
        try:
            with open(self.filename, "r") as f:  # pylint: disable=C0103
                filetext = f.read()
            return self.is_within(filetext)
        except Exception:  # pylint: disable=broad-except
            return False

    def is_within(self, text):
        """Check if there's a match for the regex in text"""
        return search(self.regex, text) is not None

    def __str__(self) -> str:
        return f"""{__class__.__name__} expected to find {self.regex}
        regex match within {self.filename} file but no match was found"""

    @property
    def passes(self):
        return self.contains()


class FileHasNoRegex(FileHasRegex):
    """
    A condition that some body of text within a file
    MUST NOT match a given regular expression.
    """

    def __str__(self) -> str:
        return f"""{__class__.__name__} expected to find {self.regex}
        regex to not match within {self.filename} file but a match was found"""

    @property
    def passes(self):
        return not self.contains()
