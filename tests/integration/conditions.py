import os
from abc import ABC, abstractmethod
from glob import glob
from re import search


# TODO when moving command line tests to pytest, change Condition boolean returns to assertions
class Condition(ABC):
    def ingest_info(self, info):
        """
        This function allows child classes of Condition
        to take in data AFTER a test is run.
        """
        for key, val in info.items():
            setattr(self, key, val)

    @abstractmethod
    def passes(self):
        pass


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
            return not self.is_within(self.stdout)
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
        self.dirpath_glob = f"{self.output_path}/{self.study_name}" f"_[0-9]*-[0-9]*"

    def glob(self, glob_string):
        candidates = glob(glob_string)
        if isinstance(candidates, list):
            return sorted(candidates)[-1]
        return candidates


class StepFileExists(StudyOutputAware):
    """
    A StudyOutputAware that checks for a particular file's existence.
    """

    def __init__(self, step, filename, study_name, output_path, params=False):
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

    def __str__(self):
        return f"{__class__.__name__} expected to find file '{self.glob_string}', but file did not exist"

    @property
    def glob_string(self):
        param_glob = ""
        if self.params:
            param_glob = "*/"
        return f"{self.dirpath_glob}/{self.step}/{param_glob}{self.filename}"

    def file_exists(self):
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

    def __init__(self, step, filename, study_name, output_path, regex):
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
        return f"{__class__.__name__} expected to find '{self.regex}' regex match in file '{self.glob_string}', but match was not found"

    @property
    def glob_string(self):
        return f"{self.dirpath_glob}/{self.step}/{self.filename}"

    def contains(self):
        glob_string = self.glob_string
        try:
            filename = self.glob(glob_string)
            with open(filename, "r") as textfile:
                filetext = textfile.read()
            return self.is_within(filetext)
        except Exception:
            return False

    def is_within(self, text):
        """
        :param `text`: text in which to search for a regex match
        """
        return search(self.regex, text) is not None

    @property
    def passes(self):
        return self.contains()


class ProvenanceYAMLFileHasRegex(HasRegex):
    """
    A condition that a Merlin provenance yaml spec in the 'merlin_info' directory
    MUST contain a given regular expression.
    """

    def __init__(self, regex, name, output_path, provenance_type, negate=False):
        """
        :param `regex`: a string regex pattern
        :param `name`: the name of a study
        :param `output_path`: the $(OUTPUT_PATH) of a study
        """
        super().__init__(regex, negate=negate)
        self.name = name
        self.output_path = output_path
        provenance_types = ["orig", "partial", "expanded"]
        if provenance_type not in provenance_types:
            raise ValueError(
                f"Invalid provenance_type '{provenance_type}' in ProvenanceYAMLFileHasRegex! Options: {provenance_types}"
            )
        self.prov_type = provenance_type

    def __str__(self):
        if self.negate:
            return f"{__class__.__name__} expected to find no '{self.regex}' regex match in provenance spec '{self.glob_string}', but match was found"
        return f"{__class__.__name__} expected to find '{self.regex}' regex match in provenance spec '{self.glob_string}', but match was not found"

    @property
    def glob_string(self):
        return (
            f"{self.output_path}/{self.name}"
            f"_[0-9]*-[0-9]*/merlin_info/{self.name}.{self.prov_type}.yaml"
        )

    def is_within(self):
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
