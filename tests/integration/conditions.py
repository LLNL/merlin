import os
from glob import glob
from re import search


class Condition:
    def ingest_info(self, info):
        """
        This function allows child classes of Condition
        to take in data AFTER a test is run.
        """
        for key, val in info.items():
            setattr(self, key, val)

    @property
    def passes(self):
        print("Extend this class!")
        return False


class ReturnCodeCond(Condition):
    """
    A condition that some process must return 0
    as its return code.
    """

    def __init__(self, expected_code=0):
        """
        :param `expected_code`: the expected return code
        """
        self.expected_code = expected_code

    @property
    def passes(self):
        return self.return_code == self.expected_code


class NoStderrCond(Condition):
    """
    A condition that some process have an empty
    stderr string.
    """

    @property
    def passes(self):
        return self.stderr == ""


class RegexCond(Condition):
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


class StudyCond(Condition):
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


class StepFileExistsCond(StudyCond):
    """
    A StudyCond that checks for a particular file's existence.
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

    def file_exists(self):
        param_glob = ""
        if self.params:
            param_glob = "*/"
        glob_string = f"{self.dirpath_glob}/{self.step}/{param_glob}{self.filename}"
        try:
            filename = self.glob(glob_string)
        except IndexError:
            return False
        return os.path.isfile(filename)

    @property
    def passes(self):
        return self.file_exists()


class StepFileContainsCond(StudyCond):
    """
    A StudyCond that checks that a particular file contains a regex.
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

    def contains(self):
        glob_string = f"{self.dirpath_glob}/{self.step}/{self.filename}"
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


class ProvenanceCond(RegexCond):
    """
    A condition that a Merlin provenance yaml spec
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
        if provenance_type not in ["orig", "partial", "expanded"]:
            raise ValueError(
                f"Bad provenance_type '{provenance_type}' in ProvenanceCond!"
            )
        self.prov_type = provenance_type

    def is_within(self):
        """
        Uses glob to find the correct provenance yaml spec.
        Returns True if that file contains a match to this
        object's self.regex string.
        """
        filepath = (
            f"{self.output_path}/{self.name}"
            f"_[0-9]*-[0-9]*/merlin_info/{self.name}.{self.prov_type}.yaml"
        )
        filename = sorted(glob(filepath))[-1]
        with open(filename, "r") as _file:
            text = _file.read()
            return super().is_within(text)

    @property
    def passes(self):
        if self.negate:
            return not self.is_within()
        return self.is_within()
