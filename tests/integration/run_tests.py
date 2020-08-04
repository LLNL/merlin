###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
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
Script for running Merlin command line interface tests.
Built for 1) manual use and 2) continuous integration.
"""
import argparse
import os
import shutil
import sys
import time
from contextlib import suppress
from glob import glob
from re import search
from subprocess import PIPE, Popen

from merlin.utils import get_flux_cmd


OUTPUT_DIR = "cli_test_studies"


def run_single_test(name, test, test_label="", buffer_length=50):
    dot_length = buffer_length - len(name) - len(str(test_label))
    print(f"TEST {test_label}: {name}{'.'*dot_length}", end="")
    command = test[0]
    conditions = test[1]
    if not isinstance(conditions, list):
        conditions = [conditions]

    start_time = time.time()
    process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = process.communicate()
    end_time = time.time()
    total_time = end_time - start_time
    if stdout is not None:
        stdout = stdout.decode("utf-8")
    if stderr is not None:
        stderr = stderr.decode("utf-8")
    return_code = process.returncode

    info = {
        "total_time": total_time,
        "command": command,
        "stdout": stdout,
        "stderr": stderr,
        "return_code": return_code,
    }

    # ensure all test conditions are satisfied
    for condition in conditions:
        condition.ingest_info(info)
        passed = condition.passes
        if passed is False:
            break

    return passed, info


def clear_test_studies_dir():
    """
    Deletes the 'test_studies' directory, in order to preserve
    state each time cli tests are run.
    """
    with suppress(FileNotFoundError):
        shutil.rmtree(f"./{OUTPUT_DIR}")


def process_test_result(passed, info, is_verbose, exit):
    """
    Process and print test results to the console.
    """
    # if the environment does not contain necessary programs, exit early.
    if passed is False and "merlin: command not found" in info["stderr"]:
        print(f"\nMissing from environment:\n\t{info['stderr']}")
        return None
    elif passed is False:
        print("FAIL")
        if exit is True:
            return None
    else:
        print("pass")

    if is_verbose is True:
        print(f"\tcommand: {info['command']}")
        print(f"\telapsed time: {round(info['total_time'], 2)} s")
        if info["return_code"] != 0:
            print(f"\treturn code: {info['return_code']}")
        if info["stderr"] != "":
            print(f"\tstderr:\n{info['stderr']}")

    return passed


def run_tests(args, tests):
    """
    Run all inputted tests.
    :param `tests`: a dictionary of
        {"test_name" : ("test_command", [conditions])}
    """
    selective = False
    n_to_run = len(tests)
    if args.ids is not None and len(args.ids) > 0:
        if not all(x > 0 for x in args.ids):
            raise ValueError(f"Test ids must be between 1 and {len(tests)}, inclusive.")
        selective = True
        n_to_run = len(args.ids)
    elif args.local is not None:
        args.ids = []
        n_to_run = 0
        selective = True
        test_id = 1
        for _, test in tests.items():
            if len(test) == 3 and test[2] == "local":
                args.ids.append(test_id)
                n_to_run += 1
            test_id += 1

    print(f"Running {n_to_run} integration tests...")
    start_time = time.time()

    total = 0
    failures = 0
    for test_name, test in tests.items():
        test_label = total + 1
        if selective and test_label not in args.ids:
            total += 1
            continue
        try:
            passed, info = run_single_test(test_name, test, test_label)
        except BaseException as e:
            print(e)
            passed = False
            info = None

        result = process_test_result(passed, info, args.verbose, args.exit)
        if result is None:
            print("Exiting early")
            return 1
        if result is False:
            failures += 1
        total += 1

    end_time = time.time()
    total_time = end_time - start_time

    if failures == 0:
        print(f"Done. {n_to_run} tests passed in {round(total_time, 2)} s.")
        return 0
    print(
        f"Done. {failures} tests out of {n_to_run} failed after {round(total_time, 2)} s.\n"
    )
    return 1


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


def define_tests():
    """
    Returns a dictionary of tests, where the key
    is the test's name, and the value is a tuple
    of (shell command, condition(s) to satisfy).
    """
    celery_regex = r"(srun\s+.*)?celery\s+worker\s+(-A|--app)\s+merlin\s+.*"

    # shortcut string variables
    workers = "merlin run-workers"
    run = "merlin run"
    restart = "merlin restart"
    purge = "merlin purge"
    examples = "merlin/examples/workflows"
    demo = f"{examples}/feature_demo/feature_demo.yaml"
    demo_pgen = f"{examples}/feature_demo/scripts/pgen.py"
    simple = f"{examples}/simple_chain/simple_chain.yaml"
    slurm = f"{examples}/slurm/slurm_test.yaml"
    slurm_restart = f"{examples}/slurm/slurm_par_restart.yaml"
    flux = f"{examples}/flux/flux_test.yaml"
    flux_restart = f"{examples}/flux/flux_par_restart.yaml"
    lsf = f"{examples}/lsf/lsf_par.yaml"
    black = "black --check --target-version py36"
    config_dir = "./CLI_TEST_MERLIN_CONFIG"

    return {
        "merlin": ("merlin", ReturnCodeCond(1), "local"),
        "merlin help": ("merlin --help", ReturnCodeCond(), "local"),
        "merlin version": ("merlin --version", ReturnCodeCond(), "local"),
        "merlin config": (
            f"merlin config -o {config_dir}; rm -rf {config_dir}",
            ReturnCodeCond(),
            "local",
        ),
        "run-workers echo simple_chain": (
            f"{workers} {simple} --echo",
            [ReturnCodeCond(), RegexCond(celery_regex)],
            "local",
        ),
        "run-workers echo feature_demo": (
            f"{workers} {demo} --echo",
            [ReturnCodeCond(), RegexCond(celery_regex)],
            "local",
        ),
        "run-workers echo slurm_test": (
            f"{workers} {slurm} --echo",
            [ReturnCodeCond(), RegexCond(celery_regex)],
            "local",
        ),
        "run-workers echo flux_test": (
            f"{workers} {flux} --echo",
            [ReturnCodeCond(), RegexCond(celery_regex)],
            "local",
        ),
        "run-workers echo override feature_demo": (
            f"{workers} {demo} --echo --vars VERIFY_QUEUE=custom_verify_queue",
            [ReturnCodeCond(), RegexCond("custom_verify_queue")],
            "local",
        ),
        "run feature_demo": (f"{run} {demo}", ReturnCodeCond()),
        "purge feature_demo": (f"{purge} {demo} -f", ReturnCodeCond()),
        "dry feature_demo": (
            f"{run} {demo} --local --dry --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            [
                StepFileExistsCond(
                    "verify", "verify_*.sh", "feature_demo", OUTPUT_DIR, params=True,
                ),
                ReturnCodeCond(),
            ],
            "local",
        ),
        "restart local simple_chain": (
            f"{run} {simple} --local --vars OUTPUT_PATH=./{OUTPUT_DIR} ; {restart} $(find ./{OUTPUT_DIR} -type d -name 'simple_chain_*') --local",
            ReturnCodeCond(),
            "local",
        ),
        "local simple_chain": (
            f"{run} {simple} --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            ReturnCodeCond(),
            "local",
        ),
        "example failure": (f"merlin example failure", RegexCond("not found"), "local"),
        "example simple_chain": (
            f"merlin example simple_chain ; {run} simple_chain.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR} ; rm simple_chain.yaml",
            ReturnCodeCond(),
            "local",
        ),
        "dry launch slurm": (
            f"{run} {slurm} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileContainsCond(
                "runs", "*/runs.slurm.sh", "slurm_test", OUTPUT_DIR, "srun "
            ),
            "local",
        ),
        "dry launch flux": (
            f"{run} {flux} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileContainsCond(
                "runs",
                "*/runs.slurm.sh",
                "flux_test",
                OUTPUT_DIR,
                get_flux_cmd("flux", no_errors=True),
            ),
            "local",
        ),
        "dry launch lsf": (
            f"{run} {lsf} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileContainsCond(
                "runs", "*/runs.slurm.sh", "lsf_par", OUTPUT_DIR, "jsrun "
            ),
            "local",
        ),
        "dry launch slurm restart": (
            f"{run} {slurm_restart} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileContainsCond(
                "runs",
                "*/runs.restart.slurm.sh",
                "slurm_par_restart",
                OUTPUT_DIR,
                "srun ",
            ),
            "local",
        ),
        "dry launch flux restart": (
            f"{run} {flux_restart} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileContainsCond(
                "runs_rs",
                "*/runs_rs.restart.slurm.sh",
                "flux_par_restart",
                OUTPUT_DIR,
                get_flux_cmd("flux", no_errors=True),
            ),
            "local",
        ),
        "local override feature_demo": (
            f"{run} {demo} --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR} --local",
            [
                ReturnCodeCond(),
                ProvenanceCond(
                    regex="PREDICT: \$\(SCRIPTS\)/predict.py",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="orig",
                ),
                ProvenanceCond(
                    regex="name: \$\(NAME\)",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="partial",
                ),
                ProvenanceCond(
                    regex="studies/feature_demo_",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="partial",
                ),
                ProvenanceCond(
                    regex="name: feature_demo",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceCond(
                    regex="\$\(NAME\)",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                    negate=True,
                ),
                StepFileExistsCond(
                    "verify",
                    "MERLIN_FINISHED",
                    "feature_demo",
                    OUTPUT_DIR,
                    params=True,
                ),
            ],
            "local",
        ),
        # "local restart expand name": (
        #    f"{run} {demo} --local --vars OUTPUT_PATH=./{OUTPUT_DIR} NAME=test_demo ; {restart} $(find ./{OUTPUT_DIR} -type d -name 'test_demo_*') --local",
        #    [
        #        ReturnCodeCond(),
        #        ProvenanceCond(
        #            regex="name: test_demo",
        #            name="test_demo",
        #            output_path=OUTPUT_DIR,
        #            provenance_type="expanded",
        #        ),
        #        StepFileExistsCond(
        #            "merlin_info", "test_demo.expanded.yaml", "test_demo", OUTPUT_DIR, params=True,
        #        ),
        #    ],
        #    "local",
        # ),
        "local csv feature_demo": (
            f"echo 42.0,47.0 > foo_testing_temp.csv; {run} {demo} --samples foo_testing_temp.csv --vars OUTPUT_PATH=./{OUTPUT_DIR} --local; rm -f foo_testing_temp.csv",
            [RegexCond("1 sample loaded."), ReturnCodeCond()],
            "local",
        ),
        "local tab feature_demo": (
            f"echo '42.0\t47.0\n7.0 5.3' > foo_testing_temp.tab; {run} {demo} --samples foo_testing_temp.tab --vars OUTPUT_PATH=./{OUTPUT_DIR} --local; rm -f foo_testing_temp.tab",
            [RegexCond("2 samples loaded."), ReturnCodeCond()],
            "local",
        ),
        "local pgen feature_demo": (
            f"{run} {demo} --pgen {demo_pgen} --vars OUTPUT_PATH=./{OUTPUT_DIR} --local",
            [
                ProvenanceCond(
                    regex="\[0.3333333",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceCond(
                    regex="\[0.5",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                    negate=True,
                ),
                ReturnCodeCond(),
            ],
            "local",
        ),
        # "local provenance spec equality": (
        #     f"{run} {simple} --vars OUTPUT_PATH=./{OUTPUT_DIR} --local ; cp $(find ./{OUTPUT_DIR}/simple_chain_*/merlin_info -type f -name 'simple_chain.expanded.yaml') ./{OUTPUT_DIR}/FILE1 ; rm -rf ./{OUTPUT_DIR}/simple_chain_* ; {run} ./{OUTPUT_DIR}/FILE1 --vars OUTPUT_PATH=./{OUTPUT_DIR} --local ; cmp ./{OUTPUT_DIR}/FILE1 $(find ./{OUTPUT_DIR}/simple_chain_*/merlin_info -type f -name 'simple_chain.expanded.yaml')",
        #     ReturnCodeCond(),
        #     "local",
        # ),
        "distributed feature_demo": (
            f"{run} {demo} --vars OUTPUT_PATH=./{OUTPUT_DIR} WORKER_NAME=cli_test_demo_workers ; {workers} {demo} --vars OUTPUT_PATH=./{OUTPUT_DIR} WORKER_NAME=cli_test_demo_workers",
            [
                ReturnCodeCond(),
                ProvenanceCond(
                    regex="cli_test_demo_workers:",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                StepFileExistsCond(
                    "verify",
                    "MERLIN_FINISHED",
                    "feature_demo",
                    OUTPUT_DIR,
                    params=True,
                ),
            ],
        ),
        # "black check merlin": (f"{black} merlin/", ReturnCodeCond(), "local"),
        # "black check tests": (f"{black} tests/", ReturnCodeCond(), "local"),
        "deplic no GNU": (
            f"deplic ./",
            [RegexCond("GNU", negate=True), RegexCond("GPL", negate=True)],
            "local",
        ),
    }


def setup_argparse():
    parser = argparse.ArgumentParser(description="run_tests cli parser")
    parser.add_argument(
        "--exit",
        action="store_true",
        help="Flag for stopping all testing upon first failure",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Flag for more detailed output messages"
    )
    parser.add_argument(
        "--local", action="store_true", default=None, help="Run only local tests"
    )
    parser.add_argument(
        "--ids",
        action="store",
        dest="ids",
        type=int,
        nargs="+",
        default=None,
        help="Provide space-delimited ids of tests you want to run."
        "Example: '--ids 1 5 8 13'",
    )
    return parser


def main():
    """
    High-level CLI test operations.
    """
    parser = setup_argparse()
    args = parser.parse_args()

    tests = define_tests()

    clear_test_studies_dir()
    result = run_tests(args, tests)
    clear_test_studies_dir()
    return result


if __name__ == "__main__":
    sys.exit(main())
