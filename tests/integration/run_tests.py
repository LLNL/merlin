##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Script for running Merlin command line interface tests.
Built for 1) manual use and 2) continuous integration.
"""
import argparse
import shutil
import sys
import time
from contextlib import suppress
from subprocess import TimeoutExpired, run

from definitions import OUTPUT_DIR, define_tests  # pylint: disable=E0401
from tabulate import tabulate


def get_definition_issues(test):
    """
    Function to make sure the test definition was written properly.
    :param `test`: The test definition we're checking
    :returns: A list of errors found with the test definition
    """
    errors = []
    # Check that commands were provided
    try:
        commands = test["cmds"]
        if not isinstance(commands, list):
            commands = [commands]
    except KeyError:
        errors.append("'cmds' flag not defined")
        commands = None

    # Check that conditions were provided
    if "conditions" not in test:
        errors.append("'conditions' flag not defined")

    # Check that correct number of cmds were given depending on
    # the number of processes we'll need to start
    if commands:
        if "num procs" not in test:
            num_procs = 1
        else:
            num_procs = test["num procs"]

        if num_procs == 1 and len(commands) != 1:
            errors.append(f"Need 1 'cmds' since 'num procs' is 1 but {len(commands)} 'cmds' were given")
        elif num_procs == 2 and len(commands) != 2:
            errors.append(f"Need 2 'cmds' since 'num procs' is 2 but {len(commands)} 'cmds' were given")

    return errors


def run_single_test(test):
    """
    Runs a single test and returns whether it passed or not
    and information about the test for logging purposes.
    :param `test`: A dictionary that defines the test
    :returns: A tuple of type (bool, dict) where the bool
                represents if the test passed and the dict
                contains info about the test.
    """
    # Parse the test definition
    commands = test.pop("cmds", None)
    if not isinstance(commands, list):
        commands = [commands]
    conditions = test.pop("conditions", None)
    if not isinstance(conditions, list):
        conditions = [conditions]
    cleanup = test.pop("cleanup", None)
    num_procs = test.pop("num procs", 1)

    start_time = time.time()
    # As of now the only time we need 2 processes is to test stop-workers
    # Therefore we only care about the result of the second process
    if num_procs == 2:
        # First command should start the workers
        try:
            run(commands[0], timeout=8, capture_output=True, shell=True)
        except TimeoutExpired:
            pass
        # Second command should stop the workers
        result = run(commands[1], capture_output=True, text=True, shell=True)
    else:
        # Run the commands
        result = run(commands[0], capture_output=True, text=True, shell=True)
    end_time = time.time()
    total_time = end_time - start_time

    info = {
        "total_time": total_time,
        "command": commands,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "return_code": result.returncode,
        "violated_condition": None,
    }

    # ensure all test conditions are satisfied
    for i, condition in enumerate(conditions):
        condition.ingest_info(info)
        passed = condition.passes
        if passed is False:
            info["violated_condition"] = (condition, i, len(conditions))
            break

    if cleanup:
        end_process = run(cleanup, capture_output=True, text=True, shell=True)
        info["end_stdout"] = end_process.stdout
        info["end_stderr"] = end_process.stderr

    return passed, info


def clear_test_studies_dir():
    """
    Deletes the 'test_studies' directory, in order to preserve
    state each time cli tests are run.
    """
    with suppress(FileNotFoundError):
        shutil.rmtree(f"./{OUTPUT_DIR}")


def process_test_result(passed, info, is_verbose, exit_on_failure):
    """
    Process and print test results to the console.
    """
    # if the environment does not contain necessary programs, exit early.
    if passed is False and "merlin: command not found" in info["stderr"]:
        print(f"\nMissing from environment:\n\t{info['stderr']}")
        return None
    if passed is False:
        print("FAIL")
        if exit_on_failure is True:
            return None
    else:
        print("pass")

    if info["violated_condition"] is not None:
        msg: str = str(info["violated_condition"][0])
        condition_id: str = info["violated_condition"][1] + 1
        n_conditions: str = info["violated_condition"][2]
        print(f"\tCondition {condition_id} of {n_conditions}: {msg}")
    if is_verbose is True:
        print(f"\tcommand: {info['command']}")
        print(f"\telapsed time: {round(info['total_time'], 2)} s")
        if info["return_code"] != 0:
            print(f"\treturn code: {info['return_code']}")
        if info["stderr"] != "":
            print(f"\tstderr:\n{info['stderr']}")
        if info["stdout"] != "":
            print(f"\tstdout:\n{info['stdout']}")

    return passed


def filter_tests_to_run(args, tests):
    """
    Filter which tests to run based on args. The tests to
    run will be what makes up the args.ids list. This function
    will return whether we're being selective with what tests
    we run and also the number of tests that match the filter.
    :param `args`: CLI args given by user
    :param `tests`: a dict of all the tests that exist
    :returns: a tuple where the first entry is a bool on whether
              we filtered the tests at all and the second entry
              is an int representing the number of tests we're
              going to run.
    """
    selective = False
    n_to_run = len(tests)
    if args.ids is not None and len(args.ids) > 0:
        if not all(x > 0 for x in args.ids):
            raise ValueError(f"Test ids must be between 1 and {len(tests)}, inclusive.")
        selective = True
        n_to_run = len(args.ids)
    elif args.local is not None or args.distributed is not None:
        args.ids = []
        n_to_run = 0
        selective = True
        for test_id, test in enumerate(tests.values()):
            run_type = test.pop("run type", None)
            if (args.local and run_type == "local") or (args.distributed and run_type == "distributed"):
                args.ids.append(test_id + 1)
                n_to_run += 1

    return selective, n_to_run


# TODO split this function up so it's not as large (this will fix the pylint issue here too)
def run_tests(args, tests):  # pylint: disable=R0914
    """
    Run all inputted tests.
    :param `tests`: a dictionary of
        {"test_name" : ("test_command", [conditions])}
    """
    selective, n_to_run = filter_tests_to_run(args, tests)

    print(f"Running {n_to_run} integration tests...")
    start_time = time.time()

    total = 0
    failures = 0
    for test_name, test in tests.items():
        test_label = total + 1
        if selective and test_label not in args.ids:
            total += 1
            continue
        dot_length = 50 - len(test_name) - len(str(test_label))
        print(f"TEST {test_label}: {test_name}{'.' * dot_length}", end="")
        # Check the format of the test definition
        definition_issues = get_definition_issues(test)
        if definition_issues:
            print("FAIL")
            print(f"\tTest with name '{test_name}' has problems with its' test definition. Skipping...")
            if args.verbose:
                print(f"\tFound {len(definition_issues)} problems with the definition of '{test_name}':")
                for error in definition_issues:
                    print(f"\t- {error}")
            total += 1
            passed = False
            if args.exit:
                result = None
            else:
                result = False
        else:
            try:
                passed, info = run_single_test(test)
            except Exception as e:  # pylint: disable=C0103,W0718
                print(e)
                passed = False
                info = None
            result = process_test_result(passed, info, args.verbose, args.exit)

        clear_test_studies_dir()
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
    print(f"Done. {failures} tests out of {n_to_run} failed after {round(total_time, 2)} s.\n")
    return 1


def setup_argparse():
    """
    Using ArgumentParser, define the arguments allowed for this script.
    :returns: An ArgumentParser object
    """
    parser = argparse.ArgumentParser(description="run_tests cli parser")
    parser.add_argument(
        "--exit",
        action="store_true",
        help="Flag for stopping all testing upon first failure",
    )
    parser.add_argument("--verbose", action="store_true", help="Flag for more detailed output messages")
    parser.add_argument("--local", action="store_true", default=None, help="Run only local tests")
    parser.add_argument("--distributed", action="store_true", default=None, help="Run only distributed tests")
    parser.add_argument(
        "--ids",
        action="store",
        dest="ids",
        type=int,
        nargs="+",
        default=None,
        help="Provide space-delimited ids of tests you want to run. Example: '--ids 1 5 8 13'",
    )
    parser.add_argument(
        "--display-tests", action="store_true", default=False, help="Display a table format of test names and ids"
    )
    return parser


def display_tests(tests):
    """
    Helper function to display a table of tests and associated ids.
    Helps choose which test to run if you're trying to debug and use
    the --id flag.
    :param `tests`: A dict of tests (Dict)
    """
    test_names = list(tests.keys())
    test_table = [(i + 1, test_names[i]) for i in range(len(test_names))]
    test_table.insert(0, ("ID", "Test Name"))
    print()
    print(tabulate(test_table, headers="firstrow"))
    print()


def main():
    """
    High-level CLI test operations.
    """
    parser = setup_argparse()
    args = parser.parse_args()

    tests = define_tests()

    if args.display_tests:
        display_tests(tests)
        return

    clear_test_studies_dir()
    result = run_tests(args, tests)
    sys.exit(result)


if __name__ == "__main__":
    main()
