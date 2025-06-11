##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `dict_deep_merge` function defined in the `utils.py` module.
"""

from typing import Any, Dict, List

import pytest

from merlin.utils import dict_deep_merge


def run_invalid_check(dict_a: Any, dict_b: Any, expected_log: str, caplog: "Fixture"):  # noqa: F821
    """
    Helper function to run invalid input tests on the `dict_deep_merge` function.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param expected_log: The log that we're expecting `dict_deep_merge` to write
    :param caplog: A built-in fixture from the pytest library to capture logs
    """

    # Store initial value of dict_a
    if isinstance(dict_a, list):
        dict_a_initial = dict_a.copy()
    else:
        dict_a_initial = dict_a

    # Check that dict_deep_merge returns None and that dict_a wasn't modified
    assert dict_deep_merge(dict_a, dict_b) is None
    assert dict_a_initial == dict_a

    # Check that dict_deep_merge logs a warning
    print(f"caplog.text: {caplog.text}")
    assert expected_log in caplog.text, "Missing expected log message"


@pytest.mark.parametrize(
    "dict_a, dict_b",
    [
        (None, None),
        (None, ["no lists allowed!"]),
        (["no lists allowed!"], None),
        (["no lists allowed!"], ["no lists allowed!"]),
        ("no strings allowed!", None),
        (None, "no strings allowed!"),
        ("no strings allowed!", "no strings allowed!"),
        (10, None),
        (None, 10),
        (10, 10),
        (10.5, None),
        (None, 10.5),
        (10.5, 10.5),
        (("no", "tuples"), None),
        (None, ("no", "tuples")),
        (("no", "tuples"), ("no", "tuples")),
        (True, None),
        (None, True),
        (True, True),
    ],
)
def test_dict_deep_merge_both_dicts_invalid(dict_a: Any, dict_b: Any, caplog: "Fixture"):  # noqa: F821
    """
    Test the `dict_deep_merge` function with both `dict_a` and `dict_b`
    parameters being an invalid type. This should log a message and do
    nothing.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param caplog: A built-in fixture from the pytest library to capture logs
    """

    # The expected log that's output by dict_deep_merge
    expected_log = f"Problem with dict_deep_merge: dict_a '{dict_a}' is not a dict, dict_b '{dict_b}' is not a dict. Ignoring this merge call."

    # Run the actual test
    run_invalid_check(dict_a, dict_b, expected_log, caplog)


@pytest.mark.parametrize(
    "dict_a, dict_b",
    [
        (None, {"test_key": "test_val"}),
        (["no lists allowed!"], {"test_key": "test_val"}),
        ("no strings allowed!", {"test_key": "test_val"}),
        (10, {"test_key": "test_val"}),
        (10.5, {"test_key": "test_val"}),
        (("no", "tuples"), {"test_key": "test_val"}),
        (True, {"test_key": "test_val"}),
    ],
)
def test_dict_deep_merge_dict_a_invalid(dict_a: Any, dict_b: Dict[str, str], caplog: "Fixture"):  # noqa: F821
    """
    Test the `dict_deep_merge` function with the `dict_a` parameter
    being an invalid type. This should log a message and do nothing.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param caplog: A built-in fixture from the pytest library to capture logs
    """

    # The expected log that's output by dict_deep_merge
    expected_log = f"Problem with dict_deep_merge: dict_a '{dict_a}' is not a dict. Ignoring this merge call."

    # Run the actual test
    run_invalid_check(dict_a, dict_b, expected_log, caplog)


@pytest.mark.parametrize(
    "dict_a, dict_b",
    [
        ({"test_key": "test_val"}, None),
        ({"test_key": "test_val"}, ["no lists allowed!"]),
        ({"test_key": "test_val"}, "no strings allowed!"),
        ({"test_key": "test_val"}, 10),
        ({"test_key": "test_val"}, 10.5),
        ({"test_key": "test_val"}, ("no", "tuples")),
        ({"test_key": "test_val"}, True),
    ],
)
def test_dict_deep_merge_dict_b_invalid(dict_a: Dict[str, str], dict_b: Any, caplog: "Fixture"):  # noqa: F821
    """
    Test the `dict_deep_merge` function with the `dict_b` parameter
    being an invalid type. This should log a message and do nothing.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param caplog: A built-in fixture from the pytest library to capture logs
    """

    # The expected log that's output by dict_deep_merge
    expected_log = f"Problem with dict_deep_merge: dict_b '{dict_b}' is not a dict. Ignoring this merge call."

    # Run the actual test
    run_invalid_check(dict_a, dict_b, expected_log, caplog)


@pytest.mark.parametrize(
    "dict_a, dict_b, expected",
    [
        ({"test_key": {}}, {"test_key": {}}, {}),  # Testing merge of two empty dicts
        ({"test_key": {}}, {"test_key": {"new_key": "new_val"}}, {"new_key": "new_val"}),  # Testing dict_a empty dict merge
        (
            {"test_key": {"existing_key": "existing_val"}},
            {"test_key": {}},
            {"existing_key": "existing_val"},
        ),  # Testing dict_b empty dict merge
        (
            {"test_key": {"existing_key": "existing_val"}},
            {"test_key": {"new_key": "new_val"}},
            {"existing_key": "existing_val", "new_key": "new_val"},
        ),  # Testing merge of dicts with content
    ],
)
def test_dict_deep_merge_dict_merge(
    dict_a: Dict[str, Dict[Any, Any]], dict_b: Dict[str, Dict[Any, Any]], expected: Dict[Any, Any]
):
    """
    Test the `dict_deep_merge` function with dicts that need to be merged.
    NOTE we're keeping the test values of this function simple since the other tests
         related to `dict_deep_merge` should be hitting the other possible scenarios.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param expected: The dict that we're expecting to now be in dict_a at 'test_key'
    """
    dict_deep_merge(dict_a, dict_b)
    assert dict_a["test_key"] == expected


@pytest.mark.parametrize(
    "dict_a, dict_b, expected",
    [
        ({"test_key": []}, {"test_key": []}, []),  # Testing merge of two empty lists
        ({"test_key": []}, {"test_key": ["new_val"]}, ["new_val"]),  # Testing dict_a empty list merge
        ({"test_key": ["existing_val"]}, {"test_key": []}, ["existing_val"]),  # Testing dict_b empty list merge
        (
            {"test_key": ["existing_val"]},
            {"test_key": ["new_val"]},
            ["existing_val", "new_val"],
        ),  # Testing merge of list of strings
        ({"test_key": [None]}, {"test_key": [None]}, [None, None]),  # Testing merge of list of None
        ({"test_key": [0]}, {"test_key": [1]}, [0, 1]),  # Testing merge of list of integers
        ({"test_key": [True]}, {"test_key": [False]}, [True, False]),  # Testing merge of list of bools
        ({"test_key": [0.0]}, {"test_key": [1.0]}, [0.0, 1.0]),  # Testing merge of list of floats
        (
            {"test_key": [(True, False)]},
            {"test_key": [(False, True)]},
            [(True, False), (False, True)],
        ),  # Testing merge of list of tuples
        (
            {"test_key": [{"existing_key": "existing_val"}]},
            {"test_key": [{"new_key": "new_val"}]},
            [{"existing_key": "existing_val"}, {"new_key": "new_val"}],
        ),  # Testing merge of list of dicts
        (
            {"test_key": ["existing_val", 0]},
            {"test_key": [True, 1.0, None]},
            ["existing_val", 0, True, 1.0, None],
        ),  # Testing merge of list of multiple types
    ],
)
def test_dict_deep_merge_list_merge(dict_a: Dict[str, List[Any]], dict_b: Dict[str, List[Any]], expected: List[Any]):
    """
    Test the `dict_deep_merge` function with lists that need to be merged.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param expected: The list that we're expecting to now be in dict_a at 'test_key'
    """
    dict_deep_merge(dict_a, dict_b)
    assert dict_a["test_key"] == expected


@pytest.mark.parametrize(
    "dict_a, dict_b, expected",
    [
        ({"test_key": None}, {"test_key": None}, None),  # Testing merge of None
        ({"test_key": "test_val"}, {"test_key": "test_val"}, "test_val"),  # Testing merge of string
        ({"test_key": 1}, {"test_key": 1}, 1),  # Testing merge of int
        ({"test_key": 1.0}, {"test_key": 1.0}, 1.0),  # Testing merge of float
        ({"test_key": False}, {"test_key": False}, False),  # Testing merge of bool
    ],
)
def test_dict_deep_merge_same_leaf(dict_a: Dict[str, Any], dict_b: Dict[str, Any], expected: Any):
    """
    Test the `dict_deep_merge` function with equivalent values in dict_a and dict_b.
    Nothing should happen here so dict_a["test_key"] should be the exact same.

    :param dict_a: The value of dict_a that we're testing against
    :param dict_b: The value of dict_b that we're testing against
    :param expected: The value that we're expecting to now be in dict_a at 'test_key'
    """
    dict_deep_merge(dict_a, dict_b)
    assert dict_a["test_key"] == expected


def test_dict_deep_merge_conflict_no_conflict_handler(caplog: "Fixture"):  # noqa: F821
    """
    Test the `dict_deep_merge` function with a conflicting value in dict_b
    and no conflict handler. Since there's no conflict handler this should
    log a warning and ignore any merge for the key that has the conflict.

    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    dict_a = {"test_key": "existing_value"}
    dict_b = {"test_key": "new_value"}

    # Call deep merge and make sure "test_key" in dict_a wasn't updated
    dict_deep_merge(dict_a, dict_b)
    assert dict_a["test_key"] == "existing_value"

    # Check that dict_deep_merge logs a warning
    assert "Conflict at test_key. Ignoring the update to key 'test_key'." in caplog.text, "Missing expected log message"


def test_dict_deep_merge_conflict_with_conflict_handler():
    """
    Test the `dict_deep_merge` function with a conflicting value in dict_b
    and a conflict handler. Our conflict handler will just concatenate the
    conflicting strings.
    """
    dict_a = {"test_key": "existing_value"}
    dict_b = {"test_key": "new_value"}

    def conflict_handler(*args, **kwargs):
        """
        The conflict handler that we'll be passing in to `dict_deep_merge`.
        This will concatenate the conflicting strings.
        """
        dict_a_val = kwargs.get("dict_a_val", None)
        dict_b_val = kwargs.get("dict_b_val", None)
        return ", ".join([dict_a_val, dict_b_val])

    # Call deep merge and make sure "test_key" in dict_a wasn't updated
    dict_deep_merge(dict_a, dict_b, conflict_handler=conflict_handler)
    assert dict_a["test_key"] == "existing_value, new_value"
