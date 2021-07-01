from typing import List, Optional, Union

import pytest

from merlin.utils import convert_timestring


def _list_all_equal(my_list: List[str]) -> bool:
    """Boolean check that all items in the list are the same"""
    return all(item == my_list[0] for item in my_list)


def check_all_equal(time_strings: List[Union[str, int]], method: Optional[str] = "HMS") -> bool:
    """Check that time strings in a List convert to the same for the given conversion method."""
    converted: List[str] = [convert_timestring(ts) for ts in time_strings]
    all_equal: bool = _list_all_equal(converted)
    return all_equal


@pytest.mark.parametrize("time_string, expected_result, method", [
    ("01:00:00", "01:00:00", "HMS"),
    ("01:00:00", "3600.0s", "FSD"),
    ("1:00", "00:01:00", "HMS"),
    ("1:00", "60.0s", "FSD"),
    ("1", "00:00:01", "HMS"),
    ("1", "1.0s", "FSD")]
)
def test_convert_explicit(time_string: str, expected_result: str, method: str) -> None:
    """Test some time strings to make sure they are converted correctly."""
    converted: str = convert_timestring(time_string, method)
    err_msg: str = f"Failed on time_string '{time_string}' for {method}, result was '{converted}', not '{expected_result}'"
    assert converted == expected_result, err_msg


@pytest.mark.parametrize("test_case, expected_bool", [
    (["01:00:00", "1:0:0", "0:60:0", "60:0", "3600", 3600],True),
    (["1:0", "0:60", "0:0:60", "60", 60],True),
    (["1:1:1", "61:1", "3661", 3661],True),
    (["0:0:0", "0", "00:00:00", "0:00", "00:0:0", "00:00:0", 0],True),
    (["1:00:00:00", "24:0:0", "86400", 86400],True),
    (["60:0", "60:1"],False),
    (["1:0:0:0", "25:00:00"],False),
    ([0, 1],False),
    (["0", 1],False)]
)
@pytest.mark.parametrize("method", ["HMS", "FSD", None])
def test_convert_timestring_same(test_case: List[Union[str, int]], expected_bool: bool, method: Optional[str]) -> None:
    """Test that HMS formatted all the same."""
    err_msg: str = f"Failed on test case '{test_case}', expected {expected_bool}, not '{not expected_bool}'"
    assert check_all_equal(test_case, method=method) == expected_bool, err_msg
