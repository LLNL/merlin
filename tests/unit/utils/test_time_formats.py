import datetime
from typing import List, Optional, Union

import pytest

from merlin.utils import convert_timestring, repr_timedelta


@pytest.mark.parametrize(
    "time_string, expected_result, method",
    [
        ("01:00:00", "01:00:00", "HMS"),
        ("01:00:00", "3600.0s", "FSD"),
        ("1:00", "00:01:00", "HMS"),
        ("1:00", "60.0s", "FSD"),
        ("1", "00:00:01", "HMS"),
        ("1", "1.0s", "FSD"),
    ],
)
def test_convert_explicit(time_string: str, expected_result: str, method: str) -> None:
    """Test some cases to make sure they are converted correctly."""
    converted: str = convert_timestring(time_string, method)
    err_msg: str = f"Failed on time_string '{time_string}' for {method}, result was '{converted}', not '{expected_result}'"
    assert converted == expected_result, err_msg


@pytest.mark.parametrize(
    "test_case, expected_bool",
    [
        (["01:00:00", "1:0:0", "0:60:0", "60:0", "3600", 3600], True),
        (["1:0", "0:60", "0:0:60", "60", 60], True),
        (["1:1:1", "61:1", "3661", 3661], True),
        (["0:0:0", "0", "00:00:00", "0:00", "00:0:0", "00:00:0", 0], True),
        (["1:00:00:00", "24:0:0", "86400", 86400], True),
        (["60:0", "60:1"], False),
        (["1:0:0:0", "25:00:00"], False),
        ([0, 1], False),
        (["0", 1], False),
    ],
)
@pytest.mark.parametrize("method", ["HMS", "FSD", None])
def test_convert_timestring_same(
    test_case: List[Union[str, int]], expected_bool: bool, method: Optional[str]
) -> None:
    """Test that HMS formatted all the same"""
    err_msg: str = f"Failed on test case '{test_case}', expected {expected_bool}, not '{not expected_bool}'"
    converted_times: List[str] = [
        convert_timestring(time_strings) for time_strings in test_case
    ]
    all_equal: bool = all(
        time_string == converted_times[0] for time_string in converted_times
    )
    assert all_equal == expected_bool, err_msg


def test_invalid_time_format() -> None:
    """Test that if not provided an appropriate format (HMS, FSD), the appropriate error is thrown."""
    with pytest.raises(ValueError) as invalid_format:
        repr_timedelta(datetime.timedelta(1), "HMD")
    examination_err_msg: str = (
        "Did not raise correct ValueError for failed repr_timedelta()."
    )
    assert "Invalid method for formatting timedelta! Valid choices: HMS, FSD" in str(
        invalid_format.value
    ), examination_err_msg
