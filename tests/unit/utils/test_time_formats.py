from typing import List, Optional, Union

from merlin.utils import convert_timestring


def _list_all_equal(my_list: List) -> bool:
    """Boolean check that all items in the list are the same"""
    return all(item == my_list[0] for item in my_list)


def check_all_equal(
    time_strings: List[Union[str, int]], method: Optional[str] = "HMS"
) -> bool:
    """Check that time strings in a list convert to the same for the given converstion method."""
    converted = [convert_timestring(ts) for ts in time_strings]
    all_equal = _list_all_equal(converted)
    return all_equal


def check_convert_explicit(
    time_string: str, expected_result: str, method: str = "HMS"
) -> None:
    """Check an explicit conversion against the expected result."""
    converted = convert_timestring(time_string, method)
    assert converted == expected_result


def test_convert_explicit() -> None:
    """Test some cases to make sure they are converted correctly."""
    cases = [
        ("01:00:00", "01:00:00", "HMS"),
        ("01:00:00", "3600.0s", "FSD"),
        ("1:00", "00:01:00", "HMS"),
        ("1:00", "60.0s", "FSD"),
        ("1", "00:00:01", "HMS"),
        ("1", "1.0s", "FSD"),
    ]
    for time_string, expected_result, method in cases:
        check_convert_explicit(time_string, expected_result, method)


def test_convert_timestring_same() -> None:
    """Test that HMS formatted all the same"""
    equal_cases: List[List[Union[str, int]]]
    equal_cases = [
        ["01:00:00", "1:0:0", "0:60:0", "60:0", "3600", 3600],
        ["1:0", "0:60", "0:0:60", "60", 60],
        ["1:1:1", "61:1", "3661", 3661],
        ["0:0:0", "0", "00:00:00", "0:00", "00:0:0", "00:00:0", 0],
        ["1:00:00:00", "24:0:0", "86400", 86400],
    ]
    for method in ("HMS", "FSD", None):
        for test_case in equal_cases:
            assert check_all_equal(test_case, method=method) == True


def test_convert_timestring_different() -> None:
    """Test that these format differently!"""
    not_equal_cases: List[List[Union[str, int]]]
    not_equal_cases = [
        ["60:0", "60:1"],
        ["1:0:0:0", "25:00:00"],
        [0, 1],
        ["0", 1],
    ]
    for method in ("HMS", "FSD", None):
        for test_case in not_equal_cases:
            assert check_all_equal(test_case, method=method) == False
