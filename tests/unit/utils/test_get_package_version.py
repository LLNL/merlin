##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from tabulate import tabulate

from merlin.utils import get_package_versions


fake_package_list = [
    ("python", sys.version.split()[0], sys.executable),
    ("merlin", "1.2.3", "/path/to/merlin"),
    ("celery", "4.5.1", "/path/to/celery"),
    ("kombu", "4.6.11", "/path/to/kombu"),
    ("redis", "3.5.3", "/path/to/redis"),
    ("amqp", "2.6.1", "/path/to/amqp"),
]


def make_mock_distribution(package, version, location):
    """
    Build a mock that mimics importlib.metadata.distribution().

    get_package_versions accesses:
      - dist.metadata["Version"]
      - dist.files[0].locate().parent.parent
    """
    dist = MagicMock()
    dist.metadata = {"Version": version}

    # Build a mock file whose .locate().parent.parent resolves to location
    mock_file = MagicMock()
    mock_file.locate.return_value = Path(location) / "pkg" / "file.py"
    dist.files = [mock_file]

    return dist


@pytest.fixture
def mock_distribution():
    """Mock importlib.metadata.distribution used inside get_package_versions."""
    with patch("merlin.utils.distribution") as mock_dist:
        mock_dist.side_effect = [
            make_mock_distribution(pkg, ver, loc)
            for _, pkg, ver, loc in [(None, *row) for row in [p for p in fake_package_list[1:]]]
        ]
        # Re-build side_effect cleanly
        mock_dist.side_effect = [make_mock_distribution(pkg, ver, loc) for pkg, ver, loc in fake_package_list[1:]]
        yield mock_dist


def test_get_package_versions(mock_distribution):
    """Test ability to get versions and format as correct table."""
    package_list = ["merlin", "celery", "kombu", "redis", "amqp"]
    fake_table = tabulate(fake_package_list, headers=["Package", "Version", "Location"], tablefmt="simple")
    expected_result = f"Python Packages\n\n{fake_table}\n"
    result = get_package_versions(package_list)
    assert result == expected_result


def test_bad_package():
    """Test that not-installed packages show 'Not installed'."""
    bogus_packages = ["garbage_package_1", "junk_package_2"]
    result = get_package_versions(bogus_packages)
    expected_data = [fake_package_list[0]]  # python row
    for package in bogus_packages:
        expected_data.append([package, "Not installed", "N/A"])
    expected_table = tabulate(expected_data, headers=["Package", "Version", "Location"], tablefmt="simple")
    expected_result = f"Python Packages\n\n{expected_table}\n"
    assert result == expected_result
