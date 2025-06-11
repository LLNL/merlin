##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

import sys
from unittest.mock import patch

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


@pytest.fixture
def mock_get_distribution():
    """Mock call to get python distribution"""
    with patch("pkg_resources.get_distribution") as mock_get_distribution:
        mock_get_distribution.side_effect = [mock_distribution(*package) for package in fake_package_list[1:]]
        yield mock_get_distribution


class mock_distribution:
    """A mock python distribution"""

    def __init__(self, package, version, location):
        self.key = package
        self.version = version
        self.location = location


def test_get_package_versions(mock_get_distribution):
    """Test ability to get versions and format as correct table"""
    package_list = ["merlin", "celery", "kombu", "redis", "amqp"]
    fake_table = tabulate(fake_package_list, headers=["Package", "Version", "Location"], tablefmt="simple")
    expected_result = f"Python Packages\n\n{fake_table}\n"
    result = get_package_versions(package_list)
    assert result == expected_result


def test_bad_package():
    """Test that it only gets the things we have in our real environment."""
    bogus_packages = ["garbage_package_1", "junk_package_2"]
    result = get_package_versions(bogus_packages)
    expected_data = [fake_package_list[0]]  # python
    for package in bogus_packages:
        expected_data.append([package, "Not installed", "N/A"])

    expected_table = tabulate(expected_data, headers=["Package", "Version", "Location"], tablefmt="simple")
    expected_result = f"Python Packages\n\n{expected_table}\n"
    assert result == expected_result
