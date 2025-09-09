##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `dumper.py` file.
"""

import csv
import json
import os
from datetime import datetime
from time import sleep

import pytest

from merlin.common.dumper import dump_handler


NUM_ROWS = 5
CSV_INFO_TO_DUMP = {
    "row_num": [i for i in range(1, NUM_ROWS + 1)],
    "other_info": [f"test_info_{i}" for i in range(1, NUM_ROWS + 1)],
}
JSON_INFO_TO_DUMP = {str(i): {f"other_info_{i}": f"test_info_{i}"} for i in range(1, NUM_ROWS + 1)}
DUMP_HANDLER_DIR = "{temp_output_dir}/dump_handler"


def test_dump_handler_invalid_dump_file():
    """
    This is really testing the initialization of the Dumper class with an invalid file type.
    This should raise a ValueError.
    """
    with pytest.raises(ValueError) as excinfo:
        dump_handler("bad_file.txt", CSV_INFO_TO_DUMP)
    assert "Invalid file type for bad_file.txt. Supported file types are: ['csv', 'json']" in str(excinfo.value)


def get_output_file(temp_dir: str, file_name: str):
    """
    Helper function to get a full path to the temporary output file.

    :param temp_dir: The path to the temporary output directory that pytest gives us
    :param file_name: The name of the file
    """
    dump_dir = DUMP_HANDLER_DIR.format(temp_output_dir=temp_dir)
    if not os.path.exists(dump_dir):
        os.mkdir(dump_dir)
    dump_file = f"{dump_dir}/{file_name}"
    return dump_file


def run_csv_dump_test(dump_file: str, fmode: str):
    """
    Run the test for csv dump.

    :param dump_file: The file that the dump was written to
    :param fmode: The type of write that we're testing ("w" for write, "a" for append)
    """

    # Check that the file exists and that read in the contents of the file
    assert os.path.exists(dump_file)
    with open(dump_file, "r") as df:
        reader = csv.reader(df)
        written_data = list(reader)

    expected_rows = NUM_ROWS * 2 if fmode == "a" else NUM_ROWS
    assert len(written_data) == expected_rows + 1  # Adding one because of the header row
    for i, row in enumerate(written_data):
        assert len(row) == 2  # Check number of columns
        if i == 0:  # Checking the header row
            assert row[0] == "row_num"
            assert row[1] == "other_info"
        else:  # Checking the data rows
            assert row[0] == str(CSV_INFO_TO_DUMP["row_num"][(i % NUM_ROWS) - 1])
            assert row[1] == str(CSV_INFO_TO_DUMP["other_info"][(i % NUM_ROWS) - 1])


def test_dump_handler_csv_write(temp_output_dir: str):
    """
    This is really testing the write method of the Dumper class.
    This should create a csv file and write to it.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the path to the file we'll write to
    dump_file = get_output_file(temp_output_dir, "csv_write.csv")

    # Run the actual call to dump to the file
    dump_handler(dump_file, CSV_INFO_TO_DUMP)

    # Assert that everything ran properly
    run_csv_dump_test(dump_file, "w")


def test_dump_handler_csv_append(temp_output_dir: str):
    """
    This is really testing the write method of the Dumper class with the file write mode set to append.
    We'll write to a csv file first and then run again to make sure we can append to it properly.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the path to the file we'll write to
    dump_file = get_output_file(temp_output_dir, "csv_append.csv")

    # Run the first call to create the csv file
    dump_handler(dump_file, CSV_INFO_TO_DUMP)

    # Run the second call to append to the csv file
    dump_handler(dump_file, CSV_INFO_TO_DUMP)

    # Assert that everything ran properly
    run_csv_dump_test(dump_file, "a")


def test_dump_handler_json_write(temp_output_dir: str):
    """
    This is really testing the write method of the Dumper class.
    This should create a json file and write to it.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the path to the file we'll write to
    dump_file = get_output_file(temp_output_dir, "json_write.json")

    # Run the actual call to dump to the file
    dump_handler(dump_file, JSON_INFO_TO_DUMP)

    # Check that the file exists and that the contents are correct
    assert os.path.exists(dump_file)
    with open(dump_file, "r") as df:
        contents = json.load(df)
    assert contents == JSON_INFO_TO_DUMP


def test_dump_handler_json_append(temp_output_dir: str):
    """
    This is really testing the write method of the Dumper class with the file write mode set to append.
    We'll write to a json file first and then run again to make sure we can append to it properly.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the path to the file we'll write to
    dump_file = get_output_file(temp_output_dir, "json_append.json")

    # Run the first call to create the file
    timestamp_1 = str(datetime.now())
    first_dump = {timestamp_1: JSON_INFO_TO_DUMP}
    dump_handler(dump_file, first_dump)

    # Sleep so we don't accidentally get the same timestamp
    sleep(0.5)

    # Run the second call to append to the file
    timestamp_2 = str(datetime.now())
    second_dump = {timestamp_2: JSON_INFO_TO_DUMP}
    dump_handler(dump_file, second_dump)

    # Check that the file exists and that the contents are correct
    assert os.path.exists(dump_file)
    with open(dump_file, "r") as df:
        contents = json.load(df)
    keys = contents.keys()
    assert len(keys) == 2
    assert timestamp_1 in keys
    assert timestamp_2 in keys
    assert contents[timestamp_1] == JSON_INFO_TO_DUMP
    assert contents[timestamp_2] == JSON_INFO_TO_DUMP
