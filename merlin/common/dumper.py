##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This file is meant to help dump information to files"""

import csv
import json
import logging
import os
from typing import Dict, List


LOG = logging.getLogger(__name__)


# TODO When we add more public methods we can get rid of this pylint disable
class Dumper:  # pylint: disable=R0903
    """
    The dumper class is intended to help write information to files.
    Currently, the supported dump file types are: csv or json.

    Attributes:
        file_name (str): The name of the file to write data to.
        file_type (str): The type of the file (either "csv" or "json") determined
            from the file name.

    Methods:
        write: Writes information to the specified output file based on the file type.
        _csv_write: Writes information to a CSV file.
        _json_write: Writes information to a JSON file.

    Example:
        CSV usage:
        ```python
        dumper = Dumper("populations.csv")
        # Eugene, OR has a population of 175096
        # Livermore, CA has a population of 86803
        population_data = {
            "City": ["Eugene", "Livermore"],
            "State": ["OR", "CA"],
            "Population": [175096, 86803]
        }
        dumper.write(population_data, "w")  # Output will be written to populations.csv
        ```

    Example:
        JSON usage:
        ```python
        dumper = Dumper("populations.json")
        population_data = {
            "OR": {"Eugene": 175096, "Portland": 641162},
            "CA": {"Livermore": 86803, "San Francisco": 815201}
        }
        dumper.write(population_data, "w")  # Output will be written to populations.json
        ```
    """

    def __init__(self, file_name: str):
        """
        Initializes the Dumper class and validates the file type.

        Args:
            file_name: The name of the file to write data to.

        Raises:
            ValueError: If the file type is not supported. Supported types are CSV and JSON.
        """
        supported_types = ["csv", "json"]

        valid_file = False
        for stype in supported_types:
            if file_name.endswith(stype):
                valid_file = True
                self.file_type = stype

        if not valid_file:
            raise ValueError(f"Invalid file type for {file_name}. Supported file types are: {supported_types}.")

        self.file_name = file_name

    def write(self, info_to_write: Dict, fmode: str):
        """
        Writes information to the specified output file.

        This method determines the file type and calls the appropriate
        method to write the data.

        Args:
            info_to_write: The information to write to the output file.
            fmode: The file write mode ("w" for write, "a" for append, etc.).
        """
        if self.file_type == "csv":
            self._csv_write(info_to_write, fmode)
        elif self.file_type == "json":
            self._json_write(info_to_write, fmode)

    def _csv_write(self, csv_to_dump: Dict[str, List], fmode: str):
        """
        Writes information to a CSV file.

        Args:
            csv_to_dump: The data to write to the CSV file. Keys are column
                headers and values are column values.
            fmode: The file write mode ("w" for write, "a" for append, etc.).
        """
        # If we have statuses to write, create a csv writer object and write to the csv file
        with open(self.file_name, fmode) as outfile:
            csv_writer = csv.writer(outfile)
            if fmode == "w":
                csv_writer.writerow(csv_to_dump.keys())
            csv_writer.writerows(zip(*csv_to_dump.values()))

    def _json_write(self, json_to_dump: Dict[str, Dict], fmode: str):
        """
        Writes information to a JSON file.

        Args:
            json_to_dump: The data to write to the JSON file.
            fmode: The file write mode ("w" for write, "a" for append, etc.).
        """
        # Appending to json requires file mode to be r+ for json.load
        if fmode == "a":
            fmode = "r+"

        with open(self.file_name, fmode) as outfile:
            # If we're appending, read in the existing file data
            if fmode == "r+":
                file_data = json.load(outfile)
                json_to_dump.update(file_data)
                outfile.seek(0)
            # Write to the outfile
            json.dump(json_to_dump, outfile)


def dump_handler(dump_file: str, dump_info: Dict):
    """
    Handles the process of creating a Dumper object and writing data
    to an output file.

    This function checks if the specified dump file exists to determine
    the appropriate file write mode (append or write). It then creates
    a Dumper object and writes the provided information to the file,
    logging the process.

    Args:
        dump_file: The filepath to the file where data will be dumped.
        dump_info: A dictionary containing the information to be written
            to the `dump_file`.
    """
    # Create a dumper object to help us write to dump_file
    dumper = Dumper(dump_file)

    # Get the correct file write mode and log message
    fmode = "a" if os.path.exists(dump_file) else "w"
    write_type = "Writing" if fmode == "w" else "Appending"
    LOG.info(f"{write_type} to {dump_file}...")

    # Write the output
    dumper.write(dump_info, fmode)
    LOG.info(f"{write_type} complete.")
