###############################################################################
# Copyright (c) 2022, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.0
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
"""This file is meant to help dump information to files"""

import csv
import json
import logging
from typing import Dict, List


LOG = logging.getLogger(__name__)


class Dumper:
    """
    The dumper class is intended to help write information to files.
    Currently, the supported file types to dump to are csv and json.

    Example csv usage:
    dumper = Dumper("populations.csv")
    # Eugene, OR has a population of 175096
    # Livermore, CA has a population of 86803
    population_data = {
        "City": ["Eugene", "Livermore"],
        "State": ["OR", "CA"],
        "Population": [175096, 86803]
    }
    dumper.write(population_data, "w")
        |---> Output will be written to populations.csv

    Example json usage:
    dumper = Dumper("populations.json")
    population_data = {
        "OR": {"Eugene": 175096, "Portland": 641162},
        "CA": {"Livermore": 86803, "San Francisco": 815201}
    }
    dumper.write(population_data, "w")
        |---> Output will be written to populations.json
    """

    def __init__(self, file_name):
        """
        Initialize the class and ensure the file is of a supported type.
        :param `file_name`: The name of the file to dump to eventually
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
        Write information to an outfile.
        :param `info_to_write`: The information you want to write to the output file
        :param `fmode`: The file write mode ("w", "a", etc.)
        """
        if self.file_type == "csv":
            self._csv_write(info_to_write, fmode)
        elif self.file_type == "json":
            self._json_write(info_to_write, fmode)

    def _csv_write(self, csv_to_dump: Dict[str, List], fmode: str):
        """
        Write information to a csv file.
        :param `csv_to_dump`: The information to write to the csv file. Dict keys will be the column headers and values will be the column values.
        :param `fmode`: The file write mode ("w", "a", etc.)
        """
        # If we have statuses to write, create a csv writer object and write to the csv file
        with open(self.file_name, fmode) as outfile:
            csv_writer = csv.writer(outfile)
            if fmode == "w":
                csv_writer.writerow(csv_to_dump.keys())
            csv_writer.writerows(zip(*csv_to_dump.values()))

    def _json_write(self, json_to_dump: Dict[str, Dict], fmode: str):
        """
        Write information to a json file.
        :param `json_to_dump`: The information to write to the json file.
        :param `fmode`: The file write mode ("w", "a", etc.)
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
