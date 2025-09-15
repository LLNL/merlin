##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Utility functions for our test suite.
"""

import os
from typing import Dict

from tests.constants import SERVER_PASS


def create_pass_file(pass_filepath: str):
    """
    Check if a password file already exists (it will if the redis server has been started)
    and if it hasn't then create one and write the password to the file.

    :param pass_filepath: The path to the password file that we need to check for/create
    """
    if not os.path.exists(pass_filepath):
        with open(pass_filepath, "w") as pass_file:
            pass_file.write(SERVER_PASS)


def create_cert_files(cert_filepath: str, cert_files: Dict[str, str]):
    """
    Check if cert files already exist and if they don't then create them.

    :param cert_filepath: The path to the cert files
    :param cert_files: A dict of certification files to create
    """
    for cert_file in cert_files.values():
        full_cert_filepath = f"{cert_filepath}/{cert_file}"
        if not os.path.exists(full_cert_filepath):
            with open(full_cert_filepath, "w"):
                pass


def create_dir(dirpath: str):
    """
    Check if `dirpath` exists and if it doesn't then create it.

    :param dirpath: The directory to create
    """
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
