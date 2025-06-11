##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module stores constants representing file paths that will be needed for
Merlin's configuration.
"""

import os


APP_FILENAME: str = "app.yaml"
USER_HOME: str = os.path.expanduser("~")
MERLIN_HOME: str = os.path.join(USER_HOME, ".merlin")
CONFIG_PATH_FILE: str = os.path.join(MERLIN_HOME, "config_path.txt")
