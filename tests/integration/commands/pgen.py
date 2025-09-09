##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This file contains pgen functionality for testing purposes.
It's specifically set up to work with the feature demo example.
"""

import random

from maestrowf.datastructures.core import ParameterGenerator


# pylint complains about unused argument `env` but it's necessary for Maestro
def get_custom_generator(env, **kwargs):  # pylint: disable=unused-argument
    """
    Custom parameter generator that's used for testing the `--pgen` flag
    of the `merlin run` command.
    """
    p_gen = ParameterGenerator()

    # Unpack any pargs passed in
    x2_min = int(kwargs.get("X2_MIN", "0"))
    x2_max = int(kwargs.get("X2_MAX", "1"))
    n_name_min = int(kwargs.get("N_NAME_MIN", "0"))
    n_name_max = int(kwargs.get("N_NAME_MAX", "10"))

    # We'll only have two parameter entries each just for testing
    num_points = 2

    params = {
        "X2": {"values": [random.uniform(x2_min, x2_max) for _ in range(num_points)], "label": "X2.%%"},
        "N_NEW": {"values": [random.randint(n_name_min, n_name_max) for _ in range(num_points)], "label": "N_NEW.%%"},
    }

    for key, value in params.items():
        p_gen.add_parameter(key, value["values"], value["label"])

    return p_gen
