###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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

"""
Used to store the application configuration.
"""

from types import SimpleNamespace
from typing import Dict, List, Optional

from merlin.utils import nested_dict_to_namespaces


# Pylint complains that there's too few methods here but this class might
# be useful if we ever need to do extra stuff with the configuration so we'll
# ignore it for now
class Config:  # pylint: disable=R0903
    """
    The Config class, meant to store all Merlin config settings in one place.
    Regardless of the config data loading method, this class is meant to
    standardize config data retrieval throughout all parts of Merlin.
    """

    def __init__(self, app_dict):
        # I think this ends up a SimpleNamespace from load_app_into_namespaces, but it seems like it should be typed as
        # the app var in celery.py, as celery.app.base.Celery
        self.celery: Optional[SimpleNamespace]
        self.broker: Optional[SimpleNamespace]
        self.results_backend: Optional[SimpleNamespace]
        self.load_app_into_namespaces(app_dict)

    def load_app_into_namespaces(self, app_dict: Dict) -> None:
        """
        Makes the application dictionary into a namespace, sets the attributes of the Config from the namespace values.
        """
        fields: List[str] = ["celery", "broker", "results_backend"]
        for field in fields:
            try:
                setattr(self, field, nested_dict_to_namespaces(app_dict[field]))
            except KeyError:
                # The keywords are optional
                pass
