##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Used to store the application configuration.

The `config` package provides functionality for managing and configuring various aspects
of the Merlin application, including broker settings, results backends, Celery configurations,
and application-level settings. It serves as the central hub for loading, processing, and
utilizing configuration data defined in the `app.yaml` file and other related resources.

Modules:
    broker.py: Manages broker configurations and connection strings for messaging systems.
    celeryconfig.py: Contains default Celery configuration settings for Merlin.
    configfile.py: Handles the loading and processing of application configuration files
        and SSL-related settings.
    results_backend.py: Configures connection strings and SSL settings for results backends.
    utils.py: Provides utilities for broker priority handling and validation.
"""
from copy import copy
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

    Attributes:
        celery (Optional[SimpleNamespace]): A namespace containing Celery configuration settings.
        broker (Optional[SimpleNamespace]): A namespace containing broker configuration settings.
        results_backend (Optional[SimpleNamespace]): A namespace containing results backend configuration settings.

    Methods:
        __copy__: Creates a shallow copy of the Config instance.
        __str__: Returns a formatted string representation of the Config instance.
        load_app_into_namespaces: Converts the provided configuration dictionary into namespaces
            and assigns them to the Config instance's attributes.
    """

    def __init__(self, app_dict: Dict):
        """
        Initializes the Config instance with configuration data from a dictionary.

        Args:
            app_dict: A dictionary containing configuration data for the application.
                The dictionary may include keys such as "celery", "broker", and "results_backend",
                each of which is converted into a `SimpleNamespace` and assigned to the corresponding
                attribute of the Config instance.
        """
        # I think this ends up a SimpleNamespace from load_app_into_namespaces, but it seems like it should be typed as
        # the app var in celery.py, as celery.app.base.Celery
        self.celery: Optional[SimpleNamespace]
        self.broker: Optional[SimpleNamespace]
        self.results_backend: Optional[SimpleNamespace]
        self.load_app_into_namespaces(app_dict)

    def __copy__(self) -> "Config":
        """
        Creates a shallow copy of the Config instance.

        Returns:
            A new Config instance with copied `celery`, `broker`, and `results_backend` attributes.
        """
        cls = self.__class__
        result = cls.__new__(cls)
        copied_attrs = {
            "celery": copy(self.__dict__["celery"]),
            "broker": copy(self.__dict__["broker"]),
            "results_backend": copy(self.__dict__["results_backend"]),
        }
        result.__dict__.update(copied_attrs)
        return result

    def __str__(self) -> str:
        """
        Returns a formatted string representation of the Config instance.

        Returns:
            str: A string containing the values of the `celery`, `broker`, and `results_backend` attributes.
        """
        formatted_str = "config:"
        attrs = {"celery": self.celery, "broker": self.broker, "results_backend": self.results_backend}
        for name, attr in attrs.items():
            if attr is not None:
                items = (f"    {k}: {v!r}" for k, v in attr.__dict__.items())
                joined_items = "\n".join(items)
                formatted_str += f"\n  {name}:\n{joined_items}"
            else:
                formatted_str += f"\n  {name}:\n    None"
        return formatted_str

    def load_app_into_namespaces(self, app_dict: Dict):
        """
        Converts the provided application dictionary into namespaces and assigns them
        to the Config instance's attributes.

        Args:
            app_dict: A dictionary containing configuration data for the application.
        """
        fields: List[str] = ["celery", "broker", "results_backend"]
        for field in fields:
            try:
                setattr(self, field, nested_dict_to_namespaces(app_dict[field]))
            except KeyError:
                # The keywords are optional
                pass
