##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Test the functionality of the Config object.
"""

from copy import copy, deepcopy
from types import SimpleNamespace

from merlin.config import Config


class TestConfig:
    """
    Class for testing the Config object. We'll store a valid `app_dict`
    as an attribute here so that each test doesn't have to redefine it
    each time.
    """

    app_dict = {
        "celery": {"override": {"visibility_timeout": 86400}},
        "broker": {
            "cert_reqs": "none",
            "name": "rabbitmq",
            "password": "/path/to/pass_file",
            "port": 5671,
            "server": "127.0.0.1",
            "username": "default",
            "vhost": "host4testing",
        },
        "results_backend": {
            "cert_reqs": "none",
            "db_num": 0,
            "name": "rediss",
            "password": "/path/to/pass_file",
            "port": 6379,
            "server": "127.0.0.1",
            "username": "default",
            "vhost": "host4testing",
            "encryption_key": "/path/to/encryption_key",
        },
    }

    def test_config_creation(self):
        """
        Test the creation of the Config object. This should create nested namespaces
        for each key in the `app_dict` variable and save them to their respective
        attributes in the object.
        """
        config = Config(self.app_dict)

        # Create the nested namespace for celery and compare result
        override_namespace = SimpleNamespace(**self.app_dict["celery"]["override"])
        updated_celery_dict = deepcopy(self.app_dict)
        updated_celery_dict["celery"]["override"] = override_namespace
        celery_namespace = SimpleNamespace(**updated_celery_dict["celery"])
        assert config.celery == celery_namespace

        # Broker and Results Backend are easier since there's no nested namespace here
        assert config.broker == SimpleNamespace(**self.app_dict["broker"])
        assert config.results_backend == SimpleNamespace(**self.app_dict["results_backend"])

    def test_config_creation_no_celery(self):
        """
        Test the creation of the Config object without the celery key. This should still
        work and just not set anything for the celery attribute.
        """

        # Copy the celery section so we can restore it later and then delete it
        celery_section = copy(self.app_dict["celery"])
        del self.app_dict["celery"]
        config = Config(self.app_dict)

        # Broker and Results Backend are the only things loaded here
        assert config.broker == SimpleNamespace(**self.app_dict["broker"])
        assert config.results_backend == SimpleNamespace(**self.app_dict["results_backend"])

        # Ensure the celery attribute is not loaded
        assert "celery" not in dir(config)

        # Reset celery section in case other tests use it after this
        self.app_dict["celery"] = celery_section

    def test_config_copy(self):
        """
        Test the `__copy__` magic method of the Config object. Here we'll make sure
        each attribute was copied properly but the ids should be different.
        """
        orig_config = Config(self.app_dict)
        copied_config = copy(orig_config)

        assert orig_config.celery == copied_config.celery
        assert orig_config.broker == copied_config.broker
        assert orig_config.results_backend == copied_config.results_backend

        assert id(orig_config) != id(copied_config)

    def test_config_str(self):
        """
        Test the `__str__` magic method of the Config object. This should just give us
        a formatted string of the attributes in the object.
        """
        config = Config(self.app_dict)

        # Test normal printing
        actual = config.__str__()
        expected = (
            "config:\n"
            "  celery:\n"
            "    override: namespace(visibility_timeout=86400)\n"
            "  broker:\n"
            "    cert_reqs: 'none'\n"
            "    name: 'rabbitmq'\n"
            "    password: '/path/to/pass_file'\n"
            "    port: 5671\n"
            "    server: '127.0.0.1'\n"
            "    username: 'default'\n"
            "    vhost: 'host4testing'\n"
            "  results_backend:\n"
            "    cert_reqs: 'none'\n"
            "    db_num: 0\n"
            "    name: 'rediss'\n"
            "    password: '/path/to/pass_file'\n"
            "    port: 6379\n"
            "    server: '127.0.0.1'\n"
            "    username: 'default'\n"
            "    vhost: 'host4testing'\n"
            "    encryption_key: '/path/to/encryption_key'"
        )

        assert actual == expected

        # Test printing with one section set to None
        config.results_backend = None
        actual_with_none = config.__str__()
        expected_with_none = (
            "config:\n"
            "  celery:\n"
            "    override: namespace(visibility_timeout=86400)\n"
            "  broker:\n"
            "    cert_reqs: 'none'\n"
            "    name: 'rabbitmq'\n"
            "    password: '/path/to/pass_file'\n"
            "    port: 5671\n"
            "    server: '127.0.0.1'\n"
            "    username: 'default'\n"
            "    vhost: 'host4testing'\n"
            "  results_backend:\n"
            "    None"
        )

        assert actual_with_none == expected_with_none
