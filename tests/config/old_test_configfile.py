"""Tests for the configfile module."""
import os
import shutil
import tempfile
import unittest
from getpass import getuser

from merlin.config import configfile

from .utils import mkfile


CONFIG_FILE_CONTENTS = """
celery:
    certs: path/to/celery/config/files

broker:
    name: rabbitmq
    username: testuser
    password: rabbit.password  # The filename that contains the password.
    server: jackalope.llnl.gov

results_backend:
    name: mysql
    dbname: testuser
    username: mlsi
    password: mysql.password  # The filename that contains the password.
    server: rabbit.llnl.gov

"""


class TestFindConfigFile(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.appfile = mkfile(self.tmpdir, "app.yaml")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_tempdir(self):
        self.assertTrue(os.path.isdir(self.tmpdir))

    def test_find_config_file(self):
        """
        Given the path to a vaild config file, find and return the full
        filepath.
        """
        path = configfile.find_config_file(path=self.tmpdir)
        expected = os.path.join(self.tmpdir, self.appfile)
        self.assertEqual(path, expected)

    def test_find_config_file_error(self):
        """Given an invalid path, return None."""
        invalid = "invalid/path"
        expected = None

        path = configfile.find_config_file(path=invalid)
        self.assertEqual(path, expected)


class TestConfigFile(unittest.TestCase):
    """Unit tests for loading the config file."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.configfile = mkfile(self.tmpdir, "app.yaml", content=CONFIG_FILE_CONTENTS)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_get_config(self):
        """
        Given the directory path to a valid merlin config file, then
        `get_config` should find the merlin config file and load the YAML
        contents to a dictionary.
        """
        expected = {
            "broker": {
                "name": "rabbitmq",
                "password": "rabbit.password",
                "server": "jackalope.llnl.gov",
                "username": "testuser",
                "vhost": getuser(),
            },
            "celery": {"certs": "path/to/celery/config/files"},
            "results_backend": {
                "dbname": "testuser",
                "name": "mysql",
                "password": "mysql.password",
                "server": "rabbit.llnl.gov",
                "username": "mlsi",
            },
        }

        self.assertDictEqual(configfile.get_config(self.tmpdir), expected)
