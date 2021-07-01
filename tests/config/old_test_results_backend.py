"""Tests for the results_backend module."""
import os
import shutil
import tempfile
import unittest

from merlin.config import results_backend

from .utils import mkfile


class TestResultsBackend(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

        # Create test files.
        self.tmpfile1 = mkfile(self.tmpdir, "mysql_test1.txt")
        self.tmpfile2 = mkfile(self.tmpdir, "mysql_test2.txt")

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_mysql_config(self):
        """
        Given the path to a directory containing the MySQL cert files and a
        dictionary of files to look for, then find and return the full path to
        all the certs.
        """
        certs = {"test1": "mysql_test1.txt", "test2": "mysql_test2.txt"}

        # This will just be the above dictionary with the full file paths.
        expected = {
            "test1": os.path.join(self.tmpdir, certs["test1"]),
            "test2": os.path.join(self.tmpdir, certs["test2"]),
        }
        results = results_backend.get_mysql_config(self.tmpdir, certs)
        self.assertDictEqual(results, expected)

    def test_mysql_config_no_files(self):
        """
        Given the path to a directory containing the MySQL cert files and
        an empty dictionary, then `get_mysql_config` should return an empty
        dictionary.
        """
        files = {}
        result = results_backend.get_mysql_config(self.tmpdir, files)
        self.assertEqual(result, {})


class TestConfingMysqlErrorPath(unittest.TestCase):
    """
    Test `get_mysql_config` against cases were the given path does not exist.
    """

    def test_mysql_config_false(self):
        """
        Given a path that does not exist, then `get_mysql_config` should return
        False.
        """
        path = "invalid/path"

        # We don't need the dictionary populated for this test. The function
        # should return False before trying to process the dictionary.
        certs = {}
        result = results_backend.get_mysql_config(path, certs)
        self.assertFalse(result)
