"""
Module to define functionality for managing encryption settings
while running our test suite.
"""
import os
from types import TracebackType
from typing import Type

from merlin.config.configfile import CONFIG


class EncryptionManager:
    """
    A class to handle safe setup and teardown of encryption tests.
    """

    def __init__(self, temp_output_dir: str, test_encryption_key: bytes):
        self.temp_output_dir = temp_output_dir
        self.key_path = os.path.abspath(os.path.expanduser(f"{self.temp_output_dir}/encrypt_data_key"))
        self.test_encryption_key = test_encryption_key
        self.orig_results_backend = CONFIG.results_backend

    def __enter__(self):
        """This magic method is necessary for allowing this class to be sued as a context manager"""
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, traceback: TracebackType):
        """
        This will always run at the end of a context with statement, even if an error is raised.
        It's a safe way to ensure all of our encryption settings at the start of the tests are reset.
        """
        self.reset_encryption_settings()

    def set_fake_key(self):
        """
        Create a fake encrypt data key to use for tests. This will save the fake encryption key to
        our temporary output directory located at:
        /tmp/`whoami`/pytest-of-`whoami`/pytest-current/integration_outfiles_current/encryption_tests/
        """
        with open(self.key_path, "w") as key_file:
            key_file.write(self.test_encryption_key.decode("utf-8"))

        CONFIG.results_backend.encryption_key = self.key_path

    def reset_encryption_settings(self):
        """
        Reset the encryption settings to what they were prior to running our encryption tests.
        """
        CONFIG.results_backend = self.orig_results_backend
