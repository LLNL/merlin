##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `encrypt.py` and `encrypt_backend_traffic.py` files.
"""

import os

import celery
import pytest

from merlin.common.security.encrypt import _gen_key, _get_key, _get_key_path, decrypt, encrypt
from merlin.common.security.encrypt_backend_traffic import _decrypt_decode, _encrypt_encode, set_backend_funcs
from merlin.config.configfile import CONFIG


class TestEncryption:
    """
    This class will house all tests necessary for our encryption modules.
    """

    def test_encrypt(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test that our encryption function is encrypting the bytes that we're
        passing to it.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        str_to_encrypt = b"super secret string shhh"
        encrypted_str = encrypt(str_to_encrypt)
        for word in str_to_encrypt.decode("utf-8").split(" "):
            assert word not in encrypted_str.decode("utf-8")

    def test_decrypt(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test that our decryption function is decrypting the bytes that we're passing to it.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # This is the output of the bytes from the encrypt test
        str_to_decrypt = b"gAAAAABld6k-jEncgCW5AePgrwn-C30dhr7dzGVhqzcqskPqFyA2Hdg3VWmo0qQnLklccaUYzAGlB4PMxyp4T-1gAYlAOf_7sC_bJOEcYOIkhZFoH6cX4Uw="
        decrypted_str = decrypt(str_to_decrypt)
        assert decrypted_str == b"super secret string shhh"

    def test_get_key_path(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `_get_key_path` function.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # Test the default behavior (`_get_key_path` will pull from CONFIG.results_backend which
        # will be set to the temporary output path for our tests in the `use_fake_encrypt_data_key` fixture)
        actual_default = _get_key_path()
        assert actual_default.startswith("/tmp/") and actual_default.endswith("/encrypt_data_key")

        # Test with having the encryption key set to None
        temp = CONFIG.results_backend.encryption_key
        CONFIG.results_backend.encryption_key = None
        with pytest.raises(ValueError) as excinfo:
            _get_key_path()
        assert "Error! No password provided for RabbitMQ" in str(excinfo.value)
        CONFIG.results_backend.encryption_key = temp

        # Test with having the entire results_backend wiped from CONFIG
        orig_results_backend = CONFIG.results_backend
        CONFIG.results_backend = None
        actual_no_results_backend = _get_key_path()
        assert actual_no_results_backend == os.path.abspath(os.path.expanduser("~/.merlin/encrypt_data_key"))
        CONFIG.results_backend = orig_results_backend

    def test_gen_key(self, temp_output_dir: str):
        """
        Test the `_gen_key` function.

        :param temp_output_dir: The path to the temporary output directory for this test run
        """
        # Create the file but don't put anything in it
        key_gen_test_file = f"{temp_output_dir}/key_gen_test"
        with open(key_gen_test_file, "w"):
            pass

        # Ensure nothing is in the file
        with open(key_gen_test_file, "r") as key_gen_file:
            key_gen_contents = key_gen_file.read()
        assert key_gen_contents == ""

        # Run the test and then check to make sure the file is now populated
        _gen_key(key_gen_test_file)
        with open(key_gen_test_file, "r") as key_gen_file:
            key_gen_contents = key_gen_file.read()
        assert key_gen_contents != ""

    def test_get_key(
        self,
        merlin_server_dir: str,
        test_encryption_key: bytes,
        redis_results_backend_config_function: "fixture",  # noqa: F821
    ):
        """
        Test the `_get_key` function.

        :param merlin_server_dir: The directory to the merlin test server configuration
        :param test_encryption_key: A fixture to establish a fixed encryption key for testing
        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # Test the default functionality
        actual_default = _get_key()
        assert actual_default == test_encryption_key

        # Modify the permission of the key file so that it can't be read by anyone
        # (we're purposefully trying to raise an IOError)
        key_path = f"{merlin_server_dir}/encrypt_data_key"
        orig_file_permissions = os.stat(key_path).st_mode
        os.chmod(key_path, 0o222)
        with pytest.raises(IOError):
            _get_key()
        os.chmod(key_path, orig_file_permissions)

        # Reset the key value to our test value since the IOError test will rewrite the key
        with open(key_path, "w") as key_file:
            key_file.write(test_encryption_key.decode("utf-8"))

    def test_set_backend_funcs(self):
        """
        Test the `set_backend_funcs` function.
        """
        orig_encode = celery.backends.base.Backend.encode
        orig_decode = celery.backends.base.Backend.decode

        # Make sure these values haven't been set yet
        assert celery.backends.base.Backend.encode != _encrypt_encode
        assert celery.backends.base.Backend.decode != _decrypt_decode

        set_backend_funcs()

        # Ensure the new functions have been set
        assert celery.backends.base.Backend.encode == _encrypt_encode
        assert celery.backends.base.Backend.decode == _decrypt_decode

        celery.backends.base.Backend.encode = orig_encode
        celery.backends.base.Backend.decode = orig_decode
