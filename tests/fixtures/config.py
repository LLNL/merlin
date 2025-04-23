"""
Fixtures for tests of the files in the `config/` directory.
"""

import os
import shutil

import pytest

from tests.fixture_types import FixtureCallable, FixtureStr


# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def config_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `config` directory.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for tests of files in the `config` directory.
    """
    return create_testing_dir(temp_output_dir, "config_testing")


@pytest.fixture
def config_create_app_yaml(config_testing_dir: FixtureStr) -> FixtureCallable:
    """
    A fixture to create an app.yaml file in the temporary output directory.

    Args:
        config_testing_dir: The path to the temporary testing directory for tests of files in the
            `config` directory.

    Returns:
        A function that creates an app.yaml file.
    """

    def _create_app_yaml(filename: str, broker: str = "rabbitmq"):
        """
        A helper method to create an app.yaml file that can be used for testing.

        Args:
            filename: The name of the app.yaml file.
        """
        app_yaml_filepath = os.path.join(config_testing_dir, filename)
        path_to_template_app_yamls = os.path.join(os.path.dirname(__file__), "..", "..", "merlin", "data", "celery")
        if broker == "rabbitmq":
            shutil.copy(os.path.join(path_to_template_app_yamls, "app.yaml"), app_yaml_filepath)
        elif broker == "redis":
            shutil.copy(os.path.join(path_to_template_app_yamls, "app_redis.yaml"), app_yaml_filepath)
        else:
            raise ValueError("Invalid broker given to `config_create_app_yaml` fixture.")

    return _create_app_yaml
