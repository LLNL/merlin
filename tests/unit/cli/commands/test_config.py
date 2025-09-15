##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `config.py` file of the `cli/` folder.
"""

import os
from argparse import ArgumentTypeError, Namespace, _SubParsersAction
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.config import ConfigCommand
from tests.fixture_types import FixtureCallable, FixtureStr


def test_add_parser_includes_all_subcommands(create_parser: FixtureCallable):
    """
    Verify that the `config` command parser includes all expected subcommands:
    `create`, `update-broker`, `update-backend`, and `use`.

    Args:
        create_parser: A fixture to help create a parser.
    """
    config_subparser = None
    parser = create_parser(ConfigCommand())
    for action in parser._subparsers._actions:
        if isinstance(action, _SubParsersAction):
            config_parser = action.choices.get("config")
            if config_parser:
                config_subparser = config_parser
                break

    assert config_subparser is not None, "Config subparser not found"

    help_text = config_subparser.format_help()

    assert "create" in help_text
    assert "update-broker" in help_text
    assert "update-backend" in help_text
    assert "use" in help_text


def test_process_command_create_invokes_methods(mocker: MockerFixture):
    """
    Ensure that running `config create` invokes the appropriate methods on MerlinConfigManager.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_config_manager_class = mocker.patch("merlin.cli.commands.config.MerlinConfigManager")
    args = Namespace(commands="create", task_server="celery", config_file="dummy.yaml", test=False)
    mock_config_manager = MagicMock()
    mock_config_manager_class.return_value = mock_config_manager

    cmd = ConfigCommand()
    cmd.process_command(args)

    mock_config_manager.create_template_config.assert_called_once()
    mock_config_manager.save_config_path.assert_called_once()


def test_process_command_update_broker(mocker: MockerFixture):
    """
    Ensure that running `config update-broker` invokes the `update_broker` method on `MerlinConfigManager`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_config_manager_class = mocker.patch("merlin.cli.commands.config.MerlinConfigManager")
    args = Namespace(commands="update-broker", config_file="dummy.yaml", type="redis")
    mock_config_manager = MagicMock()
    mock_config_manager_class.return_value = mock_config_manager

    with patch("builtins.open", create=True), patch("yaml.safe_load"):
        cmd = ConfigCommand()
        cmd.process_command(args)

    mock_config_manager.update_broker.assert_called_once()


def test_process_command_update_backend(mocker: MockerFixture):
    """
    Ensure that running `config update-backend` invokes the `update_backend` method on `MerlinConfigManager`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_config_manager_class = mocker.patch("merlin.cli.commands.config.MerlinConfigManager")
    args = Namespace(commands="update-backend", config_file="dummy.yaml", type="redis")
    mock_config_manager = MagicMock()
    mock_config_manager_class.return_value = mock_config_manager

    with patch("builtins.open", create=True), patch("yaml.safe_load"):
        cmd = ConfigCommand()
        cmd.process_command(args)

    mock_config_manager.update_backend.assert_called_once()


def test_process_command_use(mocker: MockerFixture):
    """
    Ensure that running `config use` sets the config file and calls `save_config_path` on `MerlinConfigManager`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_config_manager_class = mocker.patch("merlin.cli.commands.config.MerlinConfigManager")
    args = Namespace(commands="use", config_file="dummy.yaml")
    mock_config_manager = MagicMock()
    mock_config_manager_class.return_value = mock_config_manager

    with patch("builtins.open", create=True), patch("yaml.safe_load"):
        cmd = ConfigCommand()
        cmd.process_command(args)

    assert mock_config_manager.config_file == "dummy.yaml"
    mock_config_manager.save_config_path.assert_called_once()


def test_process_command_raises_on_missing_file():
    """
    Verify that an `ArgumentTypeError` is raised if the specified config file does not exist.
    """
    args = Namespace(commands="update-broker", config_file="nonexistent.yaml")
    cmd = ConfigCommand()

    with pytest.raises(ArgumentTypeError, match="does not exist"):
        cmd.process_command(args)


def test_process_command_raises_on_invalid_yaml(cli_testing_dir: FixtureStr):
    """
    Verify that an `ArgumentTypeError` is raised if the config file contains invalid YAML.

    Args:
        cli_testing_dir: The path to the temporary ouptut directory for cli tests.
    """
    invalid_yaml = os.path.join(cli_testing_dir, "invalid.yaml")
    with open(invalid_yaml, "w") as invalid_yaml_file:
        invalid_yaml_file.write("foo: [bar")

    args = Namespace(commands="update-broker", config_file=str(invalid_yaml), type="redis")

    cmd = ConfigCommand()
    with pytest.raises(ArgumentTypeError, match="is not a valid YAML file"):
        cmd.process_command(args)
