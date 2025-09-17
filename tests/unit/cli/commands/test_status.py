##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `status.py` file of the `cli/` folder.
"""

from argparse import Namespace

import pytest

from merlin.cli.commands.status import DetailedStatusCommand, StatusCommand
from tests.fixture_types import FixtureCallable


class TestStatusCommand:
    """
    Tests for the `StatusCommand` object.
    """

    def test_add_parser_sets_up_status_command(self, create_parser: FixtureCallable):
        """
        Test that the `status` command parser sets up the expected defaults and subcommands.

        Args:
            create_parser: A fixture to help create a parser.
        """
        command = StatusCommand()
        parser = create_parser(command)
        args = parser.parse_args(["status", "study.yaml"])
        assert hasattr(args, "func")
        assert args.func.__name__ == command.process_command.__name__
        assert args.spec_or_workspace == "study.yaml"
        assert not args.cb_help
        assert args.dump is None
        assert not args.no_prompts
        assert args.task_server == "celery"
        assert args.output_path is None
        assert not args.detailed

    def test_process_command_invalid_task_server(self):
        args = Namespace(
            spec_or_workspace="path/to/spec.yaml",
            task_server="dask",
            dump=None,
            detailed=False,
        )

        with pytest.raises(ValueError, match="only supported task server is celery"):
            StatusCommand().process_command(args)

    def test_process_command_invalid_dump_extension(self):
        args = Namespace(
            spec_or_workspace="path/to/spec.yaml",
            task_server="celery",
            dump="invalid.txt",
            detailed=False,
        )

        with pytest.raises(ValueError, match="must end with .csv or .json"):
            StatusCommand().process_command(args)

    def test_process_command_with_valid_spec_file_and_status(self, mocker):
        mock_verify_filepath = mocker.patch("merlin.cli.commands.status.verify_filepath", return_value="path/to/spec.yaml")
        mock_get_spec = mocker.patch("merlin.cli.commands.status.get_spec_with_expansion")
        mock_status_obj = mocker.Mock()
        mock_status_cls = mocker.patch("merlin.cli.commands.status.Status", return_value=mock_status_obj)

        args = Namespace(
            spec_or_workspace="path/to/spec.yaml",
            task_server="celery",
            dump=None,
            detailed=False,
        )

        StatusCommand().process_command(args)

        mock_verify_filepath.assert_called_once_with("path/to/spec.yaml")
        mock_get_spec.assert_called_once_with("path/to/spec.yaml")
        mock_status_cls.assert_called_once()
        mock_status_obj.display.assert_called_once()

    def test_process_command_dumps_status(self, mocker):
        mocker.patch("merlin.cli.commands.status.verify_filepath", return_value="spec.yaml")
        mocker.patch("merlin.cli.commands.status.get_spec_with_expansion")
        mock_status_obj = mocker.Mock()
        mocker.patch("merlin.cli.commands.status.Status", return_value=mock_status_obj)

        args = Namespace(
            spec_or_workspace="spec.yaml",
            task_server="celery",
            dump="status.json",
            detailed=False,
        )

        StatusCommand().process_command(args)

        mock_status_obj.dump.assert_called_once()

    def test_process_command_invalid_path_logs_error(self, mocker):
        mocker.patch("merlin.cli.commands.status.verify_filepath", side_effect=ValueError)
        mocker.patch("merlin.cli.commands.status.verify_dirpath", side_effect=ValueError)
        mock_log = mocker.patch("merlin.cli.commands.status.LOG")

        args = Namespace(
            spec_or_workspace="badpath",
            task_server="celery",
            dump=None,
            detailed=False,
        )

        result = StatusCommand().process_command(args)
        assert result is None
        mock_log.error.assert_called_once_with("The file or directory path badpath does not exist.")


class TestDetailedStatusCommand:
    """
    Tests for the `DetailedStatusCommand` object.
    """

    def test_add_parser_sets_up_detailed_status_command(self, create_parser: FixtureCallable):
        """
        Test that the `detailed-status` command parser sets up the expected defaults and subcommands.

        Args:
            create_parser: A fixture to help create a parser.
        """
        command = DetailedStatusCommand()
        parser = create_parser(command)
        args = parser.parse_args(["detailed-status", "study.yaml"])
        assert hasattr(args, "func")
        assert args.func.__name__ == command.process_command.__name__
        assert args.spec_or_workspace == "study.yaml"
        assert args.dump is None
        assert args.task_server == "celery"
        assert args.output_path is None
        assert args.layout == "default"
        assert args.steps == ["all"]
        assert not args.disable_pager
        assert not args.disable_theme
        assert not args.no_prompts
        assert args.detailed

    def test_add_parser_normalizes_task_status_and_return_code_filters(self, create_parser: FixtureCallable):
        """
        Test that the DetailedStatusCommand parser normalizes `--return-code` and `--task-status`
        values to uppercase, regardless of input case.

        Args:
            create_parser: A fixture to help create a parser.
        """
        command = DetailedStatusCommand()
        parser = create_parser(command)

        # Simulate user input with mixed/lowercase values
        args = parser.parse_args(
            [
                "detailed-status",
                "some_workspace",
                "--return-code",
                "success",
                "Soft_Fail",
                "--task-status",
                "running",
                "Failed",
            ]
        )

        # Both return_code and task_status should be uppercased
        assert args.return_code == ["SUCCESS", "SOFT_FAIL"]
        assert args.task_status == ["RUNNING", "FAILED"]

    def test_process_command_with_valid_workspace_and_detailed_status(self, mocker):
        mock_verify_filepath = mocker.patch("merlin.cli.commands.status.verify_filepath", side_effect=ValueError)
        mock_verify_dirpath = mocker.patch("merlin.cli.commands.status.verify_dirpath", return_value="path/to/workspace")
        mock_detailed_obj = mocker.Mock()
        mock_detailed_cls = mocker.patch("merlin.cli.commands.status.DetailedStatus", return_value=mock_detailed_obj)

        args = Namespace(
            spec_or_workspace="path/to/workspace",
            task_server="celery",
            dump=None,
            detailed=True,
        )

        StatusCommand().process_command(args)

        mock_verify_filepath.assert_called_once()
        mock_verify_dirpath.assert_called_once_with("path/to/workspace")
        mock_detailed_cls.assert_called_once_with(args, False, "path/to/workspace")
        mock_detailed_obj.display.assert_called_once()
