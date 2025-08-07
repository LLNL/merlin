##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module handles creating a formatted task-by-task status display"""
import logging
from typing import Any, Dict, List, Type, Union

from maestrowf import BaseStatusRenderer, FlatStatusRenderer
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from merlin.abstracts import MerlinBaseFactory
from merlin.exceptions import MerlinInvalidStatusRendererError
from merlin.study.status_constants import NON_WORKSPACE_KEYS


LOG = logging.getLogger(__name__)


def format_label(label_to_format: str, delimiter: str = "_") -> str:
    """
    Format a string by replacing a specified delimiter with spaces and capitalizing each word.

    This function takes a string that uses a specific delimiter to separate words and returns a
    more readable version of that string, where the words are separated by spaces and each word
    is capitalized.

    Args:
        label_to_format: The string to format.
        delimiter: The character that separates words in `label_to_format`.

    Returns:
        A formatted string where the delimiter is replaced with spaces and each word is capitalized.
    """
    return label_to_format.replace(delimiter, " ").title()


class MerlinDefaultRenderer(BaseStatusRenderer):
    """
    This class handles the default status formatting for task-by-task display.
    It will separate the display on a step-by-step basis, similar to Maestro's 'narrow' status display.

    Attributes:
        disable_theme (bool): Flag to disable theming for the display.
        disable_pager (bool): Flag to disable pager functionality for the display.
        _theme_dict (Dict[str, str]): A dictionary containing the theme settings for various status types.
        _status_table (Table): A Table object that contains the formatted status information.

    Methods:
        create_param_table: Creates the parameter section of the display.
        create_step_table: Creates each step entry in the display.
        create_task_details_table: Creates the task details section of the display.
        layout: Sets up the overall layout of the display.
        render: Performs the actual printing of the status table with optional theme customization.
    """

    def __init__(self, *args: List, **kwargs: Dict):
        """
        Initializes the `MerlinDefaultRenderer` instance, which handles the default status formatting
        for task-by-task display, with optional theming and pager functionality.

        Args:
            *args: Positional arguments passed to the superclass (`BaseStatusRenderer`).
            **kwargs: Keyword arguments used to configure the renderer. Supported keys include:\n
                - disable_theme (bool, optional): If `True`, disables theming for the display. Defaults to `False`.
                - disable_pager (bool, optional): If `True`, disables pager functionality for the display. Defaults to `False`.
        """
        super().__init__(*args, **kwargs)

        # Setup default theme
        # TODO modify this theme to add more colors
        self._theme_dict: Dict[str, str] = {
            "INITIALIZED": "blue",
            "RUNNING": "blue",
            "DRY_RUN": "green",
            "FINISHED": "green",
            "CANCELLED": "yellow",
            "FAILED": "bold red",
            "UNKNOWN": "bold red",
            "Step Name": "bold",
            "Workspace": "blue",
            "row_style": "",
            "row_style_dim": "dim",
            "row_style_failed": "bold red",
            "col_style_1": "",
            "col_style_2": "blue",
            "background": "grey7",
        }

        # Setup the status table that will contain our formatted status
        self._status_table: Table = Table.grid(padding=0)

    def create_param_table(self, parameters: Dict[str, Dict[str, str]]) -> Columns:
        """
        Create the parameter section of the display.

        This method generates a formatted table for the parameters associated with each command type.
        Each command type (e.g., "cmd", "restart") will have its own sub-table displaying the tokens
        and their corresponding values.

        Args:
            parameters: A dictionary where each key is a command type (e.g., "cmd", "restart")
                and each value is another dictionary containing token-value pairs.
                Example format: `{"cmd": {"TOKEN1": "value1"}, "restart": {"TOKEN2": "value1"}}`.

        Returns:
            A rich Columns object containing the formatted parameter tables, arranged side-by-side.
        """
        param_table = []
        # Loop through cmd and restart entries
        for param_type, param_set in parameters.items():
            # If there are no parameters, don't create a table
            if param_set is None:
                continue

            # Set up the table for this parameter type
            param_subtable = Table(
                title=format_label(f"{param_type} Parameters"), show_header=False, show_lines=True, box=box.HORIZONTALS
            )

            # Col names don't actually matter, we're just creating the style here
            style = "blue" if not self.disable_theme else ""
            param_subtable.add_column("token", style="")  # This col will have all the token values
            param_subtable.add_column("val", style=style, justify="right")  # This col will have all the parameter values
            param_subtable.add_column("padding1", style="")  # This col is just for padding in the display
            param_subtable.add_column("padding2", style=style, justify="right")  # This col is just for padding in the display

            # Loop through each parameter token/val for this param type and create a row entry for each token/val
            for token, param_val in param_set.items():
                param_val = str(param_val)
                param_subtable.add_row(token, param_val, style="row_style")

            # Add the sub table for this parameter type to the list that will store both sub tables
            param_table.append(param_subtable)

        # Put the tables side-by-side in columns and return it
        return Columns(param_table)

    def create_step_table(
        self,
        step_name: str,
        parameters: Dict[str, Dict[str, str]],
        task_queue: str = None,
        workers: str = None,
    ) -> Table:
        """
        Create each step entry in the display.

        This method constructs a formatted table entry for a specific step in the process, including
        relevant details such as the step name, associated task queue, worker(s), and any parameters
        related to the step. Each parameter set will be displayed in a sub-table format.

        Args:
            step_name: The name of the step for which the layout is being created.
            parameters: A dictionary of parameters associated with the step, where each key is a
                parameter type and each value is a dictionary of token-value pairs.
            task_queue: The name of the task queue associated with this step, if provided.
            workers: The name(s) of the worker(s) that executed this step, if provided.

        Returns:
            A rich Table object containing the formatted information for the specified step,
                including its parameters and any associated task queue or worker details.
        """
        # Initialize the table that will have our step entry information
        step_table = Table(box=box.SIMPLE_HEAVY, show_header=False)

        # Dummy columns used just for aligning our content properly
        step_table.add_column("key")
        step_table.add_column("val", overflow="fold")

        # Top level contains step name and may contain task queue and worker name
        step_table.add_row("STEP:", step_name, style="Step Name")
        if workers is not None:
            step_table.add_row("WORKER(S):", ", ".join(workers), style="Workspace")
        if task_queue is not None:
            step_table.add_row("TASK QUEUE:", task_queue, style="Workspace")

        step_table.add_row("", "")  # just a little whitespace

        # Add optional parameter tables, if step has parameters
        param_table = self.create_param_table(parameters)
        step_table.add_row("", param_table)

        return step_table

    def create_task_details_table(self, task_statuses: Dict) -> Table:
        """
        Create the task details section of the display.

        This method constructs a formatted table that displays detailed information about various tasks,
        including their statuses, return codes, elapsed times, run times, restarts, and associated workers.
        Each task is represented as a row in the table, with specific styling applied based on the task's status.

        Args:
            task_statuses: A dictionary containing task statuses, where each key represents a step workspace
                and each value is another dictionary with details such as status, return code, elapsed time,
                run time, restarts, and workers.

        Returns:
            A rich Table object containing the formatted task details, structured for easy readability
                and visual distinction based on task status.
        """
        # Initialize the task details table
        task_details = Table(title="Task Details")

        # Setup the columns
        cols = ["Step Workspace", "Status", "Return Code", "Elapsed Time", "Run Time", "Restarts", "Worker(s)"]
        for nominal_col_num, col in enumerate(cols):
            if col in list(self._theme_dict):
                col_style = col
            else:
                if nominal_col_num % 2 == 0:
                    col_style = "col_style_1"
                else:
                    col_style = "col_style_2"

            task_details.add_column(format_label(col), style=col_style, overflow="fold")

        # Set up the rows
        row_style = "row_style"
        for step_workspace, status_info in task_statuses.items():
            # Ignore the non-workspace keys
            if step_workspace in NON_WORKSPACE_KEYS:
                continue

            # Create each row entry
            status_entry = [step_workspace]
            for status_info_key, status_info_val in status_info.items():
                # For status entries we'll color the column differently
                if status_info_key == "status":
                    status_entry.append(Text(status_info_val, style=self._theme_dict[status_info_val]))
                    # If we have a failed task then let's make that stand out by bolding and styling the whole row red
                    if status_info_val in ("FAILED", "UNKNOWN"):
                        row_style = "row_style_failed"
                elif status_info_key == "workers":
                    status_entry.append(", ".join(status_info_val))
                else:
                    status_entry.append(str(status_info_val))

            # Add the row entry to the task details table
            task_details.add_row(*status_entry, style=row_style)

            # Change styling for each row so statuses stand out more
            row_style = "row_style" if row_style == "row_style_dim" else "row_style_dim"

        return task_details

    def layout(self, status_data: Dict, study_title: str = None, status_time: str = None):  # pylint: disable=W0237
        """
        Setup the overall layout of the display.

        This method configures the main display layout for the status data, including setting up
        the title with optional study information and timestamp. It organizes the status data into
        a structured table format, displaying each step's details along with associated task information.

        Args:
            status_data: A dictionary containing status data to be displayed, where each key
                represents a step and its associated information.
            study_title: A title for the study to be displayed at the top of the output.
            status_time: A timestamp to be included in the title, indicating when the status
                data was captured.

        Raises:
            ValueError: If `status_data` is not a dictionary or is empty.
        """
        if isinstance(status_data, dict) and status_data:
            self._status_data = status_data
        else:
            raise ValueError("Status data must be a dict")

        # Create the table title
        table_title = ""
        if status_time:
            table_title += f"Status as of {status_time}"
        if study_title:
            if status_time:
                table_title += "\n"
            table_title += f"Study: {study_title}"
        if table_title:
            LOG.debug(f"Table title: {table_title}")
            self._status_table.title = table_title

        # Create settings for the entire display
        self._status_table.box = box.HEAVY
        self._status_table.show_lines = True
        self._status_table.show_edge = False
        self._status_table.show_footer = True
        self._status_table.collapse_padding = True

        # Uses folding overflow for very long step/workspace names
        self._status_table.add_column("Step", overflow="fold")

        # Build out the status table by sectioning it off at each step
        for step_name, overall_step_info in self._status_data.items():
            task_queue = overall_step_info["task_queue"] if "task_queue" in overall_step_info else None
            workers = overall_step_info["workers"] if "workers" in overall_step_info else None

            # Set up the top section of each step entry
            # (this section will have step name, task queue, worker name, and parameters)
            step_table = self.create_step_table(
                step_name, overall_step_info["parameters"], task_queue=task_queue, workers=workers
            )

            # Set up the bottom section of each step entry
            # (this section will have task-by-task info; status, return code, run time, etc.)
            sample_details_table = self.create_task_details_table(overall_step_info)

            # Add the bottom section to the top section
            step_table.add_row("", sample_details_table)

            # Add this step to the full status table
            self._status_table.add_row(step_table, end_section=True)


class MerlinFlatRenderer(FlatStatusRenderer):
    """
    This class handles the flat status formatting for task-by-task display.
    It will not separate the display on a step-by-step basis and instead group
    all statuses together in a single table, similar to Maestro's 'flat' status display.

    Attributes:
        disable_theme (bool): A flag indicating whether to disable theme customization for the output.
        disable_pager (bool): A flag indicating whether to disable the use of a pager for long outputs.

    Methods:
        layout: Sets up the layout of the display, formatting the status data and study title.
        render: Renders the status table to the console, applying any specified theme settings and
            managing the output display.
    """

    def layout(self, status_data: Dict[str, List[Union[str, int]]], study_title: str = None):  # pylint: disable=W0221
        """
        Set up the layout of the display for the status information.

        This method processes the provided status data by removing unnecessary parameters,
        capitalizing the column labels, and preparing the data for display. It also allows
        for an optional study title to be displayed at the top of the output.

        Args:
            status_data: A dictionary containing status information to be displayed. The
                keys represent the status categories, and the values are lists of
                corresponding status values.
            study_title: The title of the study to display at the top of the output.
                If provided, it will be included in the layout.
        """
        if "cmd_parameters" in status_data:
            del status_data["cmd_parameters"]
        if "restart_parameters" in status_data:
            del status_data["restart_parameters"]

        # Capitalize column labels
        capitalized_keys = [format_label(key) for key in status_data]
        status_data = dict(zip(capitalized_keys, list(status_data.values())))

        super().layout(status_data, study_title=study_title)

    def render(self, theme: Dict[str, str] = None):
        """
        Render the status table to the console.

        This method is responsible for displaying the formatted status information
        in the console. It applies any specified theme settings to customize the
        appearance of the output. If the theme is disabled, it sets all theme
        attributes to 'none'. The method also handles the output display, either
        printing directly to the console or using a pager for long outputs based
        on the `disable_pager` attribute.

        Args:
            theme (Dict[str, str], optional): A dictionary of theme settings that
                customize the appearance of the output. The keys represent the
                theme attributes, and the values are the corresponding settings.
                If not provided, the default theme settings will be used.
        """
        # Apply any theme customization
        if theme:
            LOG.debug(f"Applying theme: {theme}")
            for key, value in theme.items():
                self._theme_dict[key] = value

        # If we're disabling the theme, we need to set all themes in the theme dict to none
        if self.disable_theme:
            LOG.debug("Disabling theme.")
            for key in self._theme_dict:
                self._theme_dict[key] = "none"

        # Get the rich Console
        status_theme = Theme(self._theme_dict)
        _printer = Console(theme=status_theme)

        # Display the status table
        if self.disable_pager:
            _printer.print(self._status_table)
        else:
            with _printer.pager(styles=(not self.disable_theme)):
                _printer.print(self._status_table)


class MerlinStatusRendererFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating Merlin status renderers.

    This subclass of `MerlinBaseFactory` is responsible for registering,
    validating, and creating instances of supported `BaseStatusRenderer`
    implementations (e.g., `MerlinFlatRenderer`, `MerlinDefaultRenderer`).
    It also supports dynamic discovery of plugins via Python entry points.

    Responsibilities:
        - Register built-in status renderer implementations.
        - Validate that all components subclass `BaseStatusRenderer`.
        - Provide a unified interface for instantiating renderers by name or alias.
        - Optionally support discovery of external plugins.

    Attributes:
        _registry (Dict[str, BaseStatusRenderer]): Maps canonical names to renderer classes.
        _aliases (Dict[str, str]): Maps alternate names to canonical names.

    Methods:
        register: Register a renderer class and optional aliases.
        list_available: Return a list of supported renderers.
        create: Instantiate a renderer by name or alias.
        get_component_info: Return metadata about a registered renderer.
    """

    def _register_builtins(self):
        """
        Register built-in status renderer implementations.
        """
        self.register("table", MerlinFlatRenderer)
        self.register("default", MerlinDefaultRenderer)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of BaseStatusRenderer.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass BaseStatusRenderer.
        """
        if not issubclass(component_class, BaseStatusRenderer):
            raise TypeError(f"{component_class} must inherit from BaseStatusRenderer")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering status renderer plugins.

        Returns:
            The entry point namespace for Merlin status renderer plugins.
        """
        return "merlin.study"  # TODO change this to merlin.status when we refactor status

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        """
        Raise an appropriate exception for unsupported components.

        This method is used by the base factory logic to determine which
        exception to raise when a requested component is not found or fails
        to initialize.

        Args:
            msg: The message to add to the error being raised.

        Returns:
            The exception class to raise.
        """
        raise MerlinInvalidStatusRendererError(msg)


status_renderer_factory = MerlinStatusRendererFactory()
