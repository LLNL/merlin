###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""This module handles creating a formatted task-by-task status display"""
import logging
from typing import Dict, List, Optional, Union

from maestrowf import BaseStatusRenderer, FlatStatusRenderer, StatusRendererFactory
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

from merlin.study.status_constants import NON_WORKSPACE_KEYS


LOG = logging.getLogger(__name__)


def format_label(label_to_format: str, delimiter: Optional[str] = "_") -> str:
    """
    Take a string of the format 'word1_word2_...' and format it so it's prettier.
    This would turn the string above to 'Word1 Word2 ...'.

    :param `label_to_format`: The string we want to format
    :param `delimiter`: The character separating words in `label_to_format`
    :returns: A formatted string based on `label_to_format`
    """
    return label_to_format.replace(delimiter, " ").title()


class MerlinDefaultRenderer(BaseStatusRenderer):
    """
    This class handles the default status formatting for task-by-task display.
    It will separate the display on a step-by-step basis.

    Similar to Maestro's 'narrow' status display.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.disable_theme = kwargs.pop("disable_theme", False)
        self.disable_pager = kwargs.pop("disable_pager", False)

        # Setup default theme
        # TODO modify this theme to add more colors
        self._theme_dict = {
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
        self._status_table = Table.grid(padding=0)

    def create_param_table(self, parameters: Dict[str, Dict[str, str]]) -> Columns:
        """
        Create the parameter section of the display

        :param `parameters`: A dict of the form {"cmd": {"TOKEN1": "value1"}, "restart": {"TOKEN2": "value1"}}
        :returns: A rich Columns object with the parameter info formatted appropriately
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
        task_queue: Optional[str] = None,
        workers: Optional[str] = None,
    ) -> Table:
        """
        Create each step entry in the display

        :param `step_name`: The name of the step that we're setting the layout for
        :param `parameters`: The parameters dict for this step
        :param `task_queue`: The name of the task queue associated with this step if one was provided
        :param `workers`: The name of the worker(s) that ran this step if one was provided
        :returns: A rich Table object with info for one sub step (here a 'sub step' is referencing a step
                  with multiple parameters; each parameter set will have it's own entry in the output)
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
        Create the task details section of the display

        :param `task_statuses`: A dict of task statuses to format into our layout
        :returns: A rich Table with the formatted task info for a sub step
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

    def layout(
        self, status_data, study_title: Optional[str] = None, status_time: Optional[str] = None
    ):  # pylint: disable=W0237
        """
        Setup the overall layout of the display

        :param `status_data`: A dict of status data to display
        :param `study_title`: A title for the study to display at the top of the output
        :param `status_time`: A timestamp to add to the title
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

    def render(self, theme: Optional[Dict[str, str]] = None):
        """
        Do the actual printing

        :param `theme`: A dict of theme settings (see self._theme_dict for the appropriate layout)
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


class MerlinFlatRenderer(FlatStatusRenderer):
    """
    This class handles the flat status formatting for task-by-task display.
    It will not separate the display on a step-by-step basis and instead group
    all statuses together in a single table.

    Similar to Maestro's 'flat' status display.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
        self.disable_theme = kwargs.pop("disable_theme", False)
        self.disable_pager = kwargs.pop("disable_pager", False)

    def layout(
        self, status_data: Dict[str, List[Union[str, int]]], study_title: Optional[str] = None
    ):  # pylint: disable=W0221
        """
        Setup the layout of the display

        :param `status_data`: A dict of status information that we'll display
        :param `study_title`: The title of the study to display at the top of the output
        """
        if "cmd_parameters" in status_data:
            del status_data["cmd_parameters"]
        if "restart_parameters" in status_data:
            del status_data["restart_parameters"]

        # Capitalize column labels
        capitalized_keys = [format_label(key) for key in status_data]
        status_data = dict(zip(capitalized_keys, list(status_data.values())))

        super().layout(status_data, study_title=study_title)

    def render(self, theme: Optional[Dict[str, str]] = None):
        """
        Do the actual printing

        :param `theme`: A dict of theme settings (see self._theme_dict for the appropriate layout)
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


class MerlinStatusRendererFactory(StatusRendererFactory):
    """
    This class keeps track of all available status layouts for Merlin.
    """

    # TODO: when maestro releases the pager changes:
    # - remove init and render in MerlinFlatRenderer
    # - remove the get_renderer method below
    # - remove self.disable_theme and self.disable_pager from MerlinFlatRenderer and MerlinDefaultRenderer
    #   - these variables will be in BaseStatusRenderer in Maestro
    # - remove render method in MerlinDefaultRenderer
    #   - this will also be in BaseStatusRenderer in Maestro
    def __init__(self):  # pylint: disable=W0231
        self._layouts = {
            "table": MerlinFlatRenderer,
            "default": MerlinDefaultRenderer,
        }

    def get_renderer(self, layout: str, disable_theme: bool, disable_pager: bool):  # pylint: disable=W0221
        """Get handle for specific layout renderer to instantiate

        :param `layout`: A string denoting the name of the layout renderer to use
        :param `disable_theme`: True if the user wants to disable themes when displaying status.
                                False otherwise.
        :param `disable_pager`: True if the user wants to disable the pager when displaying status.
                                False otherwise.

        :returns: The status renderer class to use for displaying the output
        """
        renderer = self._layouts.get(layout)

        # Note, need to wrap renderer in try/catch too, or return default val?
        if not renderer:
            raise ValueError(layout)

        return renderer(disable_theme=disable_theme, disable_pager=disable_pager)


status_renderer_factory = MerlinStatusRendererFactory()
