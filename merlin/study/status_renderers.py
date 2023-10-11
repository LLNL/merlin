###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.0
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
from rich.theme import Theme


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


# TODO see if we can modify these classes to use the json format of statuses rather than csv
# - Maestro uses csv format and we adopted a lot of their code here
# - the code might be easier to follow if we use json rather than csv format
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
        self._theme_dict = {
            "State": "bold red",
            "Step Name": "bold",
            "Workspace": "blue",
            "row_style": "",
            "row_style_dim": "dim",
            "col_style_1": "",
            "col_style_2": "blue",
            "background": "grey7",
        }

        # Setup the status table that will contain our formatted status
        self._status_table = Table.grid(padding=0)

    def create_param_subtable(self, params: str, param_type: str) -> Table:
        """
        Create the parameter section of the display

        :param `params`: A string of the form 'TOKEN:value;TOKEN2:value2;...'
        :param `param_type`: The type of parameter (either cmd or restart)
        :returns: A rich Table object with the parameter info formatted appropriately
        """
        if params == "-------":
            param_list = []
        else:
            param_list = params.split(";")
        LOG.debug(f"Creating param subtable using the following params: {param_list}")

        if len(param_list) > 0 and param_list[0]:
            if len(param_list) % 2 != 0:
                param_list.append("")

            num_param_rows = int(len(param_list) / 2)

            step_params = Table(title=format_label(param_type), show_header=False, show_lines=True, box=box.HORIZONTALS)

            # Note col names don't actually matter, just setting styles
            style = "blue" if not self.disable_theme else ""
            step_params.add_column("name", style="")
            step_params.add_column("val", style=style, justify="right")
            step_params.add_column("name2", style="")
            step_params.add_column("val2", style=style, justify="right")

            param_idx = 0
            for _ in range(num_param_rows):
                this_row = []
                for param_str in param_list[param_idx : param_idx + 2]:
                    if param_str:
                        this_row.extend(param_str.split(":"))
                    else:
                        this_row.extend(["", ""])

                param_idx += 2

                step_params.add_row(*this_row, style="row_style")
        else:
            step_params = None

        return step_params

    def create_step_subtable(self, row_num: int) -> Table:
        """
        Create each step entry in the display

        :param `row_num`: The index of the row we're currently at in the status_data object
        :returns: A rich Table object with info for one sub step (here a 'sub step' is referencing a step
                  with multiple parameters; each parameter set will have it's own entry in the output)
        """
        step_table = Table(box=box.SIMPLE_HEAVY, show_header=False)
        # Dummy columns
        step_table.add_column("key")
        step_table.add_column("val", overflow="fold")
        # Top level contains step name and workspace name, full table width
        step_table.add_row("STEP:", self._status_data["step_name"][row_num], style="Step Name")
        if "worker_name" in self._status_data:
            step_table.add_row("WORKER NAME:", self._status_data["worker_name"][row_num], style="Workspace")
        if "task_queue" in self._status_data:
            step_table.add_row("TASK QUEUE:", self._status_data["task_queue"][row_num], style="Workspace")

        step_table.add_row("", "")  # just a little whitespace

        # Add optional parameter tables, if step has parameters
        param_subtables = []
        for param_type in ("cmd_parameters", "restart_parameters"):
            params = self._status_data[param_type][row_num]
            step_params = self.create_param_subtable(params, param_type)
            if step_params is not None:
                param_subtables.append(step_params)
        step_table.add_row("", Columns(param_subtables))

        return step_table

    def create_task_details_subtable(self, cols: List[str]) -> Table:
        """
        Create the task details section of the display

        :param `cols`: A list of column names for each task info entry we'll display
        :returns: A rich Table with the formatted task info for a sub step
        """
        LOG.debug(f"Creating task details subtable using the following columns: {cols}")

        # We'll need a new task_details list now
        task_details = Table(title="Task Details")

        # Setup the column styles
        for nominal_col_num, col in enumerate(cols):
            if col in list(self._theme_dict):
                col_style = col
            else:
                if nominal_col_num % 2 == 0:
                    col_style = "col_style_1"
                else:
                    col_style = "col_style_2"

            task_details.add_column(format_label(col), style=col_style, overflow="fold")

        return task_details

    def layout(
        self, status_data, study_title: Optional[str] = None, status_time: Optional[str] = None
    ):  # pylint: disable=R0912,R0914
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

        self._status_table.box = box.HEAVY
        self._status_table.show_lines = True
        self._status_table.show_edge = False
        self._status_table.show_footer = True
        self._status_table.collapse_padding = True

        # Uses folding overflow for very long step/workspace names
        self._status_table.add_column("Step", overflow="fold")

        # Note, filter on columns here
        cols = [
            key
            for key in self._status_data.keys()
            if (key not in ("step_name", "cmd_parameters", "restart_parameters", "task_queue", "worker_name"))
        ]

        num_rows = len(self._status_data[cols[0]])
        LOG.debug(f"Setting the layout for {num_rows} rows of statuses")

        # We're going to create a sub table for each step so initialize that here
        step_table_tracker = {}
        for row_num, step_name in enumerate(self._status_data["step_name"]):
            if step_name not in step_table_tracker:
                step_table_tracker[step_name] = self.create_step_subtable(row_num)

        prev_step = ""
        # Setup one table to contain each steps' info
        for row in range(num_rows):
            curr_step = self._status_data["step_name"][row]

            # If we're on a new step and it's not the first one we're looking at,
            # add the previously built task_details sub-table to the step sub table
            if curr_step != prev_step and row != 0:
                step_table_tracker[prev_step].add_row("", task_details)  # noqa: F821

            # If we're on a new step, create a new step sub-table and task details sub-table
            if curr_step != prev_step:
                task_details = self.create_task_details_subtable(cols)

            if row % 2 == 0:
                row_style = "dim"
            else:
                row_style = "none"

            task_details.add_row(*[f"{self._status_data[key][row]}" for key in cols], style=row_style)

            if row == num_rows - 1:
                step_table_tracker[curr_step].add_row("", task_details)

            prev_step = curr_step

        for step_table in step_table_tracker.values():
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

    def layout(self, status_data: Dict[str, List[Union[str, int]]], study_title: Optional[str] = None):
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
