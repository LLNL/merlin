###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.3.
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

# Taken from https://lc.llnl.gov/mlsi/docs/merlin/merlin_config.html
TEMPLATE_FILE_CONTENTS = """
####################################
# Description Block (Required)
####################################
# The description block is where the description of the study is placed. This
# section is meant primarily for documentation purposes so that when a
# specification is passed to other users they can glean a general understanding
# of what this study is meant to achieve.
#-------------------------------
# Required keys:
#   name - Name of the study
#   description - Description of what this study does.
#-------------------------------
# NOTE: You can add other keys to this block for custom documentation. Merlin
# currently only looks for the required set.
####################################
description:
  description: Run a scan through Merlin
  name: MERLIN

####################################
# Batch Block (Required)
####################################
# The batch system to use for each allocation
#-------------------------------
# Required keys:
#   type - The scheduler type to use (local|slurm|flux|lsf)
#   bank - The allocation bank
#   queue - The batch queue
####################################
batch:
   type: flux
   bank: testbank
   queue: pbatch
   flux_path: <optional path to flux bin>
   flux_start_opts: <optional flux start options
   shell: <the interpreter to use for the script after the shebang>
          # e.g. /bin/bash, /bin/tcsh, python, /usr/bin/env perl, etc.


#####################################
# Environment Block
####################################
# The environment block is where items describing the study's environment are
# defined. This includes static information that the study needs to know about
# and dependencies that the workflow requires for execution.
#-------------------------------
# NOTE: This block isn't strictly required as a study may not depend on anything.
########################################################################
env:
  #-------------------------------
  # Variables
  #-------------------------------
  # Values that the workflow substitutes into steps and are similar in
  # concept to Unix environment variables. These variables are not dependent
  # on values in the environment and so are more portable.
  # set a custom output path for the study workspace. This path is where
  # Merlin will place all temporary files, state files, and any output.
  # The resulting path is usually a timestamped folder within OUTPUT_PATH
  # and in this case would be './sample_output/merlin/merlin_sample1_<timestamp>'.
  # Variables are useful for ensuring consistency with fixed formatting for
  # output files, or fixed formatting for components of steps.

  # NOTE: If not specified, OUTPUT_PATH is assumed to be the path where Merlin was launched from.
  # NOTE: If the '-o' flag is specified for the run subcommand, OUTPUT_PATH
  # the output path will be taken from there and will not generate a
  # timestamped path.
  #-------------------------------
  variables:
      OUTPUT_PATH: ./studies

####################################
# Study Block (Required)
####################################
# The study block is where the steps in the workflow are defined. This section
# of the specification represents the unexpanded set of tasks that the study
# is composed of.
#
#
# A description of what gets turned into tasks and what type of task
# would be a good addition
#
#######################################################################
study:
  - name: runs1
    description: Run on alloc1
    run:
     cmd: echo "$(VAR1) $(VAR2)" > simrun.out
     task_queue: queue1

  - name: post-process
    description: Post-Process runs on alloc1
    run:
      cmd: |
        cd $(runs1.workspace)/$(MERLIN_SAMPLE_PATH)
        echo "<post-process-command>"
      depends: [runs1]
      task_queue: queue1

  - name: runs2
    description: Run on alloc2
    run:
      cmd: |
        touch learnrun.out
        echo "$(VAR1) $(VAR2)" >> learnrun.out
        exit $(MERLIN_RESTART) # some syntax to catch a retry error code
      task_queue: lqueue
      max_retries: 5 # workflow will fail if retries exceeds this

  - name: monitor
    description: Monitor on alloc1
    run:
      cmd: date > monitor.out
      task_queue: mqueue

####################################
# Parameter Block (Required)
####################################
# The parameter block contains all the things we'd like to vary in the study.
# Currently, there are two modes of operating in the specification:
# 1. If a parameter block is specified, the study is expanded and considered a
#   parameterized study.
# 2. If a parameter block is not specified, the study is treated as linear and
#    the resulting study is not expanded.
#
# There are three keys per parameter:
# 1. A list of values that the parameter takes.
# 2. A label that represents a "pretty printed" version of the parameter. The
#    parameter values is specified by the '%%' moniker (for example, for SIZE --
#    when SIZE is equal to 10, the label will be 'SIZE.10'). To access the label
#    for SIZE, for example, the token '$(SIZE.label)' is used.
#    Labels can take one of two forms: A single string with the '%%' marker or
#    a list of per value labels (must be the same length as the list of values).
#
# NOTE: A specified parameter does not necessarily have to be used in every step
# or at all. If a parameter is specified and not used, it simply will not be
# factored into expansion or the naming of expanded steps or their workspaces.
# NOTE: You can also specify custom generation of parameters using a Python
# file containing the definition of a function as follows:
#
# 'def get_custom_generator():'
#
# The 'get_custom_generator' function is required to return a ParameterGenerator
# instance populated with custom filled values. In order to use the file, simply
# call Merlin using 'merlin run <specification path>'.
########################################################################
global.parameters:
  STUDY:
    label: STUDY.%%
    values: [MERLIN1, MERLIN2]
  SIZE:
    label   : SIZE.%%
    values  : [10, 20]
  ITERATIONS:
    label   : ITER.%%
    values  : [10, 20]

####################################
# Merlin Block (Required)
####################################
# The merlin-specific block will add any required configuration to the study
# DAG including task server config, data management and sample definitions.
#
# Merlin will replace all SPECROOT instances with the directory where
# the input yaml was run.
#######################################################################
merlin:

  ####################################
  # Resource definitions
  #
  # Define the task server configuration and workers to run the tasks.
  #
  ####################################
  resources:
    task_server: celery
    overlap: False
    workers:
      simworkers:
        args: # <celery worker args> <optional>
        steps: [runs1, post-process, monitor]  # [all] when steps is omitted

      learnworkers:
        args: # <celery worker args> <optional>
        steps: [runs2]

  ####################################
  # Sample definitions
  ####################################
  samples:
    column_labels: [VAR1, VAR2]
    file: $(MERLIN_INFO)/samples.csv
    generate:
      cmd: |
        echo "Generate samples here."
        echo "1.0,1.0" > $(MERLIN_INFO)/samples.csv
        echo "2.0,2.0" >> $(MERLIN_INFO)/samples.csv
"""


SIMPLE_EXAMPLE = """
description:
    name: simple_example
    description: This is a single one step Merlin workflow example.

study:
    - name: iterate_count
      description: A simple counter.
      run:
        cmd: |
            echo "Count: $(COUNT)"

global.parameters:
    COUNT:
        values : [1, 2, 3, 4]
        label  : COUNT.%%
"""


# Metadata for the template.
TEMPLATES = [
    {
        "name": "sample_template",
        "filename": "sample_template.yaml",
        "description": "A simple single step Merlin workflow example",
        "content": SIMPLE_EXAMPLE,
    },
    {
        "name": "merlin_documentation",
        "filename": "template_spec.yaml",
        "description": "A fully documented Merlin example spec example",
        "content": TEMPLATE_FILE_CONTENTS,
    },
]
