###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.5.0.
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

PYTHON?=python3
PYV=$(shell $(PYTHON) -c "import sys;t='{v[0]}_{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
PYVD=$(shell $(PYTHON) -c "import sys;t='{v[0]}.{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
VENV?=venv_merlin_py$(PYV)
PIP?=$(VENV)/bin/pip
MRLN=merlin
TEST=tests
DOCS=docs
WKFW=merlin/examples/workflows/
MAX_COMPLEXITY?=5

VER?=1.0.0
VSTRING=[0-9]\+\.[0-9]\+\.[0-9]\+
CHANGELOG_VSTRING="## \[$(VSTRING)\]"
INIT_VSTRING="__version__ = \"$(VSTRING)\""

PENV=merlin$(PYV)

.PHONY : all
.PHONY : install-dev
.PHONY : virtualenv
.PHONY : install-workflow-deps
.PHONY : install-merlin
.PHONY : clean-output
.PHONY : clean-docs
.PHONY : clean-release
.PHONY : clean-py
.PHONY : clean
.PHONY : release
.PHONY : unit-tests
.PHONY : cli-tests
.PHONY : tests
.PHONY : fix-style
.PHONY : check-style
.PHONY : check-camel-case
.PHONY : checks


all: install-dev install-merlin install-workflow-deps


# install requirements
install-dev: virtualenv
	$(PIP) install -r requirements/dev.txt


# this only works outside the venv
virtualenv:
	$(PYTHON) -m venv $(VENV) --prompt $(PENV) --system-site-packages
	$(PIP) install --upgrade pip


install-workflow-deps:
	$(PIP) install -r $(WKFW)feature_demo/requirements.txt


install-merlin:
	$(PIP) install -e .


# remove python bytecode files
clean-py:
	-find $(MRLN) -name "*.py[cod]" -exec rm -f {} \;
	-find $(MRLN) -name "__pycache__" -type d -exec rm -rf {} \;


# remove all studies/ directories
clean-output:
	-find $(MRLN) -name "studies*" -type d -exec rm -rf {} \;
	-find . -maxdepth 1 -name "studies*" -type d -exec rm -rf {} \;
	-find . -maxdepth 1 -name "merlin.log" -type f -exec rm -rf {} \;


# remove doc build files
clean-docs:
	rm -rf $(DOCS)/build


clean-release:
	rm -rf dist
	rm -rf build


# remove unwanted files
clean: clean-py clean-docs clean-release


release:
	$(PYTHON) setup.py sdist bdist_wheel


unit-tests:
	-$(PYTHON) -m pytest $(TEST)


# run CLI tests
cli-tests:
	-$(PYTHON) $(TEST)/integration/run_tests.py


# run unit and CLI tests
tests: unit-tests cli-tests


# automatically make python files pep 8-compliant
fix-style:
	isort -rc $(MRLN)
	isort -rc $(TEST)
	isort *.py
	black --target-version py36 $(MRLN)
	black --target-version py36 $(TEST)
	black --target-version py36 *.py


# run code style checks
check-style:
	-$(PYTHON) -m flake8 --max-complexity $(MAX_COMPLEXITY) --exclude ascii_art.py $(MRLN)
	-black --check --target-version py36 $(MRLN)


# finds all strings in project that begin with a lowercase letter,
# contain only letters and numbers, and contain at least one lowercase
# letter and at least one uppercase letter.
check-camel-case: clean-py
	grep -rnw --exclude=lbann_pb2.py $(MRLN) -e "[a-z]\([A-Z0-9]*[a-z][a-z0-9]*[A-Z]\|[a-z0-9]*[A-Z][A-Z0-9]*[a-z]\)[A-Za-z0-9]*"


# run all checks
checks: check-style check-camel-case


# Increment the Merlin version. USE ONLY ON DEVELOP BEFORE MERGING TO MASTER.
# Use like this: make VER=?.?.? verison
version:
	# do merlin/__init__.py
	sed -i 's/__version__ = "$(VSTRING)"/__version__ = "$(VER)"/g' merlin/__init__.py
	# do CHANGELOG.md
	sed -i 's/## \[Unreleased\]/## [$(VER)]/g' CHANGELOG.md
	# do all file headers (works on linux)
	find merlin/ -type f -print0 | xargs -0 sed -i 's/Version: $(VSTRING)/Version: $(VER)/g'
	find *.py -type f -print0 | xargs -0 sed -i 's/Version: $(VSTRING)/Version: $(VER)/g'

