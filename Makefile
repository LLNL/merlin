##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

include config.mk

.PHONY : virtualenv
.PHONY : install-merlin
.PHONY : install-workflow-deps
.PHONY : install-dev
.PHONY : unit-tests
.PHONY : command-tests
.PHONY : workflow-tests
.PHONY : integration-tests
.PHONY : e2e-tests
.PHONY : e2e-tests-diagnostic
.PHONY : e2e-tests-local
.PHONY : e2e-tests-local-diagnostic
.PHONY : tests
.PHONY : check-flake8
.PHONY : check-black
.PHONY : check-isort
.PHONY : check-pylint
.PHONY : check-style
.PHONY : fix-style
.PHONY : check-push
.PHONY : check-camel-case
.PHONY : checks
.PHONY : reqlist
.PHONY : release
.PHONY : clean-release
.PHONY : clean-output
.PHONY : clean-docs
.PHONY : clean-py
.PHONY : clean


# this only works outside the venv - if run from inside a custom venv, or any target that depends on this,
# you will break your venv.
virtualenv:
	$(PYTHON) -m venv $(VENV) --prompt $(PENV) --system-site-packages; \
	$(PIP) install --upgrade pip; \
	$(PIP) install -r requirements/release.txt; \


# install merlin into the virtual environment
install-merlin: virtualenv
	$(PIP) install -e .; \
	. $(VENV)/bin/activate; \
	merlin config; \


# install the example workflow to enable integrated testing
install-workflow-deps: virtualenv install-merlin
	$(PIP) install -r $(WKFW)feature_demo/requirements.txt; \


# install requirements
install-dev: virtualenv install-merlin install-workflow-deps
	$(PIP) install -r requirements/dev.txt; \


# tests require a valid dev install of merlin
unit-tests:
	. $(VENV)/bin/activate; \
	$(PYTHON) -m pytest -v --order-scope=module $(UNIT); \

command-tests:
	. $(VENV)/bin/activate; \
	$(PYTHON) -m pytest -v $(TEST)/integration/commands/; \


workflow-tests:
	. $(VENV)/bin/activate; \
	$(PYTHON) -m pytest -v $(TEST)/integration/workflows/; \


integration-tests: command-tests workflow-tests


# run CLI tests - these require an active install of merlin in a venv
e2e-tests:
	. $(VENV)/bin/activate; \
	$(PYTHON) $(TEST)/integration/run_tests.py; \


e2e-tests-diagnostic:
	. $(VENV)/bin/activate; \
	$(PYTHON) $(TEST)/integration/run_tests.py --verbose


e2e-tests-local:
	. $(VENV)/bin/activate; \
	$(PYTHON) $(TEST)/integration/run_tests.py --local; \


e2e-tests-local-diagnostic:
	. $(VENV)/bin/activate; \
	$(PYTHON) $(TEST)/integration/run_tests.py --local --verbose


# run unit and CLI tests
tests: unit-tests integration-tests e2e-tests


check-flake8:
	. $(VENV)/bin/activate; \
	echo "Flake8 linting for invalid source (bad syntax, undefined variables)..."; \
	$(PYTHON) -m flake8 --count --select=E9,F63,F7,F82 --show-source --statistics; \
	echo "Flake8 linting failure for CI..."; \
	$(PYTHON) -m flake8 . --count --max-complexity=15 --statistics --max-line-length=127; \


check-black:
	. $(VENV)/bin/activate; \
	$(PYTHON) -m black --check --line-length $(MAX_LINE_LENGTH) --target-version $(PY_TARGET_VER) $(MRLN); \
	$(PYTHON) -m black --check --line-length $(MAX_LINE_LENGTH) --target-version $(PY_TARGET_VER) $(TEST); \
	$(PYTHON) -m black --check --line-length $(MAX_LINE_LENGTH) --target-version $(PY_TARGET_VER) *.py; \


check-isort:
	. $(VENV)/bin/activate; \
	$(PYTHON) -m isort --check --line-length $(MAX_LINE_LENGTH) $(MRLN); \
	$(PYTHON) -m isort --check --line-length $(MAX_LINE_LENGTH) $(TEST); \
	$(PYTHON) -m isort --check --line-length $(MAX_LINE_LENGTH) *.py; \


check-pylint:
	. $(VENV)/bin/activate; \
	echo "PyLinting merlin source..."; \
	$(PYTHON) -m pylint merlin --rcfile=setup.cfg --ignore-patterns="$(VENV)/" --disable=logging-fstring-interpolation; \
	echo "PyLinting merlin tests..."; \
	$(PYTHON) -m pylint tests --rcfile=setup.cfg; \


check-copyright:
	@echo "🔍 Checking for required copyright header..."
	@missing_files=$$(find $(MRLN) $(TEST) \
		\( -path 'merlin/examples/workflows' -o -name '__pycache__' \) -prune -o \
		-name '*.py' -print | \
		xargs grep -L "Copyright (c) Lawrence Livermore National Security, LLC and other Merlin" || true); \
	if [ -n "$$missing_files" ]; then \
		echo "❌ The following files are missing the required copyright header:"; \
		echo "$$missing_files"; \
		exit 1; \
	else \
		echo "✅ All files contain the required header."; \
	fi


# run code style checks
check-style: check-copyright check-flake8 check-black check-isort check-pylint


check-push: tests check-style


# finds all strings in project that begin with a lowercase letter,
# contain only letters and numbers, and contain at least one lowercase
# letter and at least one uppercase letter.
check-camel-case: clean-py
	grep -rnw --exclude=lbann_pb2.py $(MRLN) -e "[a-z]\([A-Z0-9]*[a-z][a-z0-9]*[A-Z]\|[a-z0-9]*[A-Z][A-Z0-9]*[a-z]\)[A-Za-z0-9]*"


# run all checks
checks: check-style check-camel-case


# automatically make python files pep 8-compliant
fix-style:
	. $(VENV)/bin/activate; \
	$(PYTHON) -m isort -w $(MAX_LINE_LENGTH) $(MRLN); \
	$(PYTHON) -m isort -w $(MAX_LINE_LENGTH) $(TEST); \
	$(PYTHON) -m isort -w $(MAX_LINE_LENGTH) *.py; \
	$(PYTHON) -m black --target-version $(PY_TARGET_VER) -l $(MAX_LINE_LENGTH) $(MRLN); \
	$(PYTHON) -m black --target-version $(PY_TARGET_VER) -l $(MAX_LINE_LENGTH) $(TEST); \
	$(PYTHON) -m black --target-version $(PY_TARGET_VER) -l $(MAX_LINE_LENGTH) *.py; \


# # Increment the Merlin version. USE ONLY ON DEVELOP BEFORE MERGING TO MASTER.
# Usage: make version VER=1.13.0 FROM=1.13.0-beta
#        (defaults to FROM=Unreleased if not set)
version:
	@echo "Updating Merlin version from [$(FROM)] to [$(VER)]..."
	sed -i 's/__version__ = "\(.*\)"/__version__ = "$(VER)"/' merlin/__init__.py
	@if grep -q "## \[$(FROM)\]" CHANGELOG.md; then \
		sed -i 's/## \[$(FROM)\]/## [$(VER)]/' CHANGELOG.md; \
	else \
		echo "⚠️  No matching '## [$(FROM)]' found in CHANGELOG.md"; \
	fi

# Increment copyright year - Usage: make year YEAR=2026
year:
	@echo "Updating COPYRIGHT file to year $(YEAR)..."
	sed -i -E 's/(Copyright \(c\) 2019–)[0-9]{4}( Lawrence Livermore National Laboratory)/\1$(YEAR)\2/' COPYRIGHT

# Make a list of all dependencies/requirements
reqlist:
	johnnydep merlin --output-format pinned


release:
	$(PYTHON) -m build .


clean-release:
	rm -rf dist
	rm -rf build


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


# remove unwanted files
clean: clean-py clean-docs clean-release
