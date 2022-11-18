PYTHON?=python3
PYV=$(shell $(PYTHON) -c "import sys;t='{v[0]}_{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
PYVD=$(shell $(PYTHON) -c "import sys;t='{v[0]}.{v[1]}'.format(v=list(sys.version_info[:2]));sys.stdout.write(t)")
VENV?=venv_merlin_py_$(PYV)
PIP?=$(VENV)/bin/pip
MRLN=merlin
TEST=tests
UNIT=$(TEST)/unit
DOCS=docs
WKFW=merlin/examples/workflows/
# check setup.cfg exists
ifeq (,$(wildcard setup.cfg))
	MAX_COMPLEXITY?=15
	MAX_LINE_LENGTH=127
else
	MAX_COMPLEXITY?=$(shell grep 'max-complexity' setup.cfg | cut -d ' ' -f3)
	MAX_LINE_LENGTH=$(shell grep 'max-line-length' setup.cfg | cut -d ' ' -f3)
endif

VER?=1.0.0
VSTRING=[0-9]\+\.[0-9]\+\.[0-9]\+
CHANGELOG_VSTRING="## \[$(VSTRING)\]"
INIT_VSTRING="__version__ = \"$(VSTRING)\""

PENV=merlin$(PYV)
