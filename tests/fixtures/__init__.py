##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This directory is for help modularizing fixture definitions so that we don't have to
store every single fixture in the `conftest.py` file.

Fixtures must start with the same name as the file they're defined in. For instance,
if our fixture file was named `example.py` then our fixtures in this file would have
to start with "example_":

```title="example.py"
import pytest

@pytest.fixture
def example_test_data():
    return {"key": "val"}
```
"""
