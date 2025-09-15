##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
It's hard to type hint pytest fixtures in a way that makes it clear
that the variable being used is a fixture. This module will created
aliases for these fixtures in order to make it easier to track what's
happening.

The types here will be defined as such:
- `FixtureBytes`: A fixture that returns bytes
- `FixtureCelery`: A fixture that returns a Celery app object
- `FixtureDict`: A fixture that returns a dictionary
- `FixtureInt`: A fixture that returns an integer
- `FixtureModification`: A fixture that modifies something but never actually
                         returns/yields a value to be used in the test.
- `FixtureRedis`: A fixture that returns a Redis client
- `FixtureSignature`: A fixture that returns a Celery Signature object
- `FixtureStr`: A fixture that returns a string
"""

import sys
from argparse import Namespace
from collections.abc import Callable
from typing import Any, Dict, Generic, List, Tuple, TypeVar

import pytest
from celery import Celery
from celery.canvas import Signature
from redis import Redis


# TODO convert unit test type hinting to use these
# - likely will do this when I work on API docs for test library

K = TypeVar("K")
V = TypeVar("V")

# TODO when we drop support for Python 3.8, remove this if/else statement
# Check Python version
if sys.version_info >= (3, 9):
    from typing import Annotated

    FixtureBytes = Annotated[bytes, pytest.fixture]
    FixtureCallable = Annotated[Callable, pytest.fixture]
    FixtureCelery = Annotated[Celery, pytest.fixture]
    FixtureDict = Annotated[Dict[K, V], pytest.fixture]
    FixtureInt = Annotated[int, pytest.fixture]
    FixtureModification = Annotated[Any, pytest.fixture]
    FixtureNamespace = Annotated[Namespace, pytest.fixture]
    FixtureRedis = Annotated[Redis, pytest.fixture]
    FixtureSignature = Annotated[Signature, pytest.fixture]
    FixtureStr = Annotated[str, pytest.fixture]
    FixtureTuple = Annotated[Tuple[K, V], pytest.fixture]
    FixtureList = Annotated[List[K], pytest.fixture]
else:
    # Fallback for Python 3.8
    class FixtureDict(Generic[K, V], Dict[K, V]):
        """
        This class is necessary to allow FixtureDict to be subscriptable
        when using it to type hint.
        """

    class FixtureTuple(Generic[K, V], Tuple[K, V]):
        """
        This class is necessary to allow FixtureTuple to be subscriptable
        when using it to type hint.
        """

    class FixtureList(Generic[K, V], List[K]):
        """
        This class is necessary to allow FixtureList to be subscriptable
        when using it to type hint.
        """

    FixtureBytes = pytest.fixture
    FixtureCallable = pytest.fixture
    FixtureCelery = pytest.fixture
    FixtureInt = pytest.fixture
    FixtureModification = pytest.fixture
    FixtureNamespace = pytest.fixture
    FixtureRedis = pytest.fixture
    FixtureSignature = pytest.fixture
    FixtureStr = pytest.fixture
