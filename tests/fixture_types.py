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
import pytest
from argparse import Namespace
from celery import Celery
from celery.canvas import Signature
from redis import Redis
from typing import Any, Annotated, Callable, Dict, Tuple, TypeVar

# TODO convert unit test type hinting to use these
# - likely will do this when I work on API docs for test library

K = TypeVar('K')
V = TypeVar('V')

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
