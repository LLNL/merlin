"""
Tests for the `store_base.py` module.
"""

from typing import List

import pytest

from merlin.backends.store_base import StoreBase
from merlin.db_scripts.data_models import BaseDataModel


class DummyModel(BaseDataModel):
    """A minimal model stub for testing."""

    def __init__(self, id: str):
        self.id = id


def test_store_base_is_abstract():
    """
    Test that StoreBase cannot be instantiated directly.
    """
    with pytest.raises(TypeError):
        StoreBase()  # type: ignore


def test_incomplete_subclass_raises_type_error():
    """
    Test that a subclass that does not implement all abstract methods raises TypeError.
    """

    class IncompleteStore(StoreBase[DummyModel]):
        def save(self, entity: DummyModel):
            pass  # only implements one of four required methods

    with pytest.raises(TypeError):
        IncompleteStore()  # type: ignore


def test_complete_subclass_can_be_instantiated():
    """
    Test that a subclass implementing all abstract methods can be instantiated.
    """

    class CompleteStore(StoreBase[DummyModel]):
        def save(self, entity: DummyModel):
            self._store = {entity.id: entity}

        def retrieve(self, entity_id: str) -> DummyModel:
            return self._store[entity_id]

        def retrieve_all(self) -> List[DummyModel]:
            return list(self._store.values())

        def delete(self, entity_id: str):
            del self._store[entity_id]

    store = CompleteStore()
    assert isinstance(store, StoreBase)
