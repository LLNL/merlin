##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `MerlinStatusRendererFactory` class in the `status_renderers.py` module.
"""

import pytest
from maestrowf import BaseStatusRenderer

from merlin.exceptions import MerlinInvalidStatusRendererError
from merlin.study.status_renderers import MerlinDefaultRenderer, MerlinFlatRenderer, MerlinStatusRendererFactory


class TestMerlinStatusRendererFactory:
    """
    Test suite for the `MerlinStatusRendererFactory`.

    This class verifies that the factory correctly registers, resolves, instantiates,
    and reports status renderer components for the Merlin workflow framework.
    """

    @pytest.fixture
    def renderer_factory(self) -> MerlinStatusRendererFactory:
        """
        An instance of the `MerlinStatusRendererFactory` class. Resets on each test.

        Returns:
            A fresh instance of the renderer factory.
        """
        return MerlinStatusRendererFactory()

    def test_list_available_renderers(self, renderer_factory: MerlinStatusRendererFactory):
        """
        Test that `list_available` returns the correct built-in renderers.
        """
        available = renderer_factory.list_available()
        assert set(available) == {"default", "table"}

    @pytest.mark.parametrize("renderer_type, expected_cls", [
        ("default", MerlinDefaultRenderer),
        ("table", MerlinFlatRenderer),
    ])
    def test_create_valid_renderer(self, renderer_factory: MerlinStatusRendererFactory, renderer_type: str, expected_cls: BaseStatusRenderer):
        """
        Test that `create` returns the correct renderer instance for each type.
        """
        instance = renderer_factory.create(renderer_type)
        assert isinstance(instance, expected_cls)

    def test_create_invalid_renderer_raises(self, renderer_factory: MerlinStatusRendererFactory):
        """
        Test that `create` raises an error for an unrecognized renderer name.
        """
        with pytest.raises(MerlinInvalidStatusRendererError, match="not supported"):
            renderer_factory.create("unsupported_renderer")

    def test_invalid_registration_raises_type_error(self, renderer_factory: MerlinStatusRendererFactory):
        """
        Test that trying to register a non-renderer raises a TypeError.
        """
        class NotARenderer:
            pass

        with pytest.raises(TypeError, match="must inherit from BaseStatusRenderer"):
            renderer_factory.register("fake", NotARenderer)
