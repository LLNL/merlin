##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `util_sampling.py` file.
"""

import numpy as np
import pytest

from merlin.common.util_sampling import scale_samples


class TestUtilSampling:
    """
    This class will hold all of the tests for the `util_sampling.py` file.
    """

    def test_scale_samples_basic(self):
        """Test basic functionality"""
        samples_norm = np.array([[0.2, 0.4], [0.6, 0.8]])
        limits = [(-1, 1), (2, 6)]
        result = scale_samples(samples_norm, limits)
        expected_result = np.array([[-0.6, 3.6], [0.2, 5.2]])
        np.testing.assert_array_almost_equal(result, expected_result)

    def test_scale_samples_logarithmic(self):
        """Test functionality with log enabled"""
        samples_norm = np.array([[0.2, 0.4], [0.6, 0.8]])
        limits = [(1, 5), (1, 100)]
        result = scale_samples(samples_norm, limits, do_log=[False, True])
        expected_result = np.array([[1.8, 6.309573], [3.4, 39.810717]])
        np.testing.assert_array_almost_equal(result, expected_result)

    def test_scale_samples_invalid_input(self):
        """Test that function raises ValueError for invalid input"""
        with pytest.raises(ValueError):
            # Invalid input: samples_norm should be a 2D array
            scale_samples([0.2, 0.4, 0.6], [(1, 5), (2, 6)])

    def test_scale_samples_with_custom_limits_norm(self):
        """Test functionality with custom limits_norm"""
        samples_norm = np.array([[0.2, 0.4], [0.6, 0.8]])
        limits = [(1, 5), (2, 6)]
        limits_norm = (-1, 1)
        result = scale_samples(samples_norm, limits, limits_norm=limits_norm)
        expected_result = np.array([[3.4, 4.8], [4.2, 5.6]])
        np.testing.assert_array_almost_equal(result, expected_result)
