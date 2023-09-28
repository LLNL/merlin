###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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

"""
Utility functions for sampling.
"""

import numpy as np


def scale_samples(samples_norm, limits, limits_norm=(0, 1), do_log=False):
    """Scale samples to new limits, either log10 or linearly.

    Args:
        samples_norm (ndarray): The normalized samples to scale,
            with dimensions (nsamples,ndims).
        limits (list of tuples): A list of (min, max) for the various
            dimensions. Length of list is ndims.
        limits_norm (tuple of floats, optional): The (min, max) from which
            samples_norm were drawn. Defaults to (0,1).
        do_log (boolean or list of booleans, optional): Whether
            to log10 scale each dimension. Either a single boolean or
            a list of length ndims, for each dimension.
            Defaults to ndims*[False].

    Returns:
        ndarray: The scaled samples.

    Note:
        We follow the sklearn convention of requiring samples to be
        given as an (nsamples, ndims) array.

        To transform 1-D arrays:

        >>> samples = samples.reshape((-1,1)) # ndims = 1
        >>> samples = samples.reshape((1,-1)) # nsamples = 1

    Example:

        >>> # Turn 0:1 samples into -1:1
        >>> import numpy as np
        >>> norm_values = np.linspace(0,1,5).reshape((-1,1))
        >>> real_values = scale_samples(norm_values, [(-1,1)])
        >>> print(real_values)
        [[-1. ]
         [-0.5]
         [ 0. ]
         [ 0.5]
         [ 1. ]]
        >>> # Logarithmically scale to 1:10000
        >>> real_values = scale_samples(norm_values, [(1,1e4)] do_log=True)
        >>> print(real_values)
        [[  1.00000000e+00]
         [  1.00000000e+01]
         [  1.00000000e+02]
         [  1.00000000e+03]
         [  1.00000000e+04]]
    """
    norms = np.asarray(samples_norm)
    if len(norms.shape) != 2:
        raise ValueError()
    ndims = norms.shape[1]
    if not hasattr(do_log, "__iter__"):
        do_log = ndims * [do_log]
    logs = np.asarray(do_log)
    lims_norm = np.array([limits_norm] * len(logs))
    _lims = []
    for limit, log in zip(limits, logs):
        if log:
            _lims.append(np.log10(limit))
        else:
            _lims.append(limit)
    lims = np.array(_lims)

    slopes = (lims[:, 1] - lims[:, 0]) / (lims_norm[:, 1] - lims_norm[:, 0])
    samples = slopes * (norms - lims_norm[:, 0]) + lims[:, 0]
    samples[:, logs] = pow(10, samples[:, logs])
    return samples
