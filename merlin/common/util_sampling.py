##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Utility functions for sampling.
"""
from typing import List, Tuple, Union

import numpy as np


# TODO should we move this to merlin-spellbook?
def scale_samples(
    samples_norm: np.ndarray,
    limits: List[Tuple[int, int]],
    limits_norm: Tuple[int, int] = (0, 1),
    do_log: Union[bool, List[bool]] = False,
) -> np.ndarray:
    """
    Scale samples to new limits, either logarithmically or linearly.

    This function transforms normalized samples to specified limits,
    allowing for both linear and logarithmic scaling based on the
    provided parameters.

    Args:
        samples_norm: The normalized samples to scale, with dimensions
            (nsamples, ndims).
        limits: A list of (min, max) tuples for the various dimensions.
            The length of the list must match the number of dimensions (ndims).
        limits_norm: The (min, max) values from which `samples_norm` were
            derived. Defaults to (0, 1).
        do_log: Indicates whether to apply log10 scaling to each dimension.
            This can be a single boolean or a list of length ndims. Defaults
            to a list of `ndims` containing `False`.

    Returns:
        The scaled samples, with the same shape as `samples_norm`.

    Raises:
        ValueError: If `samples_norm` does not have two dimensions.

    Notes:
        - The function follows the sklearn convention, requiring
          samples to be provided as an (nsamples, ndims) array.
        - To transform 1-D arrays, reshape them accordingly:
            ```python
            >>> samples = samples.reshape((-1, 1))  # ndims = 1
            >>> samples = samples.reshape((1, -1))  # nsamples = 1
            ```

    Example:
        ```python
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
        ```
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
