###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
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
SampleIndex factory methods
"""
from parse import parse

from merlin.common.sample_index import MAX_SAMPLE, SampleIndex
from merlin.utils import cd


def create_hierarchy(
    num_samples,
    bundle_size,
    directory_sizes=None,
    root=".",
    start_sample_id=0,
    start_bundle_id=0,
    address="",
    n_digits=1,
):
    """
    SampleIndex Hierarchy Factory method. Wraps
    create_hierarchy_from_max_sample, which is a max_sample-based API, not a
    numSample-based API like this method.

    :param num_samples: The total number of samples.
    :bundle_size: The max number of samples a bundle file is responsible for.
    :directory_sizes: The number of samples each directory is responsible
        for - a list, one value for each level in the directory hierarchy.
    :root: The root path of this index. Defaults to ".".
    :start_sample_id: The start of the sample count. Defaults to 0.
    :n_digits: The number of digits to pad the directories with
    """
    if directory_sizes is None:
        directory_sizes = []
    return create_hierarchy_from_max_sample(
        num_samples + start_sample_id,
        bundle_size,
        directory_sizes=directory_sizes,
        root=root,
        start_bundle_id=start_bundle_id,
        min_sample=start_sample_id,
        address=address,
        n_digits=n_digits,
    )


def create_hierarchy_from_max_sample(
    max_sample,
    bundle_size,
    directory_sizes=None,
    root=".",
    start_bundle_id=0,
    min_sample=0,
    address="",
    n_digits=1,
):
    """"
    Construct the SampleIndex based off the total number of samples and the
    chunking size at each depth in the hierarchy.

    This method will add new SampleIndex objects as this SampleIndex's
    children if directory_sizes is not the empty set.

    :param max_sample: The max Sample ID this hierarchy is responsible for.
    :bundle_size: The max number of samples a bundle file is responsible for.
    :directory_sizes: The number of samples each directory is responsible
        for - a list, one value for each level in the directory hierarchy.
    :bundle_id: The current bundle_id count.
    :min_sample: The start of the sample count.
    :n_digits: The number of digits to pad the directories with
    """
    if directory_sizes is None:
        directory_sizes = []

    # The dict of children nodes.
    children = {}
    # The child_id. Used for naming directory children.
    child_id = 0
    # The bundle_id - will increment as we add children.
    bundle_id = start_bundle_id
    # Number of samples the child node is responsible for.
    num_samples_per_child = directory_sizes[0] if directory_sizes else bundle_size

    if address:
        address_prefix = address + "."
    else:
        address_prefix = address

    for i in range(min_sample, max_sample, num_samples_per_child):
        child_min_sample_id = i
        child_max_sample_id = min(i + num_samples_per_child, max_sample)

        child_dir = f"{child_id}".zfill(n_digits)
        child_address = address_prefix + child_dir

        if directory_sizes:
            # Append an SampleIndex sub-hierarchy child.
            children[child_address] = create_hierarchy_from_max_sample(
                child_max_sample_id,
                bundle_size,
                directory_sizes[1:],
                root=child_dir,
                min_sample=child_min_sample_id,
                start_bundle_id=bundle_id,
                address=child_address,
                n_digits=n_digits,
            )

            bundle_id += children[child_address].num_bundles
        else:
            # Append a bundle file child.
            children[child_address] = SampleIndex(
                child_min_sample_id,
                child_max_sample_id,
                {},
                f"samples{child_min_sample_id}-{child_max_sample_id}.ext",
                leafid=bundle_id,
                address=child_address,
            )
            bundle_id += 1
        child_id += 1
    num_bundles = bundle_id - start_bundle_id
    return SampleIndex(
        min_sample, max_sample, children, root, num_bundles=num_bundles, address=address
    )


def read_hierarchy(path):
    """
    TODO
    """
    children = {}
    min_sample = MAX_SAMPLE
    max_sample = -MAX_SAMPLE
    num_bundles = 0
    with cd(path):
        with open("sample_index.txt", "r") as _file:
            token = _file.readline()
            while token:
                parsed_token = parse(
                    "{type}:{ID}\tname:{name}\tsamples:[{min:d},{max:d})\n", token
                )
                if parsed_token["type"] == "DIR":
                    subhierarchy = read_hierarchy(parsed_token["name"])
                    subhierarchy.address = parsed_token["ID"]
                    num_bundles += subhierarchy.num_bundles
                    children[parsed_token["ID"]] = subhierarchy
                if parsed_token["type"] == "BUNDLE":
                    children[parsed_token["ID"]] = SampleIndex(
                        parsed_token["min"],
                        parsed_token["max"],
                        {},
                        parsed_token["name"],
                        address=parsed_token["ID"],
                    )
                    num_bundles += 1
                min_sample = min(min_sample, parsed_token["min"])
                max_sample = max(max_sample, parsed_token["max"])
                token = _file.readline()
    top_index = SampleIndex(
        min_sample, max_sample, children, path, leafid=-1, num_bundles=num_bundles
    )
    return top_index
