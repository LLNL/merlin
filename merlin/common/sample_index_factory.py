##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module houses [`SampleIndex`][common.sample_index.SampleIndex] factory methods.
"""
from typing import List

from parse import parse

from merlin.common.sample_index import MAX_SAMPLE, SampleIndex
from merlin.utils import cd


# These pylint errors I've disabled are for "too many arguments"
# and "too many local variables". I think the functions are still clear
# pylint: disable=R0913,R0914


def create_hierarchy(
    num_samples: int,
    bundle_size: int,
    directory_sizes: List[int] = None,
    root: str = ".",
    start_sample_id: int = 0,
    start_bundle_id: int = 0,
    address: str = "",
    n_digits: int = 1,
) -> SampleIndex:
    """
    Factory method to create a [`SampleIndex`][common.sample_index.SampleIndex]
    hierarchy based on the number of samples.

    This method wraps the
    [`create_hierarchy_from_max_sample`][common.sample_index_factory.create_hierarchy_from_max_sample]
    function, which operates on a maximum sample basis rather than a total
    sample count.

    Args:
        num_samples (int): The total number of samples.
        bundle_size (int): The maximum number of samples a bundle file can handle.
        directory_sizes (List[int]): A list specifying the number of samples each directory
            is responsible for.
        root (str): The root path of the index.
        start_sample_id (int): The starting sample ID.
        start_bundle_id (int): The starting bundle ID.
        address (str): An optional address prefix for the hierarchy.
        n_digits (int): The number of digits to pad the directory names.

    Returns:
        (common.sample_index.SampleIndex): The root [`SampleIndex`][common.sample_index.SampleIndex]
            object representing the hierarchy.
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
    max_sample: int,
    bundle_size: int,
    directory_sizes: List[int] = None,
    root: str = ".",
    start_bundle_id: int = 0,
    min_sample: int = 0,
    address: str = "",
    n_digits: int = 1,
) -> SampleIndex:
    """
    Constructs a [`SampleIndex`][common.sample_index.SampleIndex] hierarchy based on
    the maximum sample ID and chunking size at each depth.

    This method adds new [`SampleIndex`][common.sample_index.SampleIndex] objects as
    children if `directory_sizes` is provided.

    Args:
        max_sample: The maximum Sample ID this hierarchy is responsible for.
        bundle_size: The maximum number of samples a bundle file can handle.
        directory_sizes: A list specifying the number of samples each directory
            is responsible for.
        root: The root path of this index.
        start_bundle_id: The starting bundle ID.
        min_sample: The starting sample ID.
        address: An optional address prefix for the hierarchy.
        n_digits: The number of digits to pad the directory names.

    Returns:
        (common.sample_index.SampleIndex): The root
            [`SampleIndex`][common.sample_index.SampleIndex] object representing
            the constructed hierarchy.
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
    return SampleIndex(min_sample, max_sample, children, root, num_bundles=num_bundles, address=address)


def read_hierarchy(path: str) -> SampleIndex:
    """
    Reads a hierarchy from a specified path and constructs a
    [`SampleIndex`][common.sample_index.SampleIndex].

    This function reads a file named "sample_index.txt" in the given path,
    parsing its contents to create a hierarchical structure of
    [`SampleIndex`][common.sample_index.SampleIndex] objects based on the
    information found in the file.

    Args:
        path: The directory path where the sample index file is located.

    Returns:
        (common.sample_index.SampleIndex): The root
            [`SampleIndex`][common.sample_index.SampleIndex] object representing
            the hierarchy read from the file.
    """
    children = {}
    min_sample = MAX_SAMPLE
    max_sample = -MAX_SAMPLE
    num_bundles = 0
    with cd(path):
        with open("sample_index.txt", "r") as _file:
            token = _file.readline()
            while token:
                parsed_token = parse("{type}:{ID}\tname:{name}\tsamples:[{min:d},{max:d})\n", token)
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
    top_index = SampleIndex(min_sample, max_sample, children, path, leafid=-1, num_bundles=num_bundles)
    return top_index
