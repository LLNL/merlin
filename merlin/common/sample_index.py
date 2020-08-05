###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.3.
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
The merlin sample_index module, which contains the SampleIndex class.
"""

import logging
import os
from contextlib import suppress


LOG = logging.getLogger(__name__)

MAX_SAMPLE = 99999999999999999999999999999999999


def new_dir(path):
    """Create a new directory at the given path if it does not exist."""
    with suppress(OSError):
        os.makedirs(path)


def uniform_directories(num_samples=MAX_SAMPLE, bundle_size=1, level_max_dirs=100):
    """Create a directory hierarchy uniformly stepping up directory sizes."""
    directory_sizes = [bundle_size]
    while directory_sizes[0] < num_samples:
        directory_sizes.insert(0, directory_sizes[0] * level_max_dirs)
    # We've gone over the total number of samples, remove the first entry
    del directory_sizes[0]
    return directory_sizes


class SampleIndex:
    """
    A SampleIndex is the insitu representation of a directory hierarchy where
    bundles of samples result files will be stored. Factory methods to produce
    full hierarchies are provided, as well as to read an index from one
    previously stored on disk.
    """

    # Class variable to indicate depth (mostly used for pretty printing).
    depth = -1

    def __init__(
        self, minid, maxid, children, name, leafid=-1, num_bundles=0, address=""
    ):
        """ The constructor."""

        # The direct children of this node, generally also of type SampleIndex.
        # A dictionary keyed by the childrens' full addresses.
        if not isinstance(children, dict):
            LOG.error("SampleIndex children must be of type dict")
            raise TypeError

        self.children = children
        self.name = name  # The name of this node.
        self.address = str(address)  # The address of this node

        # @NOTE: The following are only valid if no insertions,splits, or
        # joins have taken place since the last renumber() call, or this index
        # was constructed with apriori global knowledge of its contents.

        # The minimum global sample ID of any of the children of this node.
        self.min = minid

        # The maximum global sample ID of any of the children of this node.
        self.max = maxid

        # The total number of bundles in this index.
        self.num_bundles = num_bundles

        # The unique leaf ID of this node
        self.leafid = leafid

    @property
    def is_leaf(self):
        """Returns whether this is a leaf in the graph"""
        return len(self.children) == 0

    @property
    def is_directory(self):
        """Returns whether this is a directory (not a leaf) in the graph"""
        return len(self.children) > 0

    @property
    def is_parent_of_leaf(self):
        """Returns whether this is the direct parent of a leaf in the graph"""
        if not self.is_directory:
            return False
        for child_val in self.children.values():
            if child_val.is_leaf:
                return True
        return False

    @property
    def is_grandparent_of_leaf(self):
        """Returns whether this is the parent of a parent of a leaf in the graph"""
        if not self.is_directory:
            return False
        for child_val in self.children.values():
            if child_val.is_parent_of_leaf:
                return True
        return False

    @property
    def is_great_grandparent_of_leaf(self):
        """Returns whether this is the parent of a parent of a leaf in the graph"""
        if not self.is_directory:
            return False
        for child_val in self.children.values():
            if child_val.is_grandparent_of_leaf:
                return True
        return False

    def traverse(
        self, path=None, conditional=lambda c: True, bottom_up=True, top_level=True
    ):
        """
        Yield the full path and associated node for each node that meets the
        conditional
        param:path: The path to this node.
        param:conditional: A lambda that returns a boolean, takes a
            SampleIndex as its only argument.

        param:bottom_up: If True, yield leaves of the tree first. Otherwise,
            yield top level nodes first.
        param:top_level: used to allow filtering of yielded values based off
            the conditional. Should only be set to False internally for the
            recursive calls.
        """
        if path is None:
            path = self.name

        if not bottom_up and conditional(self):
            yield path, self

        for child_val in self.children.values():
            child_path = os.path.join(path, child_val.name)
            for node in child_val.traverse(
                child_path, conditional, bottom_up=bottom_up, top_level=False
            ):
                if node != "SKIP ME":
                    yield node

        if bottom_up and conditional(self):
            yield path, self

        # Always yield something, unless this is the end of the recursion.
        if not top_level:
            yield "SKIP ME"

    def traverse_all(self, bottom_up=True):
        """
        Returns a generator that will traverse all nodes in the SampleIndex.
        """
        return self.traverse(
            path=self.name, conditional=lambda c: True, bottom_up=bottom_up
        )

    def traverse_bundles(self):
        """
        Returns a generator that will traverse all Bundles (leaves) in the
        SampleIndex.
        """
        return self.traverse(path=self.name, conditional=lambda c: c.is_leaf)

    def traverse_directories(self, bottom_up=False):
        """
        Returns a generator that will traverse all Directories in the
        SampleIndex.
        """
        return self.traverse(
            path=self.name, conditional=lambda c: c.is_directory, bottom_up=bottom_up
        )

    @staticmethod
    def check_valid_addresses_for_insertion(full_address, sub_tree):
        """
        TODO
        """
        for _, node in sub_tree.traverse_all():
            if node.address[0 : len(full_address)] != full_address:
                raise TypeError

    def __getitem__(self, full_address):
        if full_address == self.address:
            return self
        for child_val in list(self.children.values()):
            if full_address[0 : len(child_val.address)] == child_val.address:
                return child_val[full_address]
        raise KeyError

    def __setitem__(self, full_address, sub_tree):
        if full_address == self.address:
            # This should never happen.
            raise KeyError

        delete_me = None
        for child_val in list(self.children.values()):
            if full_address[0 : len(child_val.address)] == child_val.address:
                if child_val.address == full_address:
                    delete_me = full_address
                    break

                child_val[full_address] = sub_tree
                return

        # Replace if we already have something at this address.
        if delete_me is not None:
            self.children.__delitem__(full_address)
            SampleIndex.check_valid_addresses_for_insertion(full_address, sub_tree)
            self.children[full_address] = sub_tree
            return
        raise KeyError

    def write_directory(self, path):
        """Creates a new directory associated with this node in the graph."""
        if self.is_directory:
            new_dir(os.path.join(path, self.name))

    def write_directories(self, path="."):
        """
        Creates the directory tree associated with this node and its children.
        """
        self.write_directory(path)
        for child_val in list(self.children.values()):
            child_val.write_directories(os.path.join(path, self.name))

    def get_path_to_sample(self, sample_id):
        """
        Retrieves the file path to the bundle file with the sample_id of
        interest. Note this only works when the global numbering is known.
        """
        path = self.name
        for child_val in self.children.values():
            if sample_id >= child_val.min and sample_id < child_val.max:
                path = os.path.join(path, child_val.get_path_to_sample(sample_id))
        return path

    def write_single_sample_index_file(self, path):
        """Writes the index file associated with this node."""
        if not self.is_directory:
            return

        fname = os.path.join(path, self.name, "sample_index.txt")
        with open(fname, "w") as _file:
            for child_val in self.children.values():
                if child_val.is_leaf:
                    _file.write(
                        f"BUNDLE:{child_val.address}\tname:{child_val.name}\tSAMPLES:[{child_val.min}, {child_val.max})\n"
                    )
                else:
                    _file.write(
                        f"DIR:{child_val.address}\tname:{child_val.name}\tSAMPLES:[{child_val.min}, {child_val.max})\n"
                    )
        return fname

    def write_multiple_sample_index_files(self, path="."):
        """
        Write index files that couple with location in directory hierarchy,
        contain necessary info to create a new index.
        """
        filepath = self.write_single_sample_index_file(path)
        filepaths = []
        if filepath is not None:
            filepaths.append(filepath)
        for child_val in self.children.values():
            filepaths += child_val.write_multiple_sample_index_files(
                os.path.join(path, self.name)
            )
        return filepaths

    def make_directory_string(self, delimiter=" ", just_leaf_directories=True):
        """
        Make a string that is a delimited list of the directories in the
        index.

        :param delimiter: the characters used to separate the directories
        :param just_leaf_directories: A boolean on whether just to return the
            leaf (bottom) directories
        :returns: A string representation of the directories
        e.g.
        "0/0 0/1 0/2 1/0 1/1 1/2"

        """
        if just_leaf_directories:
            return delimiter.join(
                [
                    path
                    for path, node in self.traverse_directories()
                    if node.is_parent_of_leaf
                ]
            )
        return delimiter.join([path for path, _ in self.traverse_directories()])

    def __str__(self):
        """String representation."""
        SampleIndex.depth = SampleIndex.depth + 1
        if self.is_leaf:
            result = (
                ("   " * SampleIndex.depth)
                + f"{self.address}: BUNDLE {self.leafid} MIN {self.min} MAX {self.max}\n"
            )
        else:
            result = (
                ("   " * SampleIndex.depth)
                + f"{self.address}: DIRECTORY MIN {self.min} MAX {self.max} NUM_BUNDLES {self.num_bundles}\n"
            )
            for child_val in self.children.values():
                result += str(child_val)
        SampleIndex.depth = SampleIndex.depth - 1
        return result
