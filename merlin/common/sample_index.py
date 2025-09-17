##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains the logic for managing the sample hierarchy, including
the implementation of the [`SampleIndex`][common.sample_index.SampleIndex] class.
"""

import logging
import os
from contextlib import suppress
from typing import Callable, Dict, Generator, List, Tuple


LOG = logging.getLogger(__name__)

MAX_SAMPLE = 99999999999999999999999999999999999


def new_dir(path: str):
    """
    Create a new directory at the specified path if it does not already exist.

    This function attempts to create the directory and suppresses any
    OSError that may occur if the directory already exists.

    Args:
        path: The path where the new directory should be created.
    """
    with suppress(OSError):
        os.makedirs(path)


def uniform_directories(num_samples: int = MAX_SAMPLE, bundle_size: int = 1, level_max_dirs: int = 100) -> List[int]:
    """
    Create a directory hierarchy with uniformly increasing directory sizes.

    This function generates a list of directory sizes, starting from
    the specified `bundle_size` and increasing by a factor of
    `level_max_dirs` until the total number of samples is reached.

    Args:
        num_samples: The total number of samples to consider.
        bundle_size: The initial size of each bundle.
        level_max_dirs: The factor by which to increase directory sizes at each level.

    Returns:
        A list of integers representing the sizes of directories in the hierarchy.
    """
    directory_sizes = [bundle_size]
    while directory_sizes[0] < num_samples:
        directory_sizes.insert(0, directory_sizes[0] * level_max_dirs)
    # We've gone over the total number of samples, remove the first entry
    del directory_sizes[0]
    return directory_sizes


class SampleIndex:
    """
    Represents a hierarchical structure for managing bundles of sample files.

    A [`SampleIndex`][common.sample_index.SampleIndex] serves as an in-situ
    representation of a directory hierarchy where bundles of sample result files
    are stored. This class provides factory methods to create complete hierarchies
    and to read an index from a previously stored file on disk.

    Attributes:
        address (str): The full address of this node.
        children (Dict[str, SampleIndex]): A dictionary containing the direct children of this node, which are also
            of type SampleIndex.
        depth (int): (class attribute) A class variable indicating the current depth in the
            hierarchy, primarily used for pretty printing.
        is_leaf (bool): Returns whether this node is a leaf in the hierarchy.
        is_directory (bool): Returns whether this node is a directory (not a leaf).
        is_parent_of_leaf (bool): Returns whether this node is the direct parent of a leaf.
        is_grandparent_of_leaf (bool): Returns whether this node is the parent of a parent of a leaf.
        is_great_grandparent_of_leaf (bool): Returns whether this node is the parent of a grandparent of a leaf.
        leafid (int): The unique leaf ID of this node.
        max (int): The maximum global sample ID of any child node.
        min (int): The minimum global sample ID of any child node.
        name (str): The name of this node in the hierarchy.
        num_bundles (int): The total number of bundles in this index.

    Methods:
        traverse: Yields the full path and associated node for each node meeting a specified condition.
        traverse_all: Returns a generator that traverses all nodes in the
            [`SampleIndex`][common.sample_index.SampleIndex].
        traverse_bundles: Returns a generator that traverses all bundles (leaves) in the
            [`SampleIndex`][common.sample_index.SampleIndex].
        traverse_directories: Returns a generator that traverses all directories in the
            [`SampleIndex`][common.sample_index.SampleIndex].
        check_valid_addresses_for_insertion: Validates addresses for insertion into the hierarchy.
        __getitem__: Retrieves a child node by its full address.
        __setitem__: Sets a child node at a specified full address.
        write_directory: Creates a new directory associated with this node.
        write_directories: Recursively writes directories for this node and its children.
        get_path_to_sample: Retrieves the file path to the bundle file with a specified sample ID.
        write_single_sample_index_file: Writes a single sample index file for this node.
        write_multiple_sample_index_files: Writes multiple sample index files for this node and its children.
        make_directory_string: Creates a delimited string representation of the directories in the index.
        __str__: Returns a string representation of the [`SampleIndex`][common.sample_index.SampleIndex],
            including its children.
    """

    # Class variable to indicate depth (mostly used for pretty printing).
    depth: int = -1

    def __init__(
        self,
        minid: int,
        maxid: int,
        children: Dict[str, "SampleIndex"],
        name: str,
        leafid: int = -1,
        num_bundles: int = 0,
        address: str = "",
    ):  # pylint: disable=R0913
        """
        Initializes a new instance of the `SampleIndex` class.

        Args:
            minid: The minimum global sample ID of any child node.
            maxid: The maximum global sample ID of any child node.
            children: A dictionary containing the direct children of this node,
                where the keys are the children's full addresses and the values are `SampleIndex` instances.
            name: The name of this node in the hierarchy.
            leafid: The unique leaf ID of this node.
            num_bundles: The total number of bundles in this index.
            address: The full address of this node.

        Raises:
            TypeError: If `children` is not of type `dict`.
        """

        # The direct children of this node, generally also of type SampleIndex.
        # A dictionary keyed by the childrens' full addresses.
        if not isinstance(children, dict):
            LOG.error("SampleIndex children must be of type dict")
            raise TypeError

        self.children: Dict[str, "SampleIndex"] = children
        self.name: str = name  # The name of this node.
        self.address: str = str(address)  # The address of this node

        # @NOTE: The following are only valid if no insertions,splits, or
        # joins have taken place since the last renumber() call, or this index
        # was constructed with apriori global knowledge of its contents.

        # The minimum global sample ID of any of the children of this node.
        self.min: int = minid

        # The maximum global sample ID of any of the children of this node.
        self.max: int = maxid

        # The total number of bundles in this index.
        self.num_bundles: int = num_bundles

        # The unique leaf ID of this node
        self.leafid: int = leafid

    @property
    def is_leaf(self) -> bool:
        """
        Indicates whether this node is a leaf in the hierarchy.

        A leaf node is defined as a node that has no children. This property
        returns True if the node has no children, and False otherwise.
        """
        return len(self.children) == 0

    @property
    def is_directory(self) -> bool:
        """
        Indicates whether this node is a directory (not a leaf).

        A directory node is defined as a node that has one or more children.
        This property returns True if the node has children, and False if it
        is a leaf node.
        """
        return len(self.children) > 0

    @property
    def is_parent_of_leaf(self) -> bool:
        """
        Indicates whether this node is the direct parent of a leaf.

        This property checks if the current node is a directory and if any of
        its children are leaf nodes. It returns True if at least one child is
        a leaf, and False otherwise.
        """
        if not self.is_directory:
            return False
        for child_val in self.children.values():
            if child_val.is_leaf:
                return True
        return False

    @property
    def is_grandparent_of_leaf(self) -> bool:
        """
        Indicates whether this node is the parent of a parent of a leaf.

        This property checks if the current node is a directory and if any of
        its children are parents of leaf nodes. It returns True if at least
        one child is a parent of a leaf, and False otherwise.
        """
        if not self.is_directory:
            return False
        for child_val in self.children.values():
            if child_val.is_parent_of_leaf:
                return True
        return False

    @property
    def is_great_grandparent_of_leaf(self) -> bool:
        """
        Indicates whether this node is the parent of a grandparent of a leaf.

        This property checks if the current node is a directory and if any of
        its children are grandparents of leaf nodes. It returns True if at
        least one child is a grandparent of a leaf, and False otherwise.
        """
        if not self.is_directory:
            return False
        for child_val in self.children.values():
            if child_val.is_grandparent_of_leaf:
                return True
        return False

    def traverse(
        self,
        path: str = None,
        conditional: Callable = lambda c: True,
        bottom_up: bool = True,
        top_level: bool = True,
    ) -> Generator[Tuple[str, "SampleIndex"], None, None]:
        """
        Traverse the tree structure and yield the full path and associated node
        for each node that meets the specified conditional criteria.

        This method allows for flexible traversal of the tree, either from
        the top down or bottom up, depending on the `bottom_up` parameter.
        Nodes are yielded based on whether they satisfy the provided
        conditional function.

        Notes:
            - The method uses a "SKIP ME" placeholder to manage the
            recursion flow, ensuring that the traversal can skip
            non-qualifying nodes without breaking the iteration.

        Args:
            path: The current path to this node. If None, it defaults to
                the name of the node.
            conditional: A function that takes a
                [`SampleIndex`][common.sample_index.SampleIndex] instance as its
                only argument and returns a boolean. It determines whether a
                node should be yielded.
            bottom_up: If True, yields leaf nodes first, moving upwards through
                the tree. If False, yields top-level nodes first.
            top_level: A flag used internally to control filtering of yielded
                values based on the conditional. Should only be set to False
                for recursive calls.

        Yields:
            A tuple containing the full path to the node and the
                associated [`SampleIndex`][common.sample_index.SampleIndex] node
                that meets the conditional criteria.
        """
        if path is None:
            path = self.name

        if not bottom_up and conditional(self):
            yield path, self

        for child_val in self.children.values():
            child_path = os.path.join(path, child_val.name)
            for node in child_val.traverse(child_path, conditional, bottom_up=bottom_up, top_level=False):
                if node != "SKIP ME":
                    yield node

        if bottom_up and conditional(self):
            yield path, self

        # Always yield something, unless this is the end of the recursion.
        if not top_level:
            yield "SKIP ME"

    def traverse_all(self, bottom_up: bool = True) -> Generator[Tuple[str, "SampleIndex"], None, None]:
        """
        Traverse all nodes in the [`SampleIndex`][common.sample_index.SampleIndex].

        This method returns a generator that yields all nodes in the
        [`SampleIndex`][common.sample_index.SampleIndex], regardless of their type
        (leaf or directory). The traversal order can be controlled by the `bottom_up`
        parameter.

        Notes:
            This method calls the [`traverse`][common.sample_index.SampleIndex.traverse]
            method with a conditional that always returns True, ensuring all nodes are
            included.

        Args:
            bottom_up: If True, yields leaf nodes first, moving upwards
                through the tree. If False, yields top-level nodes first.

        Returns:
            A tuple containing the full path to each node and the associated
                [`SampleIndex`][common.sample_index.SampleIndex] node.
        """
        return self.traverse(path=self.name, conditional=lambda c: True, bottom_up=bottom_up)

    def traverse_bundles(self) -> Generator[Tuple[str, "SampleIndex"], None, None]:
        """
        Traverse all Bundles (leaf nodes) in the [`SampleIndex`][common.sample_index.SampleIndex].

        This method returns a generator that yields only the leaf nodes
        (Bundles) in the [`SampleIndex`][common.sample_index.SampleIndex].
        It filters the nodes based on their type, ensuring that only leaves are returned.

        Notes:
            This method calls the [`traverse`][common.sample_index.SampleIndex.traverse]
            method with a conditional that checks if a node is a leaf, ensuring only Bundles
            are yielded.

        Returns:
            A tuple containing the full path to each Bundle and the associated
                [`SampleIndex`][common.sample_index.SampleIndex] node.
        """
        return self.traverse(path=self.name, conditional=lambda c: c.is_leaf)

    def traverse_directories(self, bottom_up: bool = False) -> Generator[Tuple[str, "SampleIndex"], None, None]:
        """
        Traverse all Directories in the [`SampleIndex`][common.sample_index.SampleIndex].

        This method returns a generator that yields all directory nodes
        in the [`SampleIndex`][common.sample_index.SampleIndex]. The
        traversal order can be controlled by the `bottom_up` parameter.

        Notes:
            This method calls the [`traverse`][common.sample_index.SampleIndex.traverse]
            method with a conditional that checks if a node is a directory, ensuring only
            directories are yielded.

        Args:
            bottom_up: If True, yields leaf directories first, moving
                upwards through the tree. If False, yields top-level
                directories first.

        Yields:
            A tuple containing the full path to each Directory and the
                associated [`SampleIndex`][common.sample_index.SampleIndex]
                node.
        """
        return self.traverse(path=self.name, conditional=lambda c: c.is_directory, bottom_up=bottom_up)

    @staticmethod
    def check_valid_addresses_for_insertion(full_address: str, sub_tree: "SampleIndex"):
        """
        Check if the provided address is valid for insertion into the subtree.

        This method traverses all nodes in the given subtree and verifies
        that no existing node's address conflicts with the specified
        `full_address`. If any node's address starts with the `full_address`,
        a TypeError is raised, indicating that the insertion would create
        an invalid state.

        Args:
            full_address: The full address to be checked for validity before
                insertion.
            sub_tree: The subtree in which the address will be checked.

        Raises:
            TypeError: If any node in the subtree has an address that
                conflicts with the `full_address`.
        """
        for _, node in sub_tree.traverse_all():
            if node.address[0 : len(full_address)] != full_address:
                raise TypeError

    def __getitem__(self, full_address: str) -> "SampleIndex":
        """
        Retrieve the subtree associated with the given full address.

        This method allows for accessing nodes in the
        [`SampleIndex`][common.sample_index.SampleIndex] using
        the full address. If the full address matches the current node's
        address, the node itself is returned. Otherwise, it recursively
        searches through the children to find the corresponding node.

        Args:
            full_address: The full address of the node to retrieve.

        Returns:
            The node associated with the specified full address.

        Raises:
            KeyError: If no node with the specified full address exists
                in the [`SampleIndex`][common.sample_index.SampleIndex].
        """
        if full_address == self.address:
            return self
        for child_val in list(self.children.values()):
            if full_address[0 : len(child_val.address)] == child_val.address:
                return child_val[full_address]
        raise KeyError

    def __setitem__(self, full_address: str, sub_tree: "SampleIndex"):
        """
        Set or replace the subtree associated with the given full address.

        This method allows for inserting or updating a node in the
        [`SampleIndex`][common.sample_index.SampleIndex]. If the full
        address matches the current node's address, a KeyError is raised
        to prevent self-assignment. The method searches through the children
        to find the appropriate location for insertion. If a node already
        exists at the specified address, it will be replaced after validating
        the insertion.

        Args:
            full_address: The full address of the node to set or replace.
            sub_tree: The subtree to insert or replace at the specified
                address.

        Raises:
            KeyError: If the full address matches the current node's
                address or if no matching child node is found for
                insertion.
        """
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
            SampleIndex.check_valid_addresses_for_insertion(full_address, sub_tree)
            self.children.__delitem__(full_address)
            self.children[full_address] = sub_tree
            return
        raise KeyError

    def write_directory(self, path: str):
        """
        Create a new directory associated with this node in the graph.

        This method checks if the current node is a directory and, if so,
        creates a new directory at the specified path using the node's
        name. The directory is created using the
        [`new_dir`][common.sample_index.new_dir] function.

        Args:
            path: The base path where the new directory will be created.
        """
        if self.is_directory:
            new_dir(os.path.join(path, self.name))

    def write_directories(self, path: str = "."):
        """
        Create the directory tree associated with this node and its children.

        This method initiates the creation of the directory structure
        starting from the current node. It first creates the directory for
        the current node and then recursively creates directories for all
        child nodes. The base path can be specified, and the directories
        will be created relative to this path.

        Args:
            path: The base path where the directory tree will be created.
                Defaults to the current directory ("."), meaning the
                directories will be created in the current working
                directory.
        """
        self.write_directory(path)
        for child_val in list(self.children.values()):
            child_val.write_directories(os.path.join(path, self.name))

    def get_path_to_sample(self, sample_id: int) -> str:
        """
        Retrieve the file path to the bundle file associated with the specified
        sample ID.

        This method constructs the path to the bundle file by traversing the
        directory structure represented by the current node and its children.
        It checks each child node to determine if the provided `sample_id`
        falls within the range defined by the child's `min` and `max` attributes.
        If a matching child is found, the method recursively calls itself to
        build the complete path.

        Notes:
            - This method assumes that the global numbering system is known
            and that the `min` and `max` attributes of child nodes are
            correctly defined.
            - If no matching child is found, the method will return the
            current node's name as the base path.

        Args:
            sample_id: The identifier of the sample for which the file path is
                to be retrieved. This ID must correspond to a sample within the
                known global numbering system.

        Returns:
            The constructed file path to the bundle file associated
                with the specified `sample_id`.
        """
        path = self.name
        for child_val in self.children.values():
            if child_val.min <= sample_id < child_val.max:
                path = os.path.join(path, child_val.get_path_to_sample(sample_id))
        return path

    def write_single_sample_index_file(self, path: str) -> str:
        """
        Write the index file associated with this node.

        This method creates an index file named "sample_index.txt" in the
        specified directory path. The index file contains information about
        the child nodes of the current node. For each child, it records
        whether the child is a leaf or a directory, along with its address,
        name, and the range of sample IDs it covers.

        Notes:
            - This method will only execute if the current node is identified
            as a directory
            ([`self.is_directory`][common.sample_index.SampleIndex.is_directory]).
            - The index file format includes lines for each child in the
            following format:
                - For leaf nodes: `BUNDLE:<address>\tname:<name>\tSAMPLES:[<min>, <max>)`
                - For directory nodes: `DIR:<address>\tname:<name>\tSAMPLES:[<min>, <max>)`

        Args:
            path: The base path where the index file will be created.

        Returns:
            The full path to the created index file if the current node is
                a directory; otherwise, returns None.
        """
        if not self.is_directory:
            return None

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

    def write_multiple_sample_index_files(self, path: str = ".") -> List[str]:
        """
        Write index files that correspond to the location in the directory hierarchy.

        This method generates index files for the current node and all its
        children, allowing for a structured representation of the sample
        indices in the directory hierarchy. It first writes a single index
        file for the current node and then recursively writes index files
        for each child node.

        Args:
            path: The base path where the index files will be created.
                Defaults to the current directory ("."), meaning the index
                files will be created in the current working directory.

        Returns:
            A list of file paths to the created index files.
        """
        filepath = self.write_single_sample_index_file(path)
        filepaths = []
        if filepath is not None:
            filepaths.append(filepath)
        for child_val in self.children.values():
            filepaths += child_val.write_multiple_sample_index_files(os.path.join(path, self.name))
        return filepaths

    def make_directory_string(self, delimiter: str = " ", just_leaf_directories: bool = True) -> str:
        """
        Create a delimited string representation of the directories in the index.

        This method generates a string that lists the directories in the
        current index, separated by the specified delimiter. The user can
        choose to include only the leaf directories or all directories.

        Notes:
            - The method utilizes the
            [`traverse_directories`][common.sample_index.SampleIndex.traverse_directories]
            function to retrieve the directory paths and their corresponding nodes.

        Args:
            delimiter: The characters used to separate the directories in the
                resulting string.
            just_leaf_directories: If True, only leaf directories (the bottom-level
                directories) will be included in the output. If False, all directories
                will be included.

        Returns:
            A string representation of the directories, formatted as a delimited
                list. For example: "0/0 0/1 0/2 1/0 1/1 1/2".
        """
        # fmt: off
        if just_leaf_directories:
            return delimiter.join(
                [
                    path for path, node in self.traverse_directories() if node.is_parent_of_leaf
                ]
            )
        # fmt: on
        return delimiter.join([path for path, _ in self.traverse_directories()])

    def __str__(self) -> str:
        """
        Return a string representation of the
        [`SampleIndex`][common.sample_index.SampleIndex] object.

        This method provides a formatted string that represents the current
        node in the sample index, including its address, type (BUNDLE or
        DIRECTORY), and relevant attributes such as minimum and maximum
        sample IDs. If the node is a directory, it recursively includes
        the string representations of its child nodes.

        Notes:
            - The method uses a class variable `depth` to manage indentation
            levels for nested directories, enhancing readability.
            - The output format varies depending on whether the node is a
            leaf or a directory.

        Returns:
            A formatted string representation of the
                [`SampleIndex`][common.sample_index.SampleIndex] object,
                including its children if applicable.
        """
        SampleIndex.depth = SampleIndex.depth + 1
        if self.is_leaf:
            result = ("   " * SampleIndex.depth) + f"{self.address}: BUNDLE {self.leafid} MIN {self.min} MAX {self.max}\n"
        else:
            result = (
                "   " * SampleIndex.depth
            ) + f"{self.address}: DIRECTORY MIN {self.min} MAX {self.max} NUM_BUNDLES {self.num_bundles}\n"
            for child_val in self.children.values():
                result += str(child_val)
        SampleIndex.depth = SampleIndex.depth - 1
        return result
