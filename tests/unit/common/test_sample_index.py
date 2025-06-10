##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `sample_index.py` and `sample_index_factory.py` files.
"""

import os

import pytest

from merlin.common.sample_index import SampleIndex, new_dir, uniform_directories
from merlin.common.sample_index_factory import create_hierarchy, read_hierarchy


def test_uniform_directories():
    """
    Test the `uniform_directories` function with different inputs.
    """
    # Create the tests and the expected outputs
    tests = [
        # SMALL SAMPLE SIZE
        (10, 1, 100),  # Bundle size of 1 and max dir level of 100 is default
        (10, 1, 2),
        (10, 2, 100),
        (10, 2, 2),
        # MEDIUM SAMPLE SIZE
        (10000, 1, 100),  # Bundle size of 1 and max dir level of 100 is default
        (10000, 1, 5),
        (10000, 5, 100),
        (10000, 5, 10),
        # LARGE SAMPLE SIZE
        (1000000000, 1, 100),  # Bundle size of 1 and max dir level of 100 is default
        (1000000000, 1, 5),
        (1000000000, 5, 100),
        (1000000000, 5, 10),
    ]
    expected_outputs = [
        # SMALL SAMPLE SIZE
        [1],
        [8, 4, 2, 1],
        [2],
        [8, 4, 2],
        # MEDIUM SAMPLE SIZE
        [100, 1],
        [3125, 625, 125, 25, 5, 1],
        [500, 5],
        [5000, 500, 50, 5],
        # LARGE SAMPLE SIZE
        [100000000, 1000000, 10000, 100, 1],
        [244140625, 48828125, 9765625, 1953125, 390625, 78125, 15625, 3125, 625, 125, 25, 5, 1],
        [500000000, 5000000, 50000, 500, 5],
        [500000000, 50000000, 5000000, 500000, 50000, 5000, 500, 50, 5],
    ]

    # Run the tests and compare outputs
    for i, test in enumerate(tests):
        actual = uniform_directories(num_samples=test[0], bundle_size=test[1], level_max_dirs=test[2])
        assert actual == expected_outputs[i]


def test_new_dir(temp_output_dir: str):
    """
    Test the `new_dir` function. This will test a valid path and also raising an OSError during
    creation.

    :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
    """
    # Test basic functionality
    test_path = f"{os.getcwd()}/test_sample_index/test_new_dir"
    new_dir(test_path)
    assert os.path.exists(test_path)

    # Test OSError functionality
    new_dir(test_path)


class TestSampleIndex:
    """
    These tests focus on testing the SampleIndex class used for creating the
    sample hierarchy.

    NOTE to see output of creating any hierarchy, change `write_all_hierarchies` to True.
    The results of each hierarchy will be written to:
    /tmp/`whoami`/pytest/pytest-of-`whoami`/pytest-current/python_{major}.{minor}.{micro}_current/test_sample_index/<name of the test>
    """

    write_all_hierarchies = False

    def get_working_dir(self, test_workspace: str):
        """
        This method is called for every test to get a unique workspace in the temporary
        directory for the test output.

        :param test_workspace: The unique name for this workspace
            (all tests use their unique test name for this value usually)
        """
        return f"{os.getcwd()}/test_sample_index/{test_workspace}"

    def write_hierarchy_for_debug(self, indx: SampleIndex):
        """
        This method is for debugging purposes. It will cause all tests that don't write
        hierarchies to write them so the output can be investigated.

        :param indx: The `SampleIndex` object to write the hierarchy for
        """
        if self.write_all_hierarchies:
            indx.write_directories()
            indx.write_multiple_sample_index_files()

    def test_invalid_children(self):
        """
        This will test that an invalid type for the `children` argument will raise
        an error.
        """
        tests = [
            ["a", "b", "c"],
            True,
            "a b c",
        ]
        for test in tests:
            with pytest.raises(TypeError):
                SampleIndex(0, 10, test, "name")

    def test_is_parent_of_leaf(self, temp_output_dir: str):
        """
        Test the `is_parent_of_leaf` property.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create a hierarchy to test
        working_dir = self.get_working_dir("test_is_parent_of_leaf")
        indx = create_hierarchy(10, 1, [2], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Test to see if parent of leaf is recognized
        assert indx.is_parent_of_leaf is False
        assert indx.children["0"].is_parent_of_leaf is True

        # Test to see if leaf is recognized
        leaf_node = indx.children["0"].children["0.0"]
        assert leaf_node.is_parent_of_leaf is False

    def test_is_grandparent_of_leaf(self, temp_output_dir: str):
        """
        Test the `is_grandparent_of_leaf` property.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create a hierarchy to test
        working_dir = self.get_working_dir("test_is_grandparent_of_leaf")
        indx = create_hierarchy(10, 1, [2], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Test to see if grandparent of leaf is recognized
        assert indx.is_grandparent_of_leaf is True
        assert indx.children["0"].is_grandparent_of_leaf is False

        # Test to see if leaf is recognized
        leaf_node = indx.children["0"].children["0.0"]
        assert leaf_node.is_grandparent_of_leaf is False

    def test_is_great_grandparent_of_leaf(self, temp_output_dir: str):
        """
        Test the `is_great_grandparent_of_leaf` property.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create a hierarchy to test
        working_dir = self.get_working_dir("test_is_great_grandparent_of_leaf")
        indx = create_hierarchy(10, 1, [5, 1], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Test to see if great grandparent of leaf is recognized
        assert indx.is_great_grandparent_of_leaf is True
        assert indx.children["0"].is_great_grandparent_of_leaf is False
        assert indx.children["0"].children["0.0"].is_great_grandparent_of_leaf is False

        # Test to see if leaf is recognized
        leaf_node = indx.children["0"].children["0.0"].children["0.0.0"]
        assert leaf_node.is_great_grandparent_of_leaf is False

    def test_traverse_bundle(self, temp_output_dir: str):
        """
        Test the `traverse_bundle` method to make sure it's just returning leaves.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create a hierarchy to test
        working_dir = self.get_working_dir("test_is_grandparent_of_leaf")
        indx = create_hierarchy(10, 1, [2], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Ensure all nodes in the traversal are leaves
        for _, node in indx.traverse_bundles():
            assert node.is_leaf

    def test_getitem(self, temp_output_dir: str):
        """
        Test the `__getitem__` magic method.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create a hierarchy to test
        working_dir = self.get_working_dir("test_is_grandparent_of_leaf")
        indx = create_hierarchy(10, 1, [2], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Test getting that requesting the root returns itself
        assert indx[""] == indx

        # Test a valid address
        assert indx["0"] == indx.children["0"]

        # Test an invalid address
        with pytest.raises(KeyError):
            indx["10"]

    def test_setitem(self, temp_output_dir: str):
        """
        Test the `__setitem__` magic method.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create a hierarchy to test
        working_dir = self.get_working_dir("test_is_grandparent_of_leaf")
        indx = create_hierarchy(10, 1, [2], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        invalid_indx = SampleIndex(1, 3, {}, "invalid_indx")

        # Ensure that trying to change the root raises an error
        with pytest.raises(KeyError):
            indx[""] = invalid_indx

        # Ensure we can't just add a new subtree to a level
        with pytest.raises(KeyError):
            indx["10"] = invalid_indx

        # Test that invalid subtrees are caught
        with pytest.raises(TypeError):
            indx["0"] = invalid_indx

        # Test a valid set operation
        dummy_indx = SampleIndex(0, 1, {}, "dummy_indx", leafid=0, address="0.0")
        indx["0"]["0.0"] = dummy_indx

    def test_index_file_writing(self, temp_output_dir: str):
        """
        Test the functionality of writing multiple index files.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        working_dir = self.get_working_dir("test_index_file_writing")
        indx = create_hierarchy(1000000000, 10000, [100000000, 10000000, 1000000], root=working_dir)
        indx.write_directories()
        indx.write_multiple_sample_index_files()
        indx2 = read_hierarchy(working_dir)
        assert indx2.get_path_to_sample(123000123) == indx.get_path_to_sample(123000123)

    def test_directory_writing_small(self, temp_output_dir: str):
        """
        Test that writing a small directory functions properly.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create the directory and ensure it has the correct format
        working_dir = self.get_working_dir("test_directory_writing_small/")
        indx = create_hierarchy(2, 1, [1], root=working_dir)
        expected = (
            ": DIRECTORY MIN 0 MAX 2 NUM_BUNDLES 2\n"
            "   0: DIRECTORY MIN 0 MAX 1 NUM_BUNDLES 1\n"
            "      0.0: BUNDLE 0 MIN 0 MAX 1\n"
            "   1: DIRECTORY MIN 1 MAX 2 NUM_BUNDLES 1\n"
            "      1.0: BUNDLE 1 MIN 1 MAX 2\n"
        )
        assert expected == str(indx)

        # Write the directories and ensure the paths are actually written
        indx.write_directories()
        assert os.path.isdir(f"{working_dir}/0")
        assert os.path.isdir(f"{working_dir}/1")
        indx.write_multiple_sample_index_files()

    def test_directory_writing_large(self, temp_output_dir: str):
        """
        Test that writing a large directory functions properly.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        working_dir = self.get_working_dir("test_directory_writing_large")
        indx = create_hierarchy(1000000000, 10000, [100000000, 10000000, 1000000], root=working_dir)
        indx.write_directories()
        path = indx.get_path_to_sample(123000123)
        assert os.path.exists(os.path.dirname(path))
        assert path != working_dir
        path = indx.get_path_to_sample(10000000000)
        assert path == working_dir

    def test_bundle_retrieval(self, temp_output_dir: str):
        """
        Test the functionality to get a bundle of samples when providing a sample id to find.
        This will test a large sample hierarchy to ensure this scales properly.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create the hierarchy
        working_dir = self.get_working_dir("test_bundle_retrieval")
        indx = create_hierarchy(1000000000, 10000, [100000000, 10000000, 1000000], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Test for a small sample id
        expected = f"{working_dir}/0/0/0/samples0-10000.ext"
        result = indx.get_path_to_sample(123)
        assert expected == result

        # Test for a mid size sample id
        expected = f"{working_dir}/0/0/0/samples10000-20000.ext"
        result = indx.get_path_to_sample(10000)
        assert expected == result

        # Test for a large sample id
        expected = f"{working_dir}/1/2/3/samples123000000-123010000.ext"
        result = indx.get_path_to_sample(123000123)
        assert expected == result

    def test_start_sample_id(self, temp_output_dir: str):
        """
        Test creating a hierarchy using a starting sample id.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        working_dir = self.get_working_dir("test_start_sample_id")
        expected = (
            ": DIRECTORY MIN 203 MAX 303 NUM_BUNDLES 10\n"
            "   0: BUNDLE 0 MIN 203 MAX 213\n"
            "   1: BUNDLE 1 MIN 213 MAX 223\n"
            "   2: BUNDLE 2 MIN 223 MAX 233\n"
            "   3: BUNDLE 3 MIN 233 MAX 243\n"
            "   4: BUNDLE 4 MIN 243 MAX 253\n"
            "   5: BUNDLE 5 MIN 253 MAX 263\n"
            "   6: BUNDLE 6 MIN 263 MAX 273\n"
            "   7: BUNDLE 7 MIN 273 MAX 283\n"
            "   8: BUNDLE 8 MIN 283 MAX 293\n"
            "   9: BUNDLE 9 MIN 293 MAX 303\n"
        )
        idx203 = create_hierarchy(100, 10, start_sample_id=203, root=working_dir)
        self.write_hierarchy_for_debug(idx203)

        assert expected == str(idx203)

    def test_make_directory_string(self, temp_output_dir: str):
        """
        Test the `make_directory_string` method of `SampleIndex`. This will check
        both the normal functionality where we just request paths to the leaves and
        also the inverse functionality where we request all paths that are not leaves.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Creating the hierarchy
        working_dir = self.get_working_dir("test_make_directory_string")
        indx = create_hierarchy(20, 1, [20, 5, 1], root=working_dir)
        self.write_hierarchy_for_debug(indx)

        # Testing normal functionality (just leaf directories)
        leaves = indx.make_directory_string()
        expected_leaves_list = [
            f"{working_dir}/0/0/0",
            f"{working_dir}/0/0/1",
            f"{working_dir}/0/0/2",
            f"{working_dir}/0/0/3",
            f"{working_dir}/0/0/4",
            f"{working_dir}/0/1/0",
            f"{working_dir}/0/1/1",
            f"{working_dir}/0/1/2",
            f"{working_dir}/0/1/3",
            f"{working_dir}/0/1/4",
            f"{working_dir}/0/2/0",
            f"{working_dir}/0/2/1",
            f"{working_dir}/0/2/2",
            f"{working_dir}/0/2/3",
            f"{working_dir}/0/2/4",
            f"{working_dir}/0/3/0",
            f"{working_dir}/0/3/1",
            f"{working_dir}/0/3/2",
            f"{working_dir}/0/3/3",
            f"{working_dir}/0/3/4",
        ]
        expected_leaves = " ".join(expected_leaves_list)
        assert leaves == expected_leaves

        # Testing no leaf functionality
        all_dirs = indx.make_directory_string(just_leaf_directories=False)
        expected_all_dirs_list = [
            working_dir,
            f"{working_dir}/0",
            f"{working_dir}/0/0",
            f"{working_dir}/0/0/0",
            f"{working_dir}/0/0/1",
            f"{working_dir}/0/0/2",
            f"{working_dir}/0/0/3",
            f"{working_dir}/0/0/4",
            f"{working_dir}/0/1",
            f"{working_dir}/0/1/0",
            f"{working_dir}/0/1/1",
            f"{working_dir}/0/1/2",
            f"{working_dir}/0/1/3",
            f"{working_dir}/0/1/4",
            f"{working_dir}/0/2",
            f"{working_dir}/0/2/0",
            f"{working_dir}/0/2/1",
            f"{working_dir}/0/2/2",
            f"{working_dir}/0/2/3",
            f"{working_dir}/0/2/4",
            f"{working_dir}/0/3",
            f"{working_dir}/0/3/0",
            f"{working_dir}/0/3/1",
            f"{working_dir}/0/3/2",
            f"{working_dir}/0/3/3",
            f"{working_dir}/0/3/4",
        ]
        expected_all_dirs = " ".join(expected_all_dirs_list)
        assert all_dirs == expected_all_dirs

    def test_subhierarchy_insertion(self, temp_output_dir: str):
        """
        Test that a subhierarchy can be inserted into our `SampleIndex` properly.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Create the hierarchy and read it
        working_dir = self.get_working_dir("test_subhierarchy_insertion")
        indx = create_hierarchy(2, 1, [1], root=working_dir)
        indx.write_directories()
        indx.write_multiple_sample_index_files()
        top = read_hierarchy(os.path.abspath(working_dir))

        # Compare results
        expected = (
            ": DIRECTORY MIN 0 MAX 2 NUM_BUNDLES 2\n"
            "   0: DIRECTORY MIN 0 MAX 1 NUM_BUNDLES 1\n"
            "      0.0: BUNDLE -1 MIN 0 MAX 1\n"
            "   1: DIRECTORY MIN 1 MAX 2 NUM_BUNDLES 1\n"
            "      1.0: BUNDLE -1 MIN 1 MAX 2\n"
        )
        assert str(top) == expected

        # Create and insert the sub hierarchy
        sub_h = create_hierarchy(100, 10, address="1.0")
        top["1.0"] = sub_h

        # Compare results
        expected = (
            ": DIRECTORY MIN 0 MAX 2 NUM_BUNDLES 2\n"
            "   0: DIRECTORY MIN 0 MAX 1 NUM_BUNDLES 1\n"
            "      0.0: BUNDLE -1 MIN 0 MAX 1\n"
            "   1: DIRECTORY MIN 1 MAX 2 NUM_BUNDLES 1\n"
            "      1.0: DIRECTORY MIN 0 MAX 100 NUM_BUNDLES 10\n"
            "         1.0.0: BUNDLE 0 MIN 0 MAX 10\n"
            "         1.0.1: BUNDLE 1 MIN 10 MAX 20\n"
            "         1.0.2: BUNDLE 2 MIN 20 MAX 30\n"
            "         1.0.3: BUNDLE 3 MIN 30 MAX 40\n"
            "         1.0.4: BUNDLE 4 MIN 40 MAX 50\n"
            "         1.0.5: BUNDLE 5 MIN 50 MAX 60\n"
            "         1.0.6: BUNDLE 6 MIN 60 MAX 70\n"
            "         1.0.7: BUNDLE 7 MIN 70 MAX 80\n"
            "         1.0.8: BUNDLE 8 MIN 80 MAX 90\n"
            "         1.0.9: BUNDLE 9 MIN 90 MAX 100\n"
        )
        assert str(top) == expected

    def test_sample_index_creation_and_insertion(self, temp_output_dir: str):
        """
        Run through some basic testing of the SampleIndex class. This will try
        creating hierarchies of different sizes and inserting subhierarchies of
        different sizes as well.

        :param temp_output_dir: A pytest fixture defined in conftest.py that creates a
            temporary output path for our tests
        """
        # Define the tests for hierarchies of varying sizes
        tests = [
            (10, 1, []),
            (10, 3, []),
            (11, 2, [5]),
            (10, 3, [3]),
            (10, 3, [1]),
            (10, 1, [3]),
            (10, 3, [1, 3]),
            (10, 1, [2]),
            (1000, 100, [500]),
            (1000, 50, [500, 100]),
            (1000000000, 100000132, []),
        ]

        # Run all the tests we defined above
        for i, args in enumerate(tests):
            working_dir = self.get_working_dir(f"test_sample_index_creation_and_insertion/{i}")

            # Put at root address of "0" to guarantee insertion at "0.1" later is valid
            idx = create_hierarchy(args[0], args[1], args[2], address="0", root=working_dir)
            self.write_hierarchy_for_debug(idx)

            # Inserting hierarchy at 0.1
            try:
                idx["0.1"] = create_hierarchy(args[0], args[1], args[2], address="0.1")
            except KeyError:
                assert False
