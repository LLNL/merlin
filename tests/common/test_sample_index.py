import os
import shutil
import unittest
from contextlib import suppress

from merlin.common.sample_index_factory import create_hierarchy, read_hierarchy


TEST_DIR = "UNIT_TEST_SPACE"


def clear_test_tree():
    with suppress(FileNotFoundError):
        shutil.rmtree(TEST_DIR)


def clear(func):
    def wrapper():
        clear_test_tree()
        func()
        clear_test_tree()

    return wrapper


@clear
def test_index_file_writing():
    indx = create_hierarchy(
        1000000000, 10000, [100000000, 10000000, 1000000], root=TEST_DIR
    )
    indx.write_directories()
    indx.write_multiple_sample_index_files()
    indx2 = read_hierarchy(TEST_DIR)
    assert indx2.get_path_to_sample(123000123) == indx.get_path_to_sample(123000123)


def test_bundle_retrieval():
    indx = create_hierarchy(
        1000000000, 10000, [100000000, 10000000, 1000000], root=TEST_DIR
    )
    expected = f"{TEST_DIR}/0/0/0/samples0-10000.ext"
    result = indx.get_path_to_sample(123)
    assert expected == result

    expected = f"{TEST_DIR}/0/0/0/samples10000-20000.ext"
    result = indx.get_path_to_sample(10000)
    assert expected == result

    expected = f"{TEST_DIR}/1/2/3/samples123000000-123010000.ext"
    result = indx.get_path_to_sample(123000123)
    assert expected == result


def test_start_sample_id():
    expected = """: DIRECTORY MIN 203 MAX 303 NUM_BUNDLES 10
   0: BUNDLE 0 MIN 203 MAX 213
   1: BUNDLE 1 MIN 213 MAX 223
   2: BUNDLE 2 MIN 223 MAX 233
   3: BUNDLE 3 MIN 233 MAX 243
   4: BUNDLE 4 MIN 243 MAX 253
   5: BUNDLE 5 MIN 253 MAX 263
   6: BUNDLE 6 MIN 263 MAX 273
   7: BUNDLE 7 MIN 273 MAX 283
   8: BUNDLE 8 MIN 283 MAX 293
   9: BUNDLE 9 MIN 293 MAX 303
"""
    idx203 = create_hierarchy(100, 10, start_sample_id=203)
    assert expected == str(idx203)


@clear
def test_directory_writing():
    path = os.path.join(TEST_DIR)
    indx = create_hierarchy(2, 1, [1], root=path)
    expected = """: DIRECTORY MIN 0 MAX 2 NUM_BUNDLES 2
   0: DIRECTORY MIN 0 MAX 1 NUM_BUNDLES 1
      0.0: BUNDLE 0 MIN 0 MAX 1
   1: DIRECTORY MIN 1 MAX 2 NUM_BUNDLES 1
      1.0: BUNDLE 1 MIN 1 MAX 2
"""
    assert expected == str(indx)
    indx.write_directories()
    assert os.path.isdir(f"{TEST_DIR}/0")
    assert os.path.isdir(f"{TEST_DIR}/1")
    indx.write_multiple_sample_index_files()

    clear_test_tree()

    path = os.path.join(TEST_DIR)
    indx = create_hierarchy(1000000000, 10000, [100000000, 10000000], root=path)
    indx.write_directories()
    path = indx.get_path_to_sample(123000123)
    assert os.path.exists(os.path.dirname(path))
    assert path != TEST_DIR
    path = indx.get_path_to_sample(10000000000)
    assert path == TEST_DIR

    clear_test_tree()

    path = os.path.join(TEST_DIR)
    indx = create_hierarchy(
        1000000000, 10000, [100000000, 10000000, 1000000], root=path
    )
    indx.write_directories()


def test_directory_path():
    indx = create_hierarchy(20, 1, [20, 5, 1], root="")
    leaves = indx.make_directory_string()
    expected_leaves = "0/0/0 0/0/1 0/0/2 0/0/3 0/0/4 0/1/0 0/1/1 0/1/2 0/1/3 0/1/4 0/2/0 0/2/1 0/2/2 0/2/3 0/2/4 0/3/0 0/3/1 0/3/2 0/3/3 0/3/4"
    assert leaves == expected_leaves
    all_dirs = indx.make_directory_string(just_leaf_directories=False)
    expected_all_dirs = " 0 0/0 0/0/0 0/0/1 0/0/2 0/0/3 0/0/4 0/1 0/1/0 0/1/1 0/1/2 0/1/3 0/1/4 0/2 0/2/0 0/2/1 0/2/2 0/2/3 0/2/4 0/3 0/3/0 0/3/1 0/3/2 0/3/3 0/3/4"
    assert all_dirs == expected_all_dirs


@clear
def test_subhierarchy_insertion():
    indx = create_hierarchy(2, 1, [1], root=TEST_DIR)
    print("Writing directories")
    indx.write_directories()
    indx.write_multiple_sample_index_files()
    print("reading heirarchy")
    top = read_hierarchy(os.path.abspath(TEST_DIR))
    expected = """: DIRECTORY MIN 0 MAX 2 NUM_BUNDLES 2
   0: DIRECTORY MIN 0 MAX 1 NUM_BUNDLES 1
      0.0: BUNDLE -1 MIN 0 MAX 1
   1: DIRECTORY MIN 1 MAX 2 NUM_BUNDLES 1
      1.0: BUNDLE -1 MIN 1 MAX 2
"""
    assert str(top) == expected
    print("creating sub_heirarchy")
    sub_h = create_hierarchy(100, 10, address="1.0")
    print("inserting sub_heirarchy")
    top["1.0"] = sub_h
    print(str(indx))
    print("after insertion")
    print(str(top))
    expected = """: DIRECTORY MIN 0 MAX 2 NUM_BUNDLES 2
   0: DIRECTORY MIN 0 MAX 1 NUM_BUNDLES 1
      0.0: BUNDLE -1 MIN 0 MAX 1
   1: DIRECTORY MIN 1 MAX 2 NUM_BUNDLES 1
      1.0: DIRECTORY MIN 0 MAX 100 NUM_BUNDLES 10
         1.0.0: BUNDLE 0 MIN 0 MAX 10
         1.0.1: BUNDLE 1 MIN 10 MAX 20
         1.0.2: BUNDLE 2 MIN 20 MAX 30
         1.0.3: BUNDLE 3 MIN 30 MAX 40
         1.0.4: BUNDLE 4 MIN 40 MAX 50
         1.0.5: BUNDLE 5 MIN 50 MAX 60
         1.0.6: BUNDLE 6 MIN 60 MAX 70
         1.0.7: BUNDLE 7 MIN 70 MAX 80
         1.0.8: BUNDLE 8 MIN 80 MAX 90
         1.0.9: BUNDLE 9 MIN 90 MAX 100
"""
    assert str(top) == expected


def test_sample_index():
    """Run through some basic testing of the SampleIndex class."""
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

    for args in tests:
        print(f"############ TEST {args[0]} {args[1]} {args[2]} ###########")
        # put at root address of "0" to guarantee insertion at "0.1" later is valid
        idx = create_hierarchy(args[0], args[1], args[2], address="0")
        print(str(idx))
        try:
            idx["0.1"] = create_hierarchy(args[0], args[1], args[2], address="0.1")
            print("successful set")
            print(str(idx))
        except KeyError as error:
            print(error)
            assert False
