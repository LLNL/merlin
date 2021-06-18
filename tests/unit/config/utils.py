"""
Utils module for common test functionality.
"""
import os


def mkfile(tmpdir, filename, content=""):
    """
    A simple function for creating a file and returning the path. This is to
    abstract out file creation logic in the tests.

    :param tmpdir: (str) The path to the temp directory.
    :param filename: (str) The name of the file.
    :param contents: (str) Optional contents to write to the file. Defaults to
        an empty string.
    :returns: (str) The appended path of the given tempdir and filename.
    """
    filepath = os.path.join(tmpdir, filename)

    with open(filepath, "w") as f:
        f.write(content)

    return filepath
