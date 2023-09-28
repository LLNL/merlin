#!/usr/bin/env python

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
smallnpylib

A simple library to read the .npy file header and return a dict
containing the header from the .npy file as well as a few other
keys. Also provides the OpenNPY class which is a way of seeking
into .npy files without using the memory mapping functionality
in the load function. Finally, there is the OpenNPYList class
which opens a list of OpenNPY files and allows for random
access among all of them.

  'shape'         shape for the array
  'fortran_order' is it in fortran order (normally false)
  'descr'         the dtype description of the array
  'size'          total size of the data
  'offset'        position in file of start of data
  'rowsize'       the total size in bytes of a single row in the file (e.g. sample)
  'itemsize'      the size of a single element in the array

  SYNOPSIS :

    d = get_npy_info("myfile.txt");
    print d['offset']     # offset to data in file
    print d['rowsize']    # number of bytes per row

    with open("myfile.npy") as f :
      f.seek(d['offset']+d['rowsize]*N);

   OpenNPY

   with OpenNPY("myfile.npy") as a :
     print a[5]         # print row number 5
     print a[1:4]       # print rows from 1,2,3
     my_array = a.to_array();
     print len(a)       # number of rows in file
     for i in a :
       print i          # print all the rows in a
     print a.shape      # shape of array
     print a.dtype      # dtype of array

   with OpenNPYList(["myfile1.npy","myfile2.npy",...]) as a :
     print a[5]         # print row number 5
     print a[1:4]       # print rows from 1,2,3
     my_array = a.to_array();
     print len(a)       # number of rows in file
     for i in a :
       print i          # print all the rows in a
     print a.shape      # shape of array
     print a.dtype      # dtype of array

"""
# This file is not currently used so we don't care what pylint has to say
# pylint: skip-file

from typing import List, Tuple

import numpy as np


try:
    unistr = (unicode, str)
    npy_magic = "\x93NUMPY"
except NameError:
    unistr = str
    npy_magic = b"\x93NUMPY"


def _get_npy_info2(f):
    if isinstance(f, unistr):
        f = open(f, "rb")  # noqa
    magic = f.read(6)  # must be .npy file
    assert magic == npy_magic  # must be .npy file or ELSE
    major, _ = list(map(ord, f.read(2)))
    if major == 1:
        hlen_char = list(map(ord, f.read(2)))
        hlen = hlen_char[0] + 256 * hlen_char[1]
    elif major == 2:
        hlen_char = list(map(ord, f.read(4)))
        # fmt: off
        hlen = (
            hlen_char[0]
            + 256 * hlen_char[1]
            + 65536 * hlen_char[2]
            + (1 << 24) * hlen_char[3]
        )
        # fmt: on
    else:
        raise Exception("unknown .npy format, e.g. not 1 or 2")
    hdr = eval(f.read(hlen))  # TODO remove eval
    hdr["dtype"] = np.dtype(hdr["descr"])
    hdr["offset"] = f.tell()  # location of data start
    hdr["itemsize"] = np.dtype(hdr["descr"]).itemsize
    hdr["rowsize"] = hdr["itemsize"] * np.product(hdr["shape"][1:])
    hdr["items"] = np.product(hdr["shape"])
    hdr["size"] = hdr["itemsize"] * hdr["items"]
    return f, hdr


def _get_npy_info3(f):
    if isinstance(f, unistr):
        f = open(f, "rb")  # noqa
    magic = f.read(6)  # must be .npy file
    assert magic == npy_magic  # must be .npy file or ELSE
    major, _ = list(f.read(2))
    if major == 1:
        hlen_char = list(f.read(2))
        hlen = hlen_char[0] + 256 * hlen_char[1]
    elif major == 2:
        hlen_char = list(f.read(4))
        # fmt: off
        hlen = (
            hlen_char[0]
            + 256 * hlen_char[1]
            + 65536 * hlen_char[2]
            + (1 << 24) * hlen_char[3]
        )
        # fmt: on
    else:
        raise Exception("unknown .npy format, e.g. not 1 or 2")
    hdr = eval(f.read(hlen))  # TODO remove eval
    hdr["dtype"] = np.dtype(hdr["descr"])
    hdr["offset"] = f.tell()  # location of data start
    hdr["itemsize"] = np.dtype(hdr["descr"]).itemsize
    hdr["rowsize"] = hdr["itemsize"] * np.product(hdr["shape"][1:])
    hdr["items"] = np.product(hdr["shape"])
    hdr["size"] = hdr["itemsize"] * hdr["items"]
    return f, hdr


def _get_npy_info(f):
    d = None
    try:
        d = _get_npy_info2(f)
    except TypeError:
        d = _get_npy_info3(f)

    return d


def get_npy_info(f):
    try:
        d = _get_npy_info2(f)
    except TypeError:
        d = _get_npy_info3(f)
    d[0].close()
    return d[1]


def read_items(f, hdr, idx, n=-1, sep=""):
    f.seek(hdr["offset"] + idx * hdr["itemsize"])
    if n < 0:
        n = hdr["items"] - idx
    n = min(hdr["items"] - idx, n)
    print(f"n is {n}")
    return np.fromfile(f, dtype=hdr["dtype"], count=n, sep=sep)


def read_rows(f, hdr, idx, n=-1, sep=""):
    f.seek(hdr["offset"] + idx * hdr["rowsize"])
    if n < 0:
        n = hdr["shape"][0] - idx
    n = min(hdr["shape"][0] - idx, n)
    a = np.fromfile(f, dtype=hdr["dtype"], count=n * hdr["rowsize"] // hdr["itemsize"], sep=sep)
    return np.reshape(a, (n,) + hdr["shape"][1:])


def verify_open(func):  # A wrapper function used by FileSamples.
    """
    :param func: (function) a class instance method that needs to call
          _verify_open before doing anything else.
    """

    def wrapper(self, *v, **kw):
        self._verify_open()
        return func(self, *v, **kw)

    return wrapper


class OpenNPY:
    def __init__(self, f):
        self.hdr = self.f = None
        if isinstance(f, unistr):
            self.fname = f
        else:
            self.f = f
            self._verify_open()

    def _verify_open(self):
        if self.f is None:
            self.f, self.hdr = _get_npy_info(self.f if self.f is not None else self.fname)

    @verify_open
    def load_header(self, close=True):
        if close:
            self.close()
        return self.hdr

    def close(self):
        if self.f is not None:
            self.f.close()
            self.f = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tback):
        self.close()

    @verify_open
    def _shape(self):
        return self.hdr["shape"]

    shape = property(fget=_shape)

    @verify_open
    def _dtype(self):
        return self.hdr["dtype"]

    dtype = property(fget=_dtype)

    @verify_open
    def __getitem__(self, k):
        if isinstance(k, int):
            return read_rows(self.f, self.hdr, k, 1)[0]
        elif isinstance(k, slice):
            if k.step == 1 or k.step is None:
                return read_rows(self.f, self.hdr, k.start, k.stop - k.start)
            else:
                return np.asarray(
                    [read_rows(self.f, self.hdr, _, 1)[0] for _ in range(k.start, k.stop, 1 if k.step is None else k.step)]
                )

    @verify_open
    def __len__(self):
        return self.hdr["items"]

    @verify_open
    def __iter__(self):
        for k in range(self.hdr["shape"][0]):
            yield self[k]

    @verify_open
    def to_array(self):
        return read_rows(self.f, self.hdr, 0)


class OpenNPYList:
    def __init__(self, filename_strs: List[str]):
        self.filenames: List[str] = filename_strs
        self.files: List[OpenNPY] = [OpenNPY(file_str) for file_str in self.filenames]
        i: OpenNPY
        for i in self.files:
            i.load_header()
        self.shapes: List[Tuple[int]] = [openNPY_obj.hdr["shape"] for openNPY_obj in self.files]
        k: Tuple[int]
        for k in self.shapes[1:]:
            # Match subsequent axes shapes.
            if k[1:] != self.shapes[0][1:]:
                raise AttributeError(f"Mismatch in subsequent axes shapes: {k[1:]} != {self.shapes[0][1:]}")
        self.tells: np.ndarray = np.cumsum([arr_shape[0] for arr_shape in self.shapes])  # Tell locations.
        self.tells = np.hstack(([0], self.tells))

    def close(self):
        for i in self.files:
            i.close()

    def __del__(self):
        self.close()

    def __iter__(self):
        for i in self.files:
            for j in i:
                yield j

    def __getitem__(self, k):
        if isinstance(k, int):
            if k < 0:
                k = self.tells[-1] + k  # Negative indexing.
            if k >= self.tells[-1]:
                raise IndexError("index %d is out of bounds" % k)
            fno = (self.tells > k).argmax()
            return self.files[fno - 1][k - self.tells[fno - 1]]
        else:  # Slice indexing.
            # TODO : Implement a faster version.
            return np.asarray([self[_] for _ in np.arange(k.start, k.stop, k.step if k.step is not None else 1)])

    def to_array(self):
        return np.vstack([_.to_array() for _ in self.files])

    def __len__(self):
        return self.tells[-1]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tback):
        self.close()


__all__ = [
    "OpenNPYList",
    "OpenNPY",
    "unistr",
    "read_items",
    "read_rows",
    "get_npy_info",
]

if __name__ == "__main__":
    import os
    import sys
    import unittest
    import uuid

    if "-h" in sys.argv:
        print(__doc__)
        sys.exit(1)

    class TestOpenNPY(unittest.TestCase):
        def test_seeknpy(self):
            e = np.diag(np.arange(5.0))
            try:
                fn = unicode(str(uuid.uuid1()) + ".npy")
            except NameError:
                fn = str(str(uuid.uuid1()) + ".npy")
            np.save(fn, e)
            with OpenNPY(fn) as a:
                ep = np.asarray([_ for _ in a])
            os.unlink(fn)
            self.assertTrue((ep == e).all())

        def test_seeknpylist(self):
            e = np.diag(np.arange(5.0))
            fn = str(uuid.uuid1()) + ".npy"
            np.save(fn, e)
            with OpenNPYList([fn, fn, fn]) as a:
                ep = np.asarray([_ for _ in a])
                en = np.asarray(a[5:10])
                en2 = np.asarray(a[11:14])
                en3 = np.asarray(a[1:14])
                en4 = a.to_array()
                # test __len__ method
                self.assertEqual(len(a), 3 * len(e))
            os.unlink(fn)
            # test read slice of whole file
            self.assertTrue((en == e).all())
            self.assertTrue((en2 == e[1:4]).all())  # test slice
            # test read all
            self.assertTrue((ep == np.vstack((e, e, e))).all())
            # test to_array method
            self.assertTrue((en4 == np.vstack((e, e, e))).all())
            # test slice read across files
            self.assertTrue((en3 == np.vstack((e, e, e))[1:14]).all())

    unittest.main()
