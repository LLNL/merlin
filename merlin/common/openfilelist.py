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

#!/usr/bin/env python

"""
  OpenFileList

  A synthetic file class that opens a list of files and reads them as if they
  were a single file

  SYNOPSIS:

    with OpenFileList(["file1.txt","file2.txt",...]) as f :
      print f.read();

  reads the concatenation of file1.txt, file2.txt, etc.

  file methods supported :

    f.read([bytes])
    f.readlines([bytes])
    f.readline([bytes])
    f.tell()
    f.close()
    f.__iter__()

  TODO:
    implement a seek method

"""

import copy


class OpenFileList:
    openwas = open

    def __new__(cls, files, *v, **kw):
        if isinstance(files, str):
            return open(files, *v, **kw)
        return super(OpenFileList, cls).__new__(cls)

    def __init__(self, files, *v, **kw):
        self.files = copy.copy(files)
        self.argv, self.argkw = (v, kw)
        if self.files:
            self.fnnow = self.files.pop(0)
            self.fnow = open(self.fnnow, *v, **kw) if files else None
            self.atend = False
        else:
            self.fnnow = self.fnow = None
            self.atend = True
        self._tell = 0

    def _errclosed(self):
        raise ValueError("I/O operation on closed file")

    def _tonext(self):
        if self.fnow is not None:
            self._tell += self.fnow.tell()
            self.fnow.close()
            if self.files:
                self.fnnow = self.files.pop(0)
                self.fnow = open(self.fnnow, *self.argv, **self.argkw)
            else:
                self.fnnow = self.fnow = None
                self.atend = True

    def tell(self):
        if self.fnow is None:
            return self._tell
        return self._tell + self.fnow.tell()

    def read(self, n=None):
        if self.atend:
            return ""
        if self.fnow is None:
            self._errclosed()
        if n is None:
            n = 1 << 32
        s = ""
        while n and (self.files or self.fnow is not None):
            ns = self.fnow.read(n)
            if ns:
                n -= len(ns)
                s += ns
            else:
                self._tonext()
        return s

    def readlines(self, b=None):
        if self.atend:
            return []
        if self.fnow is None:
            self._errclosed()
        s = self.read(b)
        if not s:
            return []
        if not s.endswith("\n"):
            ch = ""
            while ch != "\n":
                ch = self.read(1)
                if ch == "" or ch == "\n":
                    break
                s += ch
            return s.split("\n")
        else:
            return s[:-1].split("\n")

    def readline(self, b=None):
        if self.atend:
            return []
        if self.fnow is None:
            return self._errclosed()
        if b is None:
            s = self.fnow.readline()
        else:
            s = self.fnow.readline(b)
        if not s:
            self._tonext()
        return s

    def __iter__(self):
        while not self.atend:
            yield self.readline()

    def close(self):
        if self.fnow is not None:
            self.fnow.close()
        self.files = []
        self.atend = True
        self.fnow = self.fnnow = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tback):
        self.close()


if __name__ == "__main__":
    import unittest
    import os
    import numpy
    import uuid

    class TestOpenFileList(unittest.TestCase):
        def test_opener(self):
            stride = 5
            fn = [str(uuid.uuid1()) for i in range(3)]
            e = numpy.diag(numpy.arange(float(stride) * len(fn)))

            for n, ff in enumerate(fn):  # Create files.
                with open(ff, "w") as f:
                    for i in range(stride * n, stride * (n + 1)):
                        print(" ".join(map(str, e[i])), file=f)

            with OpenFileList(fn) as f:  # Load using loadtxt method.
                ep = numpy.loadtxt(f)

            for i in fn:
                os.unlink(i)  # Delete files.

            self.assertTrue(numpy.all(ep == e))

    unittest.main()
