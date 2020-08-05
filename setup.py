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

import os

from setuptools import find_packages, setup


version = __import__("merlin").VERSION

extras = ["dev"]


def readme():
    with open("README.md") as f:
        return f.read()


# The reqs code from celery setup.py
def _strip_comments(l):
    return l.split("#", 1)[0].strip()


def _pip_requirement(req):
    if req.startswith("-r "):
        _, path = req.split()
        return reqs(*path.split(os.path.sep))
    return [req]


def _reqs(*f):
    return [
        _pip_requirement(r)
        for r in (
            _strip_comments(l)
            for l in open(os.path.join(os.getcwd(), "requirements", *f)).readlines()
        )
        if r
    ]


def reqs(*f):
    """Parse requirement file.
    Example:
        reqs('default.txt')          # requirements/default.txt
        reqs('extras', 'redis.txt')  # requirements/extras/redis.txt
    Returns:
        List[str]: list of requirements specified in the file.
    """
    trl = [req for subreq in _reqs(*f) for req in subreq]
    rl = [r for r in trl if "-e" not in r]
    return rl


def install_requires():
    """Get list of requirements required for installation."""
    return reqs("release.txt")


def extras_require():
    """Get map of all extra requirements."""
    return {x: reqs(x + ".txt") for x in extras}


setup(
    name="merlin",
    author="Merlin Dev team",
    author_email="merlin@llnl.gov",
    version=version,
    description="The building blocks of workflows!",
    long_description=readme(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="machine learning workflow",
    url="https://github.com/LLNL/merlin",
    license="MIT",
    packages=find_packages(exclude=["tests.*", "tests"]),
    install_requires=install_requires(),
    extras_require=extras_require(),
    entry_points={
        "console_scripts": [
            "merlin=merlin.main:main",
            "merlin-templates=merlin.merlin_templates:main",
        ]
    },
    include_package_data=True,
    zip_safe=False,
)
