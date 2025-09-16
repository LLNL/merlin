##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

import os

from setuptools import find_packages, setup


version = __import__("merlin").VERSION

extras = ["dev"]


def readme():
    with open("README.md") as f:
        return f.read()


# The reqs code from celery setup.py
def _strip_comments(line: str):
    """Removes comments from a line passed in from _reqs()."""
    return line.split("#", 1)[0].strip()


def _pip_requirement(req):
    if req.startswith("-r "):
        _, path = req.split()
        return reqs(*path.split(os.path.sep))
    return [req]


def _reqs(*f):
    return [
        _pip_requirement(r)
        for r in (_strip_comments(line) for line in open(os.path.join(os.getcwd(), "requirements", *f)).readlines())  # noqa
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
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
        ]
    },
    include_package_data=True,
    zip_safe=False,
)
