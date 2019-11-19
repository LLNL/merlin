import os
from setuptools import setup
from setuptools import find_packages

version = __import__("merlin").VERSION

extras = ["mysql"]


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
    version=version,
    description="The building blocks of workflows!",
    long_description=readme(),
    classifiers=["Programming Language :: Python :: 3.6+"],
    keywords="machine learning workflow",
    url="https://lc.llnl.gov/bitbucket/projects/MLSI/repos/merlin/browse",
    license="MIT",
    packages=find_packages(exclude=["tests.*", "tests"]),
    install_requires=install_requires(),
    extras_require=extras_require(),
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            "merlin=merlin.main:main",
            "merlin-templates=merlin.merlin_templates:main"
        ]
    },
    include_package_data=True,
    zip_safe=False,
)
