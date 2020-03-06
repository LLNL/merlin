Contributing
============

Welcome to the Merlin developer documentation! This section provides
instructions for contributing to Merlin.

Getting Started
++++++++++++++++


Follow the :doc:`Getting Started <./getting_started>` documentation to setup
your Merlin development environment.

Once your development is setup create a branch:

.. code-block:: bash

    $ git checkout -b feature/<username>/description

.. note::

    Other common types of branches besides feature are: ``bugfix``,
    ``hotfix``, or ``refactor``.

    Select the branch type that best represents the development.

Merlin follows a `gitflow workflow <https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow>`_.
Updates to the develop branch are made via pull requests.


Developer Guide
+++++++++++++++

This section provides Merlin's guide for contributing features/bugfixes to
Merlin.

Pull Request Checklist
++++++++++++++++++++++

.. warning:: All pull requests must pass ``make tests`` prior to consideration!

To expedite review, please ensure that pull requests

- Are from a meaningful branch name (e.g. ``feature/my_name/cool_thing``)

- Into the `appropriate branch <https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow>`_

- Include testing for any new features

  - unit tests in ``tests/*``
  - integration tests in ``tests/integration``

- Include descriptions of the changes

  - a summary in the pull request
  - details in ``CHANGELOG.md``

- Ran ``make fix-style`` to adhere to style guidelines

- Pass ``make tests``; output included in pull request

- Increment version number `appropriately <https://semver.org>`_

  - in ``CHANGELOG.md``
  - in ``merlin.__init__.py``

Testing
+++++++

All pull requests must pass unit and integration tests. To ensure that they do run

.. code-block:: bash

    $ make tests

Python Code Style Guide
++++++++++++++++++++++++

This section documents Merlin's style guide. Unless otherwise specified,
`PEP-8 <https://www.python.org/dev/peps/pep-0008/>`_
is the preferred coding style and `PEP-0257 <https://www.python.org/dev/peps/pep-0257/>`_
for docstrings.

.. note:: ``make fix-style`` will automatically fix any style issues.

Merlin has style checkers configured. They can be run from the Makefile:

.. code-block:: bash

    $ make check-style
