# Contributing

Welcome to the Merlin developer documentation! This section provides
instructions for contributing to Merlin.

## Getting Started

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


## Developer Guide

This section provides Merlin's guide for contributing features/bugfixes to
Merlin.

## Pull Request Checklist

.. warning:: All pull requests must pass ``make tests`` prior to consideration!

To expedite review, please ensure that pull requests

- Are from a meaningful branch name (e.g. ``feature/my_name/cool_thing``)

- Are being merged into the `appropriate branch <https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow>`_

- Include testing for any new features

  - unit tests in ``tests/unit``
  - integration tests in ``tests/integration``

- Include descriptions of the changes

  - a summary in the pull request
  - details in ``CHANGELOG.md``

- Ran ``make fix-style`` to adhere to style guidelines

- Pass ``make tests``; output included in pull request

- Increment version number `appropriately <https://semver.org>`_

  - in ``CHANGELOG.md``
  - in ``merlin.__init__.py``

- Have `squashed <https://github.com/LLNL/merlin/wiki/Squash-commits>`_ commits

## Testing

All pull requests must pass unit and integration tests. To ensure that they do run

.. code-block:: bash

    $ make tests

## Python Code Style Guide

This section documents Merlin's style guide. Unless otherwise specified,
`PEP-8 <https://www.python.org/dev/peps/pep-0008/>`_
is the preferred coding style and `PEP-0257 <https://www.python.org/dev/peps/pep-0257/>`_
for docstrings.

.. note:: ``make fix-style`` will automatically fix any style issues.

Merlin has style checkers configured. They can be run from the Makefile:

.. code-block:: bash

    $ make check-style

## Adding New Features to YAML Spec File

In order to conform to Maestro's verification format introduced in Maestro v1.1.7,
we now use `json schema <https://www.json-schema.org>`_ validation to verify our spec
file. 

If you are adding a new feature to Merlin that requires a new block within the yaml spec
file or a new property within a block, then you are going to need to update the 
merlinspec.json file located in the merlin/spec/ directory. You also may want to add 
additional verifications within the specification.py file located in the same directory. 

.. note::
   If you add custom verifications beyond the pattern checking that the json schema
   checks for, then you should also add tests for this verification in the test_specification.py
   file located in the merlin/tests/unit/spec/ directory. Follow the steps for adding new
   tests in the docstring of the TestCustomVerification class.

### Adding a New Property

To add a new property to a block in the yaml file, you need to create a 
template for that property and place it in the correct block in merlinspec.json. For 
example, say I wanted to add a new property called ``example`` that's an integer within 
the ``description`` block. I would modify the ``description`` block in the merlinspec.json file to look 
like this:

.. code-block:: json

    "DESCRIPTION": {
      "type": "object",
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "description": {"type": "string", "minLength": 1},
        "example": {"type": "integer", "minimum": 1}
      },
      "required": ["name", "description"]
    }

If you need help with json schema formatting, check out the `step-by-step getting 
started guide <https://json-schema.org/learn/getting-started-step-by-step.html>`_.

That's all that's required of adding a new property. If you want to add your own custom
verifications make sure to create unit tests for them (see the note above for more info).

### Adding a New Block

Adding a new block is slightly more complicated than adding a new property. You will not
only have to update the merlinspec.json schema file but also add calls to verify that 
block within specification.py.

To add a block to the json schema, you will need to define the template for that entire
block. For example, if I wanted to create a block called ``country`` with two 
properties labeled ``name`` and ``population`` that are both required, it would look like so:

.. code-block:: json

    "COUNTRY": {
      "type": "object",
      "properties": {
        "name": {"type": "string", "minLength": 1},
        "population": {
          "anyOf": [
            {"type": "string", "minLength": 1},
            {"type": "integer", "minimum": 1}
          ]
        }
      },
      "required": ["name", "capital"]
    }

Here, ``name`` can only be a string but ``population`` can be both a string and an integer. 
For help with json schema formatting, check out the `step-by-step getting started guide`_.

The next step is to enable this block in the schema validation process. To do this we need to:

#. Create a new method called verify_<your_block_name>() within the MerlinSpec class
#. Call the YAMLSpecification.validate_schema() method provided to us via Maestro in your new method
#. Add a call to verify_<your_block_name>() inside the verify() method

If you add your own custom verifications on top of this, please add unit tests for them.
