Testing Merlin
==============

.. note:: Merlin testing is under active development.
       Settings and defaults are subject to change.

       Benjamin Bay (bay1@llnl.gov) is the current point-of-contact 
       responsible for the documentation on this page.

This section provides documentation on testing in Merlin. 
We differentiate between 3 types of testing:

#. :ref:`unit-testing`
#. :ref:`integration-testing`
#. :ref:`style-checks`

Tests are not to be confused with examples, though examples may be 
used as high-level system smoke tests. Documentation on Merlin 
examples can be found `here. 
<https:/github.com/LLNL/merlin/tree/master/workflows>`_

.. _unit-testing:

Unit tests
**********
To run Merlin unit tests, from the primary directory simply type::

    (merlin3_7) $ make unit-tests

which in turn will run::

    (merlin3_7) $ py.tests merlin/ 

Python versions
+++++++++++++++
To test the project code on different versions of python, use::

    (merlin3_7) $ make version-tests

which is equivalent to::

    (merlin3_7) $ tox

See the file ``tox.ini`` for details on what this runs.


.. _integration-testing:

Continuous integration testing
******************************


Continuous integration testing is being implemented for the GitHub repo.


.. _style-checks:

Style checks
************
Our all-inclusive Makefile target for style-checking is::

    (merlin3_7) $ make check-style

.. warning:: The combined checking tools in the ``check-style`` target may
   produce hundreds of lines of output text.

To customize what is tested for, see the files ``tox.ini``.
