Virtual environments
====================

This section provides a quick reference for using
`virtual environments <https://virtualenv.pypa.io/en/stable/>`_  for the Merlin project.


Creating a virtual environment
++++++++++++++++++++++++++++++

To create a new virtual environment:

.. code:: bash

    $ python3 -m venv venv

.. caution:: A virtual environment will need to be created for each system type. It's
  recommended to name the virtual environment `venv_<system>` to make it easier to
  switch between them. This documentation will use `venv` for simplicity to
  reference the virtual environment.

.. tip:: Virtual environments provide an isolated environment for working on Python
    projects to avoid dependency conflicts.


Activating a Virtualenv
------------------------

Once the virtual environment is created it can be activated like so:

.. code:: bash

    $ source venv/bin/activate
    (venv) $

This will set the Python and Pip path to the virtual environment at ``venv/bin/python``
and ``venv/bin/pip`` respectively.

The virtual environment name should now display in the terminal, which means
it is active. Any calls to pip will install to the virtual environment.

.. tip:: To verify that Python and Pip are pointing to the virtual environment, run
    ``$ which python`` and ``$ which pip``.


Deactivating a Virtualenv
---------------------------

Virtualenvs can be exited via the following:

.. code:: bash

    (venv) $ deactivate
    $
