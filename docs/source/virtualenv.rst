Using Virtualenvs with Merlin
==============================

This section provides a quick reference for using
`virtualenvs <https://virtualenv.pypa.io/en/stable/>`_  for the Merlin project.


Creating a Virtualenv
++++++++++++++++++++++

To create a new virtualenv::

    $ python -m venv venv

.. caution:: A virtualenv will need to be created for each system type. It's
  recommended to name the virtualenv `venv_<system>` to make it easier to
  switch between them. This documentation will use `venv` for simplicity to
  reference the virtualenv.

.. tip:: Virtualenvs provide an isolated environment for working on Python
    projects to avoid dependency conflicts.


Activating a Virtualenv
------------------------

Once the virtualenv is created it can be activated like so::

    $ source venv/bin/activate
    (venv) $

This will set the Python and Pip path to the virtualenv at ``venv/bin/python``
and ``venv/bin/pip`` respectively.

The virtualenv name should now display in the terminal which means the
virtualenv is active. Any calls to pip will install to the virtualenv.

.. tip:: To verify that Python and Pip are pointing to the virtualenv run
    ``$ which python`` and ``$ which pip``.


Deactivating a Virtualenv
---------------------------

Virtualenvs can be exited via the following::

    (venv) $ deactivate
    $
