Getting Started
================

Quick Start
++++++++++++++
::

    pip3 install merlin

All set up? See the :doc:`Merlin Commands <./merlin_commands>` section for using merlin.

Check out the :doc:`Tutorial<./tutorial>`!

Developer Setup
++++++++++++++++++
To install with the additional developer dependencies, use::

    pip3 install "merlin[dev]"

or::

    pip3 install -e "git+https://github.com/LLNL/merlin.git@develop#egg=merlin[dev]"

See the :doc:`Spack <./spack>` section for an alternative method to setup merlin on supercomputers.


Configuring Merlin
*******************

Once Merlin has been installed, the installation needs to be configured.
Documentation for merlin configuration is in the :doc:`Configuring Merlin <./merlin_config>` section.

That's it. To start running Merlin see the :doc:`Merlin Workflows. <./merlin_workflows>`


Custom Setup
+++++++++++++

This section documents how to install Merlin without using the Makefile. This
setup is more complicated; however, allows for more customization of the setup
configurations.

Clone the `Merlin <https://github.com/LLNL/merlin.git>`_
repository::

    git clone https://github.com/LLNL/merlin.git


Create a virtualenv
*******************

Merlin uses `virtualenvs <https://virtualenv.pypa.io/en/stable/>`_ to manage
package dependencies which can be installed via Pip, Python's default
package manager.

More documentation about using Virtualenvs with Merlin can be found at
:doc:`Using Virtualenvs with Merlin <./virtualenv>`.

To create a new virtualenv and activate it:

.. code:: bash

    $ python3 -m venv venv_merlin_$SYS_TYPE_py3_6
    $ source venv_merlin_$SYS_TYPE_py3/bin/activate  # Or activate.csh for .cshrc


Install Python Package Dependencies
************************************

Merlin uses Pip to manage Python dependencies. Merlin dependencies can be
found in the requirements directory in the Merlin repository.

To install the standard set of dependencies run:

.. code:: bash

    (merlin3_7) $ pip install -r requirements.txt

This will install all the required dependencies for Merlin and development
development dependencies.


Installing Merlin
*******************

Merlin can be installed in editable mode. From within the Merlin repository:

.. code:: bash

    (merlin3_7) $ pip install -e .

Any changes made to the Merlin source code should automatically reflect in the
virtualenv.

.. tip:: If changes to Merlin's source code do not reflect when running Merlin
    try running `pip install -e .` from within the Merlin repository.
