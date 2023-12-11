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
The developer setup can be done via pip or via make. This section will cover how to do both.

Additionally, there is an alternative method to setup merlin on supercomputers. See the :doc:`Spack <./spack>` section for more details.

Pip Setup
******************

To install with the additional developer dependencies, use::

    pip3 install "merlin[dev]"

or::

    pip3 install -e "git+https://github.com/LLNL/merlin.git@develop#egg=merlin[dev]"

Make Setup
*******************

Visit the `Merlin repository <https://github.com/LLNL/merlin/>`_ on github. `Create a fork of the repo <https://docs.github.com/en/get-started/quickstart/fork-a-repo>`_ and `clone it <https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository>`_ onto your system.

Change directories into the merlin repo:

.. code-block:: bash

    $ cd merlin/

Install Merlin with the developer dependencies:

.. code-block:: bash

    $ make install-dev

This will create a virtualenv, start it, and install Merlin and it's dependencies for you.

More documentation about using Virtualenvs with Merlin can be found at
:doc:`Using Virtualenvs with Merlin <./virtualenv>`.

We can make sure it's installed by running:

.. code-block:: bash

    $ merlin --version

If you don't see a version number, you may need to restart your virtualenv and try again. 

Configuring Merlin
*******************

Once Merlin has been installed, the installation needs to be configured.
Documentation for merlin configuration is in the :doc:`Configuring Merlin <./merlin_config>` section.

That's it. To start running Merlin see the :doc:`Merlin Workflows. <./merlin_workflows>`

(Optional) Testing Merlin
*************************

.. warning::

    With python 3.6 you may see some tests fail and a unicode error presented. To fix this, you need to reset the LC_ALL environment variable to en_US.utf8.

If you have ``make`` installed and the `Merlin repository <https://github.com/LLNL/merlin/>`_ cloned, you can run the test suite provided in the Makefile by running:

.. code-block:: bash

    $ make tests

This will run both the unit tests suite and the end-to-end tests suite.

If you'd just like to run the unit tests you can run:

.. code-block:: bash

    $ make unit-tests

Similarly, if you'd just like to run the end-to-end tests you can run:

.. code-block:: bash

    $ make e2e-tests

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
