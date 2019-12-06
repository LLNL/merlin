Getting Started
================

Quick Start
++++++++++++++
::
    pip3 install merlinwf

Developer Setup
++++++++++++++++++

This setup will work in most cases and can be used to quickly setup Merlin.

Clone the `Merlin <https://github.com/LLNL/merlin.git>`_
repository. See the :doc:`Spack <./spack>` section for an alternative method to setup merlin.

::

    git clone https://github.com/LLNL/merlin.git

Then run the following::

   cd merlin
   make all

for a different python3 version::

   cd merlin
   make PYTHON=python3-3.7.2 all

The Makefile should have created a virtualenv and installed all required
dependencies.

Activate the virtualenv::

    $ source venv_merlin_<system>_py<version>/bin/activate  # Or activate.csh if using .cshrc
    # The prompt will be merlin<python major version>_<python minor version>
    # This can be changed by setting the PENV variable in the Makefile.
    (merlin3_7) $

    with python 3.7.* this will be,
    $ source venv_merlin_${SYS_TYPE}_py3_7/bin/activate

.. note:: The ${SYS_TYPE} variable may not be defined on your machine.

.. note:: A virtualenv may only be created for the current system. This virtualenv
   setup process must be repeated for each different system being run on.

See the :doc:`Merlin Commands <./merlin_commands>` section for using merlin.

To exit the virtualenv::

    (merlin3_7) $ deactivate
    $

Configuring Merlin
*******************

Once Merlin has been installed, the installation needs to be configured.
Documentiation for merlin configuration is in the :doc:`Configuring Merlin <./merlin_config>` section. 

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
*********************

Merlin uses `virtualenvs <https://virtualenv.pypa.io/en/stable/>`_ to manage
package dependencies which can be installed via Pip, Python's default 
package manager.

More documentation about using Virtualenvs with Merlin can be found at
:doc:`Using Virtualenvs with Merlin <./virtualenv>`.

To create a new virtualenv and activate it::

    $ python3 -m venv venv_merlin_$SYS_TYPE_py3_6
    $ source venv_merlin_$SYS_TYPE_py3/bin/activate  # Or activate.csh for .cshrc


Install Python Package Dependencies
************************************

Merlin uses Pip to manage Python dependencies. Merlin dependencies can be
found in the requirements directory in the Merlin repository.

To install the standard set of dependencies run::

    (merlin3_7) $ pip install -r requirements.txt

This will install all the required dependencies for Merlin and development
development dependencies.


Installing Merlin
*******************

Merlin can be installed in editable mode. From within the Merlin repository::

    (merlin3_7) $ pip install -e .

Any changes made to the Merlin source code should automatically reflect in the
virtualenv.

.. tip:: If changes to Merlin's source code do not reflect when running Merlin
    try running `pip install -e .` from within the Merlin repository.
