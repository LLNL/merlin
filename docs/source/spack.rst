Spack
=====

The virtualenv method is not the only method to install merlin in a
separate python install.  The spack method will build python and
all required modules for a specific set of configuration options.
These options include the compiler version, system type and python version.
Merlin will then be installed in this specific version allowing for
multiple python versions on a single system without the need for a
virtualenv. The py-merlin package builds with python3.6+.


Checkout spack
**************


Get the latest version of spack from Github. This is independent from 
merlin so make sure merlin and spack are in separate directories.

.. code:: bash

  git clone https://github.com/spack/spack.git
  # The merlin spack package is in the develop branch
  git checkout develop


Setup spack
***********

cd to spack directory

Source the ``setup-env.sh`` or ``setup-env.csh``. This will put spack in
your path and setup module access for later use. This should be done every
time the modules are used.

.. code:: bash

    source ./share/spack/setup-env.sh

Add compilers if you haven't already:

.. code:: bash

    spack compiler add

To see the compilers.

.. code:: bash

    spack compiler list


Build merlin
************

Build merlin, this will take a *long* time, be prepared to wait.  It will
build python and all python modules merlin needs including numpy.

.. code:: bash

    spack install py-merlin


The build will be done with the default compiler, in general this is the 
newest gcc compiler. You can choose a different compiler by using the ``%``
syntax, this will create an entirely separate build and module.

.. code:: bash

    spack install py-merlin%gcc@7.1.0


A different python version can be specified as part of the package config. 
To build merlin with python-3.6.8 you would type:

.. code:: bash

    spack install py-merlin^python@3.6.8

A tree of all of the packages and their dependencies needed to build the
merlin package can be shown by using the spec keyword.

.. code:: bash

    spack spec py-merlin


Activate merlin
***************

To use merlin you can activate the module.

.. code:: bash

    spack activate py-merlin

    or

    spack activate py-merlin%gcc@7.1.0

    or

    spack activate py-merlin^python@3.6.8


Load python
***********

The associated python module can then be loaded into your environment, this
will only work if you have sourced the setup-env.sh or setup-env.csh.

.. code:: bash

    module avail python

    example:
    ------ <path to>/spack/share/spack/modules/linux-rhel7-x86_64 -------
       python-3.6.8-gcc-8.1.0-4ilk3kn (L)


This will give you a list, the spack version will have a long hash
associated with the name.

.. code:: bash

    module load python-3.6.8-<compiler>-<hash>
    e.g.
    module load python-3.6.8-gcc-8.1.0-4ilk3kn

At this point the module specific python, merlin, maestro and celery will 
all be in your path.
