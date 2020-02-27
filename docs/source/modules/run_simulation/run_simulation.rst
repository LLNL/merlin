Run a Real Simulation
=====================
.. admonition:: Prerequisites

      * :doc:`Module 0: Prerequisites<../prereqs>`
      * :doc:`Module 2: Installation<../installation>`
      * :doc:`Module 3: Hello World<../hello_world/hello_world>`

.. admonition:: Estimated time

      * 60 minutes

.. admonition:: You will learn

      * How to run the simulation OpenFOAM, using merlin.
      * How to use machine learning on OpenFOAM results, using merlin.

.. contents::
  :local:

Setting Up
++++++++++

Merlin
~~~~~~
We will need to activate the merlin virtual environment created in :doc:`Module 2: Installation<installation>`

.. code:: bash

  source merlin_venv/bin/activate

Configuring redis
~~~~~~~~~~~~~~~~~
When that is done, we will need to set up the redis server using docker.
This is done by using this command:

.. code:: bash

  docker run --detach --name my-redis -p 6379:6379 redis
  merlin config --broker redis

This sets up the redis server using a docker container without the hassle of
downloading the tar file and making it.

Copying the Module Scripts
~~~~~~~~~~~~~~~~~~~~~~~~~~
This module contains scripts that are expected to be cloned in order to avoid
confusion and debugging non-merlin items. All the required for this module are
available in the merlin github repository.

.. code:: bash

  cp -r merlin/docs/source/modules/run_simulation .

Specification File
++++++++++++++++++

This module aims to do a parameter study on a  well-known benchmark problem for
viscous incompressible fluid flow. We will be setting up our inputs, running
multiple simulations in parallel, combining the outputs, and finally doing some
predictive modeling and visualization using the outputs of these runs.

Setting Up
~~~~~~~~~~~~~~~~~~
It is always recommended to copy the scripts from your SPECROOT into the MERLIN_INFO
file in case you change one of the scripts while merlin is running so we will do that first.

We will also need to download some python packages such as Ofpp and scikit-learn in
order to run this module.

Finally we will need to copy the lid driven cavity deck from the openfoam docker
container and adjust the write controls. This last part is scripted already for convenience.

This is how the step should look like by the end:

.. code:: yaml

  study:
    - name: setup
      description: |
                Installs necessary python packages and copies scripts from SPECROOT
                to the merlin_info directory
      run:
        cmd: |
          cp -r $(SPECROOT)/scripts $(MERLIN_INFO)/

          pip install -r $(SPECROOT)/requirements.txt

          # Set up the cavity directory in the MERLIN_INFO directory
          source $(SCRIPTS)/cavity_setup.sh $(MERLIN_INFO)
        task_queue: setupworkers


Running the Simulation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is where we specify the input parameters and run each of the simulations.

Sample Generation
#################
Just like in :doc:`Module 2: Installation<installation>`, we
