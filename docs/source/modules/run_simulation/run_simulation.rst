Run a Real Simulation
=====================
.. admonition:: Prerequisites

      * :doc:`Module 0: Before you come<../before>`
      * :doc:`Module 2: Installation<../installation>`
      * :doc:`Module 3: Hello World<../hello_world/hello_world>`

.. admonition:: Estimated time

      * 60 minutes

.. admonition:: You will learn

      * How to run the simulation OpenFOAM, using merlin.
      * How to use machine learning on OpenFOAM results, using merlin.

.. contents::
  :local:

Setup redis
+++++++++++

.. Merlin
 ~~~~~~
 We will need to activate the merlin virtual environment created in :doc:`Module 2: Installation<installation>`

.. .. code:: bash

.. source merlin_venv/bin/activate

.. Configuring redis
 ~~~~~~~~~~~~~~~~~
We will need to set up the redis server using a docker container.
This removes the hassle of downloading and making the redis tar file.
Run:

.. code:: bash

    docker run --detach --name my-redis -p 6379:6379 redis

Now configure merlin for redis with:

.. code:: bash

    merlin config --broker redis

Specification File
++++++++++++++++++

This module aims to do a parameter study on a well-known benchmark problem for
viscous incompressible fluid flow. We will be setting up our inputs, running
multiple simulations in parallel, combining the outputs, and finally doing some
predictive modeling and visualization using the outputs of these runs.

Samples and Scripts
~~~~~~~~~~~~~~~~~~~
It is always recommended to copy the scripts from your SPECROOT into the MERLIN_INFO
file in case you change one of the scripts while merlin is running so we will do
that first. We will put this in the merlin block since it runs before anything
else along with the sample generation.

Just like in the :ref:`Using Samples` step of the previous module, we will be
generating samples using the merlin block. We are only concerned with how the
variation of two initial conditions affects outputs of the system. The
make_samples script is designed to make log uniform random samples and the column
labels are appropriately assigned.

First we will need to specify some variables to make our life easier:

.. code:: yaml

  env:
      variables:
          OUTPUT_PATH: ./openfoam_wf_output
          SCRIPTS: $(MERLIN_INFO)/scripts

          N_SAMPLES: 10

The merlin block should look like the following

.. code:: yaml

  merlin:
      samples:
          generate:
              cmd: |
                  cp -r $(SPECROOT)/scripts $(MERLIN_INFO)/
                  python $(SCRIPTS)/make_samples.py -n $(N_SAMPLES) -outfile=$(MERLIN_INFO)/samples
          file: $(MERLIN_INFO)/samples.npy
          column_labels: [LID_SPEED, VISCOSITY]

After this block we can move on to starting the steps in our study.

Setting Up
~~~~~~~~~~
We will need to download some python packages such as Ofpp and scikit-learn in
order to run this module. They are currently in the requirements.txt file.

We will also need to copy the lid driven cavity deck from the openfoam docker
container and adjust the write controls. This last part is scripted already for convenience.

This is how the step should look like by the end:

.. code:: yaml

  study:
    - name: setup
      description: |
                Installs necessary python packages and imports the cavity directory
                from the docker container
      run:
        cmd: |
          pip install -r $(SPECROOT)/requirements.txt

          # Set up the cavity directory in the MERLIN_INFO directory
          source $(SCRIPTS)/cavity_setup.sh $(MERLIN_INFO)
        task_queue: setupworkers


Running the Simulation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is where we specify the input parameters and run each of the simulations.
For OpenFOAM, we simply need to change the values in each of the files related
to Lidspeed and Viscosity. We then utilize the OpenFOAM docker image to run each
of these input parameters locally.
