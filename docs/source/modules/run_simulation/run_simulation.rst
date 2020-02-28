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

This module aims to do a parameter study on a well-known benchmark problem for
viscous incompressible fluid flow. We will be setting up our inputs, running
multiple simulations in parallel, combining the outputs, and finally doing some
predictive modeling and visualization using the outputs of these runs.

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

Specification file
++++++++++++++++++

Samples and scripts
~~~~~~~~~~~~~~~~~~~
A best practice for merlin is to copy any scripts your workflow may use from your ``SPECROOT`` directory into the ``MERLIN_INFO``
directory, in case one of the original scripts is modified while merlin is running (which may be a long time).
We will do that first.
We will put this in the merlin sample generation section, since it runs before anything else.

Just like in the :ref:`Using Samples` step of the hello world  module, we will be
generating samples using the merlin block. We are only concerned with how the
variation of two initial conditions, lid speed and viscosity, affects outputs of the system.
These are the ``column_labels``.
The ``make_samples.py`` script is designed to make log uniform random samples.

First we specify some variables to make our life easier:

.. code:: yaml

  env:
      variables:
          OUTPUT_PATH: ./openfoam_wf_output
          SCRIPTS: $(MERLIN_INFO)/scripts
          N_SAMPLES: 10

The merlin block should look like the following:

.. code:: yaml

  merlin:
      samples:
          generate:
              cmd: |
                  cp -r $(SPECROOT)/scripts $(MERLIN_INFO)/
                  python $(SCRIPTS)/make_samples.py -n $(N_SAMPLES) -outfile=$(MERLIN_INFO)/samples
          file: $(MERLIN_INFO)/samples.npy
          column_labels: [LID_SPEED, VISCOSITY]

Now, we can move on the steps of our workflow.

Setting up
~~~~~~~~~~
We will need to download some python packages such as ``Ofpp`` and ``scikit-learn`` in
order to run this module. They are found in the ``requirements.txt`` file.

We will also need to copy the lid driven cavity deck from the openfoam docker
container and adjust the write controls. This last part is scripted already for convenience.

This is how the ``setup`` step should look like by the end:

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



Running the simulation
~~~~~~~~~~~~~~~~~~~~~~
This is where we specify the input parameters and run each of the simulations.
For OpenFOAM, we simply need to change the values in each of the files related
to Lidspeed and Viscosity. We then utilize the OpenFOAM docker image to run each
of these input parameters locally.

The parameters of interest are the Enstrophy and Kinetic Energy at each cell.
The enstrophy is calculated through an OpenFOAM post processing of the

This part should look like:

.. code:: yaml

  - name: sim_runs
    description: |
                Edits the Lidspeed and viscosity then runs OpenFOAM simulation
                using the icoFoam solver
    run:
        cmd: |
            cp -r $(MERLIN_INFO)/cavity cavity/
            cd cavity

            sed -i '' "18s/.*/nu              [0 2 -1 0 0 0 0] $(VISCOSITY);/" constant/transportProperties
            sed -i '' "26s/.*/        value           uniform ($(LID_SPEED) 0 0);/" 0/U

            cd ..
            cp $(SCRIPTS)/run_openfoam .

            CONTAINER_NAME='OPENFOAM_ICO_$(MERLIN_SAMPLE_ID)'
            docker container run -ti --rm -v $(pwd):/cavity -w /cavity --name=${CONTAINER_NAME} cfdengine/openfoam ./run_openfoam $(LID_SPEED)
            docker wait ${CONTAINER_NAME}
        task_queue: simworkers
        depends: [setup]

Combining Outputs, Predictive Modeling, and Visualizing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The following steps combine the outputs from the previous step and outputs it into
 a .npz file for further use in the predictive learning step.
