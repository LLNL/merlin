Hello, World!
=============
.. admonition:: Prerequisites

    * :doc:`Module 2: Installation<installation>`

.. admonition:: Estimated time

      * 30 minutes

.. admonition:: You will learn

      * The components of a merlin workflow specification.
      * How to run a simple merlin workflow.
      * How to interpret the results of your workflow.

.. contents::
  :local:

Elements of a specification
+++++++++++++++++++++++++++

Central to Merlin is something called a specifiation file, or a "spec" for short.
The spec defines all aspects of your workflow.
The spec is formatted in yaml (if you're unfamilar with yaml, it's worth reading up on for a few minutes).

Let's build our spec piece by piece.


description
~~~~~~~~~~~
Just what it sounds like. Name and briefly summarize your workflow.

.. code:: yaml

    description:
        name: hello world workflow
        description: say hello in 3 languages

global.parameters
~~~~~~~~~~~~~~~~~
.. better explanation??

Global parameters are constants that you want to vary across simulations.
The whole workflow is run for index of parameter values.

.. code:: yaml

    global.parameters:
        GREET:
            values : ["hello","hola"]
            label  : GREET.%%
        WORLD:
            values : ["world","mundo"]
            label  : WORLD.%%

So this will give us an English result, and a Spanish one (you could add as many more langauges as you want, as long as both parameters hold the same number of values).

study
~~~~~
This is where you define worfklow steps.

.. code:: yaml

    study:
        - name: step_1
          description: say hello
          run:
              cmd: echo "$(GREET), $(WORLD)!"

        - name: step_2
          description: print a success message
          run:
              cmd: print("Hurrah, we did it!")
              depends: [step_1]
              shell: /usr/bin/env python3

``$(GREET)`` and ``$(WORLD)`` expand the global parameters seperately into their two values.
.. ``$(step_1.workspace)`` gets the path to ``step_1``.
The default value for ``shell`` is ``/bin/bash``. In ``step_2`` we override this to use python instead.
Steps must be defined as nodes in a DAG, so no cyclical dependencies are allowed.
Our step DAG currently looks like this:

.. image:: dag1.png
    :width: 100
    :align: center

Since our global parameters have 2 values, this is actually what the DAG looks like:

.. image:: dag2.png
    :width: 300
    :align: center

It looks like running ``step_2`` twice is redundant. Instead of doing that, we can collapse it back into a single step, by having it wait for both parameterized versions of ``step_1`` to finish. Add ``_*`` to the end of the step name in ``step_1``'s depend entry. Go from this:

.. code:: yaml

    depends: [step_1]

...to this:

.. code:: yaml

    depends: [step_1_*]

Now the DAG looks like this:

.. image:: dag3.png
    :width: 300
    :align: center

Your full hello world spec should now look like this:

.. literalinclude:: ../../../../merlin/examples/workflows/hello/hello.yaml
   :language: yaml

We'll name it ``hello.yaml``.
The order of the spec sections doesn't matter.

.. note::

    At this point, our spec is still merlin- and maestro-compatible. The primary difference is that maestro won't understand anything in the ``merlin`` block, which we will still add later. If you want to try it, run: ``$ maestro run hello.yaml``

Try it!
+++++++

First, we'll run merlin locally. On the command line, run:

.. code:: bash

    $ merlin run --local hello.yaml

If your spec is bugless, you should see a few messages proclaiming successful step completion, like this (for now we'll ignore the warning):

.. literalinclude :: local_out.txt
    :language: text

Great! But what happened? We can inspect the output directory to find out.

Look for a directory named ``hello_<TIMESTAMP>``. That's your output directory.
Within, there should be a directory for each step of the workflow, plus one called ``merlin_info``.
The whole file tree looks like this:

.. image:: merlin_output.png
    :align: center

A lot of stuff, right? Here's what it means:

.. * ``new_file.txt`` is the name of the file we wrote in ``step_1``.

* The yaml file inside ``merlin_info/`` is called the provenance spec. It's a copy of the original spec that was run.

* ``MERLIN_FINISHED`` files indicate that the step ran successfully.

* ``.sh`` files contain the command for the step.

* ``.out`` files contain the step's stdout. Look at one of these, and it should contain your "hello" message.

* ``.err`` files contain the step's stderr. Hopefully empty, and useful for debugging.

.. Assuming config is ready

Run distributed!
++++++++++++++++

.. important::

    Before trying this, make sure you've properly set up your merlin config file ``app.yaml``. Run ``$ merlin info`` for information on your merlin configuration.

Now we will run the same workflow, but in parallel on our task server:

.. code:: bash

    $ merlin run hello.yaml

If your merlin configuration is set up correctly, you should see something like this:

.. literalinclude :: run_out.txt
   :language: text

That means we have launched our tasks! Now we need to launch the workers that will complete those tasks. Run this:

.. code:: bash

    $ merlin run-workers hello.yaml

Here's the expected merlin output message for running workers:

.. literalinclude :: run_workers_out.txt
   :language: text

Immediately after that, this will pop up:

.. literalinclude :: celery.txt
   :language: text

The terminal you ran workers in is now being taken over by Celery, the powerful task queue library that merlin uses internally. The workers will continue to report their task status here until their tasks are complete.

.. _Using Samples:

Using samples
+++++++++++++
It's a little boring to say "hello world" in just two different ways. Let's instead say hello to many people!

To do this, we'll need samples. Specifically, we'll change ``WORLD`` from a global parameter to a sample. While parameters are static, samples are generated dynamically, and can be more complex data types. In this case, ``WORLD`` will go from being "world" or "mundo" to being a randomly-generated name.

First, we remove the global parameter ``WORLD``.

Now add these yaml sections to your spec:

.. code:: yaml

    env:
        variables:
            N_SAMPLES: 3

This makes ``N_SAMPLES`` into a user-defined variable that you can use elsewhere in your spec.

.. code:: yaml

    merlin:
        samples:
            generate:
                cmd: pip3 install names ; python3 $(SPECROOT)/make_samples.py --filepath=$(MERLIN_INFO)/samples.csv --number=$(N_SAMPLES)
            file: $(MERLIN_INFO)/samples.csv
            column_labels: [WORLD]

This is the merlin block, an exclusively merlin feature. It provides a way to generate samples for your workflow. In this case, a sample is the name of a person.

For simplicity we give ``column_labels`` the name ``WORLD``, just like before.

It's good practice to shift larger chunks of code to external scripts. At the same location of your spec, make a new file called ``make_samples.py``:

.. literalinclude :: ../../../../merlin/examples/workflows/hello/make_samples.py
   :language: text

Since our environment variable ``N_SAMPLES`` is set to 3, this sample-generating command should churn out 3 different names.

Here's our DAG with samples:

.. image:: dag4.png
    :width: 400
    :align: center

Here's the new spec:

.. literalinclude:: ../../../../merlin/examples/workflows/hello/hello_samples.yaml
   :language: yaml


Run the workflow again!

Once finished, this is what the insides of ``step_1`` look like:

.. image:: merlin_output2.png
    :align: center



* ``sample_index.txt`` keeps track of samples in its directory. Similarly to ``MERLIN_FINISHED``, this is used interally by merlin and doesn't usually require user attention.

* Numerically-named directories like ``0``, ``1``, and ``2`` are sample directories. Instead of storing sample output in a single flattened location, merlin stores them in a tree-like sample index, which helps get around file system constraints when working with massive amounts of data.

Lastly, let's flex merlin's muscle and scale up our workflow to 1000 samples. To do this, you could interally change thevalue in the spec from 3 to 1000. OR you could just this run this:

.. code:: bash

    $ merlin run --vars N_SAMPLES=1000 hello.yaml

    $ merlin run-workers hello.yaml

To send a warm stop signal to your workers, run:

.. code:: bash

    $ merlin stop-workers

Congratulations! You concurrently greeted 1000 friends in English and Spanish!

.. note::

    To get a fresh copy of the specs and script from this module, run ``merlin example hello``.
