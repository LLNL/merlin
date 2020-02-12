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

Stuff inside a specification
++++++++++++++++++++++++++++

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
            values : ["hello","bonjour"]
            label  : GREET.%%
        WORLD:
            values : ["world","monde"]
            label  : WORLD.%%

So this will give us an English result, and a French one.

study
~~~~~
This is where you define worfklow steps.

.. code:: yaml

    study:
        - name: step_1
          description: step 1
          run:
              cmd: |
                  touch "$(GREET), world!"

        - name: step_2
          description: look at the files in step_1
          run:
              cmd: |
                  ls $(step_1.workspace)
              depends: [step_1]

``$(GREET)`` expands the global parameter ``GREET`` seperately into its two values.
``$(step_1.workspace)`` gets the path to step_1.
Steps must be defined as a DAG, so no cyclical dependencies are allowed.
Our step DAG currently looks like this:

.. image:: dag1.png
    :width: 100
    :align: center



Your complete hello world spec should look like this:

.. literalinclude:: hello.yaml
   :language: yaml

We'll name it ``hello.yaml``.
The order of the spec sections doesn't matter.

.. note::

    At this point, our spec is both merlin- and maestro-compatible. The primary difference is that maestro won't understand anything in the ``merlin`` block, which we will add later. If you want to try it, run: ``$ maestro run hello.yaml``

Try it!
+++++++

First, we'll run merlin locally. On the command line, run:

.. code:: bash

    $ merlin run --local hello.yaml

If your spec is bugless, you should see a few messages proclaiming successful step completion, like this (for now we'll ignore the warning):

.. literalinclude :: local_out.txt
    :language: text

Great! But what happened? We can inspect the output directory to find out.

Look for a directory named ``hello_world_workflow_<TIMESTAMP>``. That's your output directory.
Within, there should be a directory for each step of the workflow, plus one called ``merlin_info``.
The whole file tree looks like this:

.. image:: fig1.png

A lot of stuff, right? Here's what it means:

* The yaml file inside ``merlin_info/`` is called the provenance spec. It's a copy of the original spec that was run.

* ``MERLIN_FINISHED`` files indicate that the step ran successfully.

* ``.sh`` files contain the command for the step.

* ``.out`` files contain the step's stdout.

* ``.err`` files contain the step's stderr.

.. Assuming config is ready
Run distributed!
++++++++++++++++

.. important::

    Before trying this, make sure you've properly set up your merlin config file ``app.yaml``. Run ``merlin info`` for information on your merlin configuration.

Now we will run the same workflow, but on our task server:

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

Add samples
+++++++++++

< add merlin section to spec >

< add a make_samples.py script >

< change to 1000 samples >

Miscellany
++++++++++

.. ?
< merlin stop-workers > 

< merlin --help >
