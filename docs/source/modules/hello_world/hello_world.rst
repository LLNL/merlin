Hello, World!
=============
.. admonition:: Estimated time

      * 30 minutes
    
.. admonition:: You will learn

      * The components of a merlin workflow specification.
      * How to run a simple merlin workflow.
      * How to interpret the results of your workflow.

Stuff inside a specification
++++++++++++++++++++++++++++

Central to Merlin is something called a specifiation file, or spec for short.

The spec is formatted in yaml, and defines all aspects of a workflow.

We will build our spec piece by piece.

description
~~~~~~~~~~~

batch
~~~~~

env
~~~

study
~~~~~

global.parameters
~~~~~~~~~~~~~~~~~

merlin
~~~~~~

Your complete hello world spec should look like this:

.. literalinclude :: simple_chain.yaml
   :language: yaml

We'll call our spec ``hello.yaml``.

Try it!
+++++++

First, we'll run merlin locally. On the command line, run:

.. code:: bash

    $ merlin run --local hello.yaml

You should see something like this:

.. literalinclude :: local_out.txt
    :language: text

< Explain what the output means >

< Look inside the output directories >

.. Assuming config is ready
Run distributed!
++++++++++++++++

Now, we will run the same workflow, but on our task server:

.. code:: bash

    $ merlin run hello.yaml

You should see something like this:

.. literalinclude :: run_out.txt
   :language: text

< That means we have launched our tasks... >

< launch workers >

.. literalinclude :: run_workers_out.txt
   :language: text

< explain >

.. Is this overkill for this section?
Add samples
+++++++++++

< add merlin section to spec >

< add a make_samples.py script >

Miscellany
++++++++++

.. ?
< merlin stop-workers > 

< merlin --help >
