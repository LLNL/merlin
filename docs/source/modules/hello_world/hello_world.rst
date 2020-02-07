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

Try it!
+++++++

We'll call our spec ``hello.yaml``. On the command line, run:

.. code:: bash

    $ merlin run hello.yaml

You should see something like this:

...

That means we have launched our tasks...
