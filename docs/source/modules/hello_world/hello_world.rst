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

* description
* batch
* env
* study
* global.parameters
* merlin

Try it!
+++++++

We'll call our spec ``hello.yaml``. Go ahead and make this file, pasting in this text:

.. literalinclude :: simple_chain.yaml
   :language: yaml

