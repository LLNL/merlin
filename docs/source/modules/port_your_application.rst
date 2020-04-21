Port Your Own Application
=========================
.. admonition:: Prerequisites

      * :doc:`Module 2: Installation<installation/installation>`
      * :doc:`Module 3: Hello World<hello_world/hello_world>`
      * :doc:`Module 4: Running a Real Simulation<run_simulation/run_simulation>`

.. admonition:: Estimated time

      * 15 minutes

.. admonition:: You will learn

      * Tips for building workflows
      * Tips for scaling
      * Debugging

.. contents:: Table of Contents:
  :local:


Tips for porting your app, building workflows
+++++++++++++++++++++++++++++++++++++++++++++

The first step of building a new workflow, or porting an existing app to a workflow, is to describe it as a set of discrete, and ideally focused steps.  Decoupling the steps and making them generic when possible will facilitate more rapid composition of future workflows.  This will also require mapping out the dependencies and parameters that get passed between/shared across these steps.

Setting up a template using tools such as `cookiecutter <https://github.com/cookiecutter/cookiecutter/>`_ can be useful for more production style workflows that will be frequently reused.  Additionally, make use of the built-in examples accessible from the merlin command line with ``merlin example``.
.. (machine learning applications on different data sets?)

Use dry runs ``merlin run --dry --local`` to prototype without actually populating task broker's queues.  Similarly, once the dry run prototype looks good, try it on a small number of parameters before throwing millions at it.

Merlin inherits much of the input language and workflow specification philosophy from `Maestro <https://github.com/LLNL/maestrowf/>`_.  Thus a good first step is to learn to use that tool.  As seen in the :doc:`Module 5: Advanced Topics<advanced_topics/advanced_topics>` there are also use cases that combine Merlin and Maestro.

.. send signal to workers <at, before?> alloc ends    -> what was this referring to?
   
Make use of exit keys such as ``MERLIN_RESTART`` or ``MERLIN_RETRY`` in your step logic.

Tips for debugging your workflows
+++++++++++++++++++++++++++++++++

The scripts defined in the workflow steps are also written to the output directories; this is a useful debugging tool as it can both catch parameter and variable replacement errors, as well as providing a quick way to reproduce, edit, and retry the step offline before fixing the step in the workflow specification.  The ``<stepname>.out`` and ``<stepname>.err`` files log all of the output to catch any runtime errors.  Additionally, you may need to grep for ``'WARNING'`` and ``'ERROR'`` in the worker logs.

.. where are the worker logs, and what might show up there that .out and .err won't see? -> these more developer focused output?

When a bug crops up in a running study with many parameters, there are a few other commands to make use of.  Rather than trying to spam ``Ctrl-c`` to kill all the workers, you will want to instead use ``merlin stop-workers <workflow_name>.yaml`` to stop the workers.  This should then be followed up with ``merlin purge <workflow_name>.yaml`` to clear out the task queue to prevent the same
buggy tasks from continuing to run the next time ``run-workers`` is invoked.

.. last item from board: use merlin status to see if have workers ... is that 'dangling tasks' in the image?

Tips for scaling workflows
++++++++++++++++++++++++++

Most of the worst bottlenecks you'll encounter when scaling up your workflow are caused by the file system.  This can be caused by using too much space or too many files, even in a single workflow if you're not careful.  There is a certain number of inodes created just based upon the sample counts even without accounting for the steps being executed.  This can be mitigated by avoiding reading/writing to the file system when possible.  If file creation is unavoidable, you may need to consider adding cleanup steps to your workflow: dynamically pack up the previous step in a tarball, transfer to another file system or archival system, or even just delete files. 

.. Making a temporary directory to run the main app in can be helpful for containing voluminous outputs and cleaning it up without risking any of the <nomenclature for the .out, .err files, shell script, ...?>

Misc tips
+++++++++

Avoid reliance upon storage at the ``$(SPECROOT)`` level.  This is particularly dangerous if using symlinks as it can violate the provenance of what was run, possibly ruining the utility of the dataset that was generated.  It is preferred to make local copies of any input decks and supporting scripts and data sets inside the workflows' workspace.  This of course has limits, regarding shared/system libraries that any programs running in the steps may need; alternate means of recording this information in a log file or something similar may be needed in this case.


.. some other lines on the board that are hard to read..
   run your sim as ...
   (mu !) p...    -> need some other eyes on what that's supposed to be in image of notes
   
.. standard data format discussion?  hdf5?
   this something we should be in the business of recommending?  a lot will be dictated by what the 'big app' is doing anyway...
