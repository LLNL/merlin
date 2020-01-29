Celery
======

Merlin uses `Celery <http://www.celeryproject.org>`_, a Python based
distributed task management system. Merlin uses Celery to queue work which
is processed by Celery workers.

Merlin queues tasks to the broker which receives and routes tasks. Merlin by
default is configured to use `RabbitMQ <https://www.rabbitmq.com/>`_.

Celery has many functions, it defines the interface to the task broker, the
backend results database and the workers that will run the tasks.

The broker and backend are configured through the app.yaml file. A
configuration for the rabbit ampq server is shown below.

.. literalinclude:: app_config/app_amqp.yaml


The default location for the app.yaml is in the merlin repo under the
config directory. This default can be overridden by files in one of two
other locations. The current working directory is first checked for the
app.yaml file, then the user's ``~/.merlin`` directory is checked.

The celery command needs application configuration for the specific module that
includes celery, this is specified using the ``-A <module>`` syntax. All celery
commands should include the ``-A`` argument.

.. code-block:: python

  celery -A merlin

The merlin run command will define the tasks from the steps in the yaml file
and then send them to the broker through the celery broker interface. If these
tasks are no longer needed or are incorrect, they can be purged by using one of
these commands:

.. code-block:: python

  celery -A merlin -Q <queue list> purge
  # This is the equivalent of merlin purge <yaml file>
  e.g.
  celery -A merlin -Q merlin,queue2,queue3 purge

  or with rabbitmq:
  celery -A merlin amqp queue.purge <queue name>
  e.g.
  celery -A merlin amqp queue.purge merlin

  a third option with rabbitmq is deleting the queue
  celery -A merlin amqp queue.delete <queue>
  e.g.
  celery -A merlin amqp queue.delete merlin


.. _celery-config:

Configuring celery workers
__________________________

The common configurations used for the celery workers in the
`celery workers  guide <https://docs.celeryproject.org/en/latest/userguide/workers.html>`_
are not the best for HPC applications. Here are some parameters you
may want to use for HPC specific workflows.

These options can be altered by setting the args for an entry of type
worker in the merlin resources section.

The number of threads to use on each node of the HPC allocation is set
through the ``--concurrency`` keyword. A good choice for this is the number of
simulations that can be run per node.

.. code-block:: bash

  celery -A merlin worker --concurrency <num threads>

  e.g.
  # If the HPC simulation is a simple 1D short running sim
  # then on Lassen you might want to use all Hardware threads.
  celery -A merlin worker --concurrency 160

  # If the HPC simulation will take the whole node you may want
  # to limit this to only a few threads.
  celery -A merlin worker --concurrency 2


The ``--prefetch-multiplier`` argument sets how many tasks are requested from
the task server per worker thread.  If ``--concurrency`` is 2 and
``--prefetch-multiplier`` is 3, then 6 tasks will be requested from the task
server by the worker threads. Since HPC tasks are generally not short
running tasks, the recommendation is to set this to 1.

.. code-block:: bash

  celery -A merlin worker --prefetch-multiplier <num_tasks>
  e.g.
  celery -A merlin worker --prefetch-multiplier 1

The ``-O fair`` option is another parameter used for long running celery
tasks.  With this set, celery will only send tasks to threads that are
available to run them.

.. code-block:: bash

  celery -A merlin worker -O fair

The ``-n`` option allows the workers to be given a unique name so multiple
workers running tasks from different queues may share the allocation
resources. The names are automatically set to ``<queue name>.%h``, where
``<queue name>`` is from the task_queue config or merlin (default) and ``%h``
will resolve to the hostname of the compute node.

.. code-block:: bash

  celery -A merlin worker -n <name>
  e.g.
  celery -A merlin worker -n merlin.%h
  or
  celery -A merlin worker -n queue_1.%h

On the toss3 nodes, the CPU affinity can be set for the worker processes.
This is enabled by setting the environment variable ``CELERY_AFFINIITY`` to the
number of CPUs to skip.
e.g. ``export CELERY_AFFINIITY=4``
This will skip 4 CPUs between each celery worker thread.
