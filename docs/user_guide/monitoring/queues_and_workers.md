# Queue Information

Merlin provides users with the [`merlin queue-info`](../command_line.md#queue-info-merlin-queue-info) command to help monitor celery queues. This command will list queue statistics in a table format where the columns are as follows: queue name, number of tasks in the queue, number of workers connected to the queue.

The default functionality of this command is to display queue statistics for active queues. Active queues are any queues that either have a worker watching them, tasks in the queue, or both.

**Usage:**

```bash
merlin queue-info
```

Example output with no active queues:

.. figure:: ../images/queue-info/no-active-queues.png
  :alt: the queue-info output when no queues are active

  The output of the queue-info command when no queues are active

Example output with active queues:

.. figure:: ../images/queue-info/active-queues.png
  :alt: the queue-info output when there are active queues

  The output of the queue-info command when there are active queues

If you know exactly what queues you want to check on, you can use the `--specific-queues` option to list
one or more queues to view.

Usage:

.. code-block:: bash

  merlin queue-info --specific-queues <queue name(s)>

Example:

.. figure:: ../images/queue-info/specific-queues-active.png
  :alt: the queue-info output using the specific-queues option with active queues

  The output when using the specific-queues option to query two active queues named "hello_queue" and "goodbye_queue"

If you ask for queue-info of inactive queues with the `--specific-queues` option, a table format will still
be output for you.

Example:

.. figure:: ../images/queue-info/specific-queues-inactive.png
  :alt: the queue-info output using the specific-queues option with inactive queues

  The output when using the specific-queues option to query two inactive queues named "hello_queue" and "goodbye_queue"

To modify the task server from the command line you can use the `--task-server` option. However, the only currently available
option for task server is celery so you most likely will not want to use this option.

## Specification Options

There are three options that revolve around using a spec file to query queue information: `--spec`, `--steps`,
and `--vars`.

.. note::

  The `--steps` and `--vars` options MUST be used alongside the `--spec` option. They CANNOT be used by themselves.

Using the `--spec` option allows you to query queue statistics for queues that only exist in the spec file you provide.
This is the same functionality as the `merlin status` command prior to the release of Merlin v1.11.0.

Usage:

.. code-block:: bash

  merlin queue-info --spec <spec file>

Example:

.. figure:: ../images/queue-info/specification-option.png
  :alt: output of the queue-info command using the specification option

  Output of the queue-info command using the specification option

If you'd like to see queue information for queues that are attached to specific steps in your workflow, use the `--steps` option.
This option MUST be used alongside the `--spec` option.

Usage:

.. code-block:: bash

  merlin queue-info --spec <spec file> --steps <step name(s)>

Say I have a spec file with steps named `step_1` through `step_4` and each step is attached to a different queue `step_1_queue` through
`step_4_queue` respectively. Using the `--steps` option for these two steps gives us:

.. figure:: ../images/queue-info/steps-option.png
  :alt: output of queue-info using the steps option

  Output of the queue-info command using the steps option to query steps named step_1 and step_4 of a workflow

The `--vars` option can be used to modify any variables defined in your spec file from the CLI. This option MUST be used alongside the
`--spec` option. The list is space-delimited and should be given after the input yaml file.

Usage:

.. code-block:: bash

  merlin queue-info --spec <spec file> --vars QUEUE_VAR=new_queue_var_value

## Dumping Queue Info to Output Files

Much like the two status commands, the `queue-info` command provides a way to dump the queue statistics to an output file.

Example CSV dump:

.. code-block:: bash

  merlin queue-info --dump queue-info.csv

When dumping to a file that DOES NOT yet exist, Merlin will create that file for you and populate it with the queue statistics you requested.

When dumping to a file that DOES exist, Merlin will append the requested queue statistics to that file. You can differentiate between separate
dump calls by looking at the timestamps of the dumps. For CSV files this timestamp exists in the "Time" column (see
:ref:`Queue Info CSV Dump Format` below) and for JSON files this timestamp will be the top level key to the queue info entry (see
:ref:`Queue Info JSON Dump Format` below).

Using any of the `--specific-steps`, `--spec`, or `--steps` options will modify the output that's written to the output file.

### Queue Info CSV Dump Format

The format of a CSV dump file for queue information is as follows:

.. code-block::

  Time,[merlin]_<queue_name>:tasks,[merlin]_<queue_name>:consumers

The <queue_name>:tasks and <queue_name>:consumers columns will be created for each queue that's listed in the queue-info output at the time of
your dump.

The image below shows an example of dumping the queue statistics of active queues to a csv file, and then displaying that csv file using
the `rich-cli library <https://github.com/Textualize/rich-cli>`_:

.. figure:: ../images/queue-info/dump-csv.png
  :alt: example of dumping queue info to a csv file and outputting it's contents

  An example showcasing how to do a csv dump of active queue statistics and view it's contents

### Queue Info JSON Dump Format

The format of a JSON dump file for queue information is as follows:

.. code-block:: json

  { 
    "YYYY-MM-DD HH:MM:SS": {
      "[merlin]_queue_name": {
        "tasks": 0
        "consumers": 1
      }
    }
  }

The values of the "tasks" and "consumers" fields may differ in your output.

The image below shows an example of dumping the queue info to a json file, and then displaying that json file using
the `rich-cli library <https://github.com/Textualize/rich-cli>`_:

.. figure:: ../images/queue-info/dump-json.png
  :alt: example of dumping to a json file and outputting it's contents

  An example showcasing how to do json dump of active queue statistics and view it's contents
