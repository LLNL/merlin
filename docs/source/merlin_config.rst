Configuration
=============

This section provides documentation for configuring Merlin's connections with task servers and results backends.


Merlin server configuration
---------------------------

Merlin works best configuring celery to run with a RabbitMQ_ broker and a redis_ backend.

.. _redis: https://redis.io/
.. _RabbitMQ: https://www.rabbitmq.com/

Merlin's configuration is controlled by an app.yaml file, such as the one below:

.. literalinclude:: app_config/app_amqp.yaml

The default location for the app.yaml is in the merlin repo under the
merlin/config directory. This default can be overridden by files in one of two
other locations. The current working directory is first checked for the
app.yaml file, then the user's ``~/.merlin`` directory is checked.

``broker/name``: can be ``rabbitmq``, ``redis``, or ``redis+sock``. As their names imply,
``rabbitmq`` will use RabbitMQ_ as a task broker (preferred for multi user configurations),
``redis`` will use redis_ as a task broker, and ``redis+sock`` will connect to a redis_ task broker
using a socket. 


Broker: ``rabbitmq``
--------------------
Merlin constructs the following connection string from the relevant options in the ``broker`` section of the app.yaml file.

``"amqps://{username}:{password}@{server}:5671//{vhost}"``

Security with RabbitMQ_
_______________________

Merlin can only be configured to communicate with RabbitMQ_ over an SSL connection and does not permit use of a RabbitMQ_ server configured_ without
SSL.

.. _configured : https://www.rabbitmq.com/ssl.html

Broker: ``redis``
-----------------
Merlin constructs the following connection string from the relevant options in the `broker` section of the app.yaml file.
``"redis://{password}{server}:{port}/{db_num}"``

::

  broker:
    name: redis
    server: localhost
    port: 6379


Broker: ``redis+socket``
------------------------
Merlin constructs the following connection string from the relevant options in the ``broker`` section of the app.yaml file.
``"redis+socket://{path}?virtual_host={db_num}"``

::

  broker:
    name: redis+socket
    path: /tmp/username/redis.sock
    db_num: 0


Results backend: ``redis``
--------------------------
Merlin constructs the following connection string from relevant options in the `results_backend` section of the app.yaml file.
``"redis://{password}{server}:{port}/{db_num}"``

::

  results_backend:
    name: redis
    server: localhost
    port: 6379


Resolving password fields
_________________________

The ``results_backend/password`` is interpreted in the following way. First, it is treated as an absolute path to a file containing your backend
password. If that path doesn't exist, it then looks for a file of that name under the  directory defined under ``celery/certs``. If that file doesn't
exist, it then looks treats ``results_backend/password`` as the password itself.

The ``broker/password`` is simply the full path to a file containing your password for the user defined by ``broker/username``.

Security with redis_
--------------------

Redis does not natively support multiple users or SSL. We address security concerns here by redefining the core celery routine that communicates with
redis to encrypt all data it sends to redis and then decrypt anything it receives. Each user should have their own encryption key as defined by 
``results_backend/encryption_key`` in the app.yaml file. Merlin will generate a key if that key does not yet exist.



