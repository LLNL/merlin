Configuration
=============

This section provides documentation for configuring Merlin's connections with task servers and results backends.


Merlin server configuration
---------------------------

Merlin works best configuring celery to run with a RabbitMQ_ broker and a redis_ 
backend. Merlin uses celery chords which require a results backend be configured. The 
Amqp (rpc Rabbitmq) server does not support chords but the Redis, Database, Memcached 
and more, support chords.

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


Broker: ``rabbitmq``, ``amqps``, ``amqp``
-----------------------------------------
Merlin constructs the following connection string from the relevant options in the 
``broker`` section of the app.yaml file. If the ``port`` argument is not defined,
the default rabbitmq TLS port, 5671,  will be used. See the :ref:`broker_rabbitmq_ssl`
section for more info about security with this broker. When the ``broker``
is ``amqp``, the default port will be 5672.


| The prototype url for this configuration is:
| ``{conn}://{username}:{password}@{server}:{port}/{vhost}``

Here ``conn`` is ``amqps`` (with ssl) when ``name`` is ``rabbitmq`` or ``amqps`` and
``amqp`` (without ssl) when name is ``amqp``.

::

  broker:
    name: rabbitmq
    #username: # defaults to your username unless changed here
    password: ~/.merlin/rabbit-password
    # server URL
    server: server.domain.com
    #vhost: # defaults to your username unless changed here

Broker: ``redis``
-----------------
Merlin constructs the following connection string from the relevant options in the `broker` section of the app.yaml file.

| The prototype url for this configuration is:
| ``redis://:{password}@{server}:{port}/{db_num}``

::

  broker:
    name: redis
    server: localhost
    port: 6379

Broker: ``rediss``
------------------
Newer versions of Redis (version 6 or greater) can be configured with ssl. The
``rediss`` name is used to enable this support. See the :ref:`broker_redis_ssl`
section for more info.

| The prototype url for this configuration is:
| ``rediss://:{password}@{server}:{port}/{db_num}``

::

  broker:
    name: rediss
    server: localhost
    port: 6379

Broker: ``redis+socket``
------------------------
Merlin constructs the following connection string from the relevant options in the 
``broker`` section of the app.yaml file.

| The prototype url for this configuration is:
| ``redis+socket://{path}?virtual_host={db_num}``

::

  broker:
    name: redis+socket
    path: /tmp/username/redis.sock
    db_num: 0

Broker: ``url``
---------------

A ``url`` option is available to specify the broker connection url, in this
case the server name is ignored. The url must include all 
the entire connection url except the ssl if the broker name is recognized 
by the ssl processing system.
Currently the ssl system will only configure the Rabbitmq and Redis
servers.

| The prototype url for this configuration is:
| ``{url}``

::

  broker:
    url: redis://localhost:6379/0

Broker: Security
----------------

.. _broker_rabbitmq_ssl:

Security with RabbitMQ_
_______________________

Merlin can only be configured to communicate with RabbitMQ_ over an SSL connection and 
does not permit use of a RabbitMQ_ server configured_ without SSL. The default value 
of the broker_use_ssl keyword is True. The keys can be given in the broker config as
show below.


.. _configured : https://www.rabbitmq.com/ssl.html

::

  broker:
    # can be redis, redis+sock, or rabbitmq
    name: rabbitmq
    #username: # defaults to your username unless changed here
    password: ~/.merlin/rabbit-password
    # server URL
    server: server.domain.com

    ### for rabbitmq, redis+sock connections ###
    #vhost: # defaults to your username unless changed here

    # ssl security
    keyfile: /var/ssl/private/client-key.pem
    certfile: /var/ssl/amqp-server-cert.pem
    ca_certs: /var/ssl/myca.pem
    # This is optional and can be required, optional or none
    # (required is the default)
    cert_req: required



This results in a value for broker_use_ssl given below:

::

  broker_use_ssl = {
  'keyfile': '/var/ssl/private/client-key.pem',
  'certfile': '/var/ssl/amqp-server-cert.pem',
  'ca_certs': '/var/ssl/myca.pem',
  'cert_reqs': ssl.CERT_REQUIRED
  }


.. _broker_redis_ssl:

Security with redis_
____________________


The same ssl config and resulting ssl_use_broker can be used with the ``rediss://``
url when using a redis server version 6 or greater with ssl_.

.. _ssl : https://redis.io/topics/encryption

::

  broker:
    name: rediss
    #username: # defaults to your username unless changed here
    # server URL
    server: server.domain.com
    port: 6379
    db_num: 0


    # ssl security
    keyfile: /var/ssl/private/client-key.pem
    certfile: /var/ssl/amqp-server-cert.pem
    ca_certs: /var/ssl/myca.pem
    # This is optional and can be required, optional or none
    # (required is the default)
    cert_req: required


The resulting ``broker_use_ssl`` configuration for a ``rediss`` server is given below.

::

  broker_use_ssl = {
  'ssl_keyfile': '/var/ssl/private/client-key.pem',
  'ssl_certfile': '/var/ssl/amqp-server-cert.pem',
  'ssl_ca_certs': '/var/ssl/myca.pem',
  'ssl_cert_reqs': ssl.CERT_REQUIRED
  }



Results backend: ``redis``
--------------------------
Merlin constructs the following connection string from relevant options in the 
`results_backend` section of the app.yaml file.

| The prototype url for this configuration is:
| ``redis://:{password}{server}:{port}/{db_num}``

::

  results_backend:
    name: redis
    server: localhost
    port: 6379

Results backend: ``rediss``
---------------------------
Newer versions of Redis (version 6 or greater) can be configured with ssl. The
``rediss`` name is used to enable this support. See the :ref:`results_redis_ssl`
section for more info.

| The prototype url for this configuration is:
| ``rediss://:{password}{server}:{port}/{db_num}``

::

  results_backend:
    name: rediss
    server: localhost
    port: 6379

Results backend: ``url``
------------------------

A ``url`` option is available to specify the results connection url, in this
case the server name is ignored. The url must include the entire connection url
including the ssl configuration.

| The prototype url for this configuration is:
| ``{url}``

::

  results_backend:
    url: redis://localhost:6379/0

The ``url`` option can also be used to define a server that is not explicitly
handled by the merlin configuration system.

::

  results_backend:
    url: db+postgresql://scott:tiger@localhost/mydatabase

Resolving password fields
_________________________

The ``results_backend/password`` is interpreted in the following way. First, it is treated as an absolute path to a file containing your backend
password. If that path doesn't exist, it then looks for a file of that name under the  directory defined under ``celery/certs``. If that file doesn't
exist, it then looks treats ``results_backend/password`` as the password itself.

The ``broker/password`` is simply the full path to a file containing your password for the user defined by ``broker/username``.

Results backend: Security
-------------------------

.. _results_redis_ssl:

Security with redis_
____________________

Redis versions less than 6 do not natively support multiple users or SSL. We address security concerns here by redefining the core celery routine that communicates with
redis to encrypt all data it sends to redis and then decrypt anything it receives. Each user should have their own encryption key as defined by 
``results_backend/encryption_key`` in the app.yaml file. Merlin will generate a key if that key does not yet exist.

Redis versions 6 or greater can use the ssl keys as in the broker section. The ssl
config with redis (``rediss``) in the results backend is the placed in the 
``redis_backend_use_ssl`` celery argument.
The values in this argument are the same as the broker.

::

  redis_backend_use_ssl = {
  'ssl_keyfile': '/var/ssl/private/client-key.pem',
  'ssl_certfile': '/var/ssl/amqp-server-cert.pem',
  'ssl_ca_certs': '/var/ssl/myca.pem',
  'ssl_cert_reqs': ssl.CERT_REQUIRED
  }
