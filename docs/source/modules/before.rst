0. Before you start
===================

It will be helpful to have these steps already completed before you
start the tutorial modules:

* Make sure you have `python 3.6`__ or newer.

__ https://www.python.org/downloads/release/python-360/

* Make sure you have `pip`__ version 22.3 or newer.

__ https://www.pypi.org/project/pip/
    
    * You can upgrade pip to the latest version with:

    .. code-block:: bash

        pip install --upgrade pip

    * OR you can upgrade to a specific version with:

    .. code-block:: bash

        pip install --upgrade pip==x.y.z 


* Make sure you have `GNU make tools`__ and `compilers`__.

__ https://www.gnu.org/software/make/
__ https://gcc.gnu.org/

* (OPTIONAL) Install `docker`__.

__ https://docs.docker.com/install/

    * Download OpenFOAM image with:

    .. code-block:: bash

        docker pull cfdengine/openfoam

    * Download redis image with:

    .. code-block:: bash

        docker pull redis
