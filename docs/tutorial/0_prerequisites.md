# Before You Start

It will be helpful to have these steps already completed before you
start the tutorial modules:

* Make sure you have [python 3.8](https://www.python.org/downloads/release/python-380/) or newer.

* Make sure you have [pip](https://www.pypi.org/project/pip/) version 22.3 or newer.
    
    * You can upgrade pip to the latest version with:

        ```bash
        pip install --upgrade pip
        ```

    * OR you can upgrade to a specific version with:

        ```bash
        pip install --upgrade pip==x.y.z
        ```

* Make sure you have [GNU make tools](https://www.gnu.org/software/make/) and [compilers](https://gcc.gnu.org/).

* (OPTIONAL) Install [docker](https://docs.docker.com/install/).

    * Download OpenFOAM image with:

        ```bash
        docker pull cfdengine/openfoam
        ```

    * Download redis image with:

        ```bash
        docker pull redis
        ```
