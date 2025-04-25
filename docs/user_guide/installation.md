# Installation

The Merlin library can be installed by using [virtual environments and pip](#installing-with-virtual-environments-pip) or [spack](#installing-with-spack).

Contributors to Merlin should follow the [Developer Setup](#developer-setup) below.

Once Merlin has been installed, the installation needs to be configured. See the [Configuration](./configuration/index.md) page for instructions on how to configure Merlin.

## Installing With Virtual Environments & Pip

The most common way to install Merlin is via pip. To accomplish this method of installation we'll need to:

1. Set up and activate a virtual environment
2. Install Merlin with pip

### Creating A Virtual Environment

We'll be creating a Python [virtual environment](https://virtualenv.pypa.io/en/stable/) in this section. To do this, run:

```bash
python3 -m venv venv
```

!!! warning

    A virtual environment will need to be created for each system type. It's recommended to name the virtual environment `venv_<system>` to make it easier to switch between them. This documentation will use `venv` for simplicity to reference the virtual environment.

!!! tip

    Virtual environments provide an isolated environment for working on Python projects to avoid dependency conflicts.

### Activating A Virtual Environment

Once the virtual environment is created it can be activated like so:

=== "bash"

    ```bash
    source venv/bin/activate
    ```

=== "csh"

    ```csh
    source venv/bin/activate.csh
    ```

This will set the Python and Pip path to the virtual environment at `venv/bin/python` and `venv/bin/pip` respectively.

The virtual environment name should now display in the terminal, which means it is active. Any calls to pip will install to the virtual environment.

!!! tip

    To verify that Python and Pip are pointing to the virtual environment, run

    ```bash
    which python pip
    ```

### Pip Installing Merlin

Ensure your virtual environment is activated. Once it is, Merlin can be installed using pip:

```bash
pip3 install merlin
```

Verify that Merlin installed properly by ensuring the following command can run:

```bash
merlin --version
```

All set up? See the [Configuration](./configuration/index.md) page for instructions on how to configure Merlin or check out the [Tutorial](../tutorial/index.md)!

More information on Merlin commands can be found at the [Command Line Interface](command_line.md) page.

### Deactivating A Virtual Environment

Virtualenvs can be exited via the following:

```bash
deactivate
```

## Installing With Spack

The virtualenv method is not the only method to install Merlin in a separate Python install. The Spack method will build Python and all required modules for a specific set of configuration options. These options include the compiler version, system type and Python version. Merlin will then be installed in this specific version allowing for multiple Python versions on a single system without the need for a virtualenv. The py-merlin package builds with python3.6+.

### Checkout Spack

Get the latest version of Spack from Github. This is independent from Merlin so make sure Merlin and Spack are in separate directories.

```bash
git clone https://github.com/spack/spack.git
```

Move into the Spack directory:

```bash
cd spack/
```

The Merlin Spack package is in the develop branch so checkout that branch:

```bash
git checkout develop
```

### Setup Spack

Source the `setup-env.sh` or `setup-env.csh`. This will put Spack in your path and setup module access for later use. This should be done every time the modules are used.

```bash
source ./share/spack/setup-env.sh
```

Add compilers if you haven't already:

```bash
spack compiler add
```

To see the compilers:

```bash
spack compiler list
```

### Build Merlin

Merlin can be built with the default compiler which, in general, is the newest gcc compiler, or the compiler can be specified. In the "Specified Compiler Install" example below, we're specifying gcc v7.1.0. Additionally, a different Python version can be specified as part of the package config. The "Python Version Install" example below shows how to build Merlin with Python v3.6.8:

=== "Default Compiler Install"

    ```bash
    spack install py-merlin
    ```

=== "Specified Compiler Install"

    ```bash
    spack install py-merlin%gcc@7.1.0
    ```

=== "Python Version Install"

    ```bash
    spack install py-merlin^python@3.6.8
    ```

Building Merlin will take a *long* time, be prepared to wait. It will build Python and all Python modules Merlin needs including numpy.

A tree of all of the packages and their dependencies needed to build the Merlin package can be shown by using the spec keyword.

```bash
spack spec py-merlin
```

### Activate Merlin

To use Merlin you can activate the module.

=== "Default Activation"

    ```bash
    spack activate py-merlin
    ```

=== "Specified Compiler Activation"

    ```bash
    spack activate py-merlin%gcc@7.1.0
    ```

=== "Python Version Activation"

    ```bash
    spack activate py-merlin^python@3.6.8
    ```

### Load Python

The associated Python module can then be loaded into your environment, this will only work if you have sourced the `setup-env.sh` or `setup-env.csh`.

```bash
module avail python
```

This will give you a list, the spack version will have a long hash associated with the name.

!!! example

    ```bash
    ------ <path to>/spack/share/spack/modules/linux-rhel7-x86_64 -------
       python-3.6.8-gcc-8.1.0-4ilk3kn (L)
    ```

Now all that's left to do is select which Python to load:

```bash
module load python-3.6.8-<compiler>-<hash>
```

Using the example output above, we could choose to load Python like so:

!!! example

    ```bash
    module load python-3.6.8-gcc-8.1.0-4ilk3kn
    ```

At this point the module specific Python, Merlin, Maestro and Celery will all be in your path.

Congratulations, you're ready to use Merlin! See the [Configuration](./configuration/index.md) page for instructions on how to configure Merlin or check out the [Tutorial](../tutorial/index.md)!

More information on Merlin commands can be found at the [Command Line Interface](command_line.md) page.

## Developer Setup

The developer setup can be done via pip or via make. This section will cover how to do both.

Additionally, there is an alternative method to setup Merlin on supercomputers. See the [Spack](#installing-with-spack) section above for more details.

### Make Setup

Visit the [Merlin repository](https://github.com/LLNL/merlin/) on github. [Create a fork of the repo](https://github.com/LLNL/merlin/fork) and [clone it](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository) onto your system.

Change directories into the Merlin repo:

```bash
cd merlin/
```

Install Merlin with the developer dependencies:

```bash
make install-dev
```

This will create a virtualenv, start it, and install Merlin and it's dependencies for you.

We can make sure it's installed by running:

```bash
merlin --version
```

If you don't see a version number, you may need to restart your virtualenv and try again.

All set up? See the [Configuration](./configuration/index.md) page for instructions on how to configure Merlin or check out the [Tutorial](../tutorial/index.md)!

More information on Merlin commands can be found at the [Command Line Interface](command_line.md) page.

### Pip Setup

[Create a virtual environment](#creating-a-virtual-environment) and [activate it](#activating-a-virtual-environment), then install with the additional developer dependencies:

=== "GitHub"
    ```bash
    pip3 install -e "git+https://github.com/LLNL/merlin.git@develop#egg=merlin[dev]"
    ```

=== "PyPi"
    ```bash
    pip3 install "merlin[dev]"
    ```

All set up? See the [Configuration](./configuration/index.md) page for instructions on how to configure Merlin or check out the [Tutorial](../tutorial/index.md)!

More information on Merlin commands can be found at the [Command Line Interface](command_line.md) page.
