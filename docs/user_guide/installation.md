# Installation

!!! note

    It's highly recommended to create and activate a [virtual environment](https://virtualenv.pypa.io/en/stable/) prior to following the instructions on this page. See the [Virtual Environments](virtual_envs.md) page for more info.

## Installing With Pip

Merlin can be installed using pip:

```bash
pip3 install merlin
```

Verify that Merlin installed properly by ensuring the following command can run:

```bash
merlin --version
```

All set up? See the [Configuration](configuration.md) page for instructions on how to configure Merlin or check out the [Tutorial](../tutorial/index.md)!

More information on Merlin commands can be found at the [Command Line Interface](command_line.md) page.

## Developer Setup

The developer setup can be done via pip or via make. This section will cover how to do both.

Additionally, there is an alternative method to setup merlin on supercomputers. See the :doc:`Spack <./spack>` section for more details.

### Pip Setup

To install with the additional developer dependencies:

=== "PyPi"
    ```bash
    pip3 install "merlin[dev]"
    ```

=== "GitHub"
    ```bash
    pip3 install -e "git+https://github.com/LLNL/merlin.git@develop#egg=merlin[dev]"
    ```

### Make Setup

Visit the [Merlin repository](https://github.com/LLNL/merlin/) on github. [Create a fork of the repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo) and [clone it](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository) onto your system.

Change directories into the merlin repo:

```bash
cd merlin/
```

Install Merlin with the developer dependencies:

```bash
make install-dev
```

This will create a virtualenv, start it, and install Merlin and it's dependencies for you.

More documentation about using Virtualenvs with Merlin can be found at the [Virtual Environments](virtual_envs.md) page.

We can make sure it's installed by running:

```bash
merlin --version
```

If you don't see a version number, you may need to restart your virtualenv and try again. 

### Configuring Merlin

Once Merlin has been installed, the installation needs to be configured. See the [Configuration](configuration.md) page for instructions on how to configure Merlin

That's it. To start running Merlin see the :doc:`Merlin Workflows. <./merlin_workflows>`
