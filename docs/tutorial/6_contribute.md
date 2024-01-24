# Contribute to Merlin

!!! info "Estimated Time"

    10 minutes

!!! abstract "You Will Learn"

    - How to post issues to the Merlin repo.
    - Guidelines for contributing to Merlin.

## Issues

Found a bug? Have an idea? Or just want to ask a question? [Create a new issue](https://github.com/LLNL/merlin/issues/new/choose) on GitHub.

### Bug Reports

To report a bug, simply navigate to [Issues](https://github.com/LLNL/merlin/issues), click "New Issue", then click "Bug report". Then simply fill out a few fields such as "Describe the bug" and "Expected behavior". Try to fill out every field as it will help us figure out your bug sooner.

### Feature Requests

We are still adding new features to Merlin. To suggest one, simply navigate to [Issues](https://github.com/LLNL/merlin/issues), click "New Issue", then click "Feature request". Then fill out a few fields such as "What problem is this feature looking to solve?"

### Questions

!!! note

    Who knows? Your question may already be answered in the [FAQ](../faq.md).

We encourage questions to be asked in a collaborative setting: on GitHub, direct any questions to [General Questions](https://github.com/LLNL/merlin/issues/new?labels=question&template=question.md&title=%5BQ%2FA%5D+) in Issues.

For more ways to reach out with questions, see the [Contact](../contact.md) page.

## Contributing

Merlin is an open source project, so contributions are welcome. Contributions can be anything from bugfixes, documentation, or even new core features.

Merlin uses a rough approximation of the Git Flow branching model. The `develop` branch contains the latest contributions, and `main` is always tagged and points to the latest stable release.

If you're a contributor, try to test and run by branching off of `develop`. That's where all the magic is happening (and where we hope bugs stop).

More detailed information on contributing can be found on the [Contributing](../user_guide/contributing.md).

### How to Contribute

Contributing to Merlin is as easy as following these steps:

1. [Fork the Merlin repository](https://github.com/LLNL/merlin/fork)

2. [Clone your forked repository](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository)

3. Move into the `merlin/` directory:

    ```bash
    cd merlin/
    ```

4. Ensure you're on the `develop` branch by running:

    ```bash
    git checkout develop
    ```

5. Create a new virtual environment and install an editable version of Merlin with:

    ```bash
    make install-dev
    ```

    !!! note

        The name of this virtual environment will be `venv_merlin_py_x_y` where `x` and `y` represent the Python version that was used to create the virtual environment. For example, if you had Python 3.10.8 loaded when you run this command, you'll get a virtual environment named `venv_merlin_py_3_10`.

6. Activate the environment that was just created with the below command. You'll have to modify `x` and `y` here to match the python version that your virtual environment was created with.

    === "bash"

        ```bash
        source venv_merlin_py_x_y/bin/activate
        ```

    === "csh"

        ```csh
        source venv_merlin_py_x_y/bin/activate.csh
        ```

7. Create a new branch for your changes. Typically branch names will start with one of the following prefixes: `feature`, `bugfix`, `refactor`, or `docs`. The following command will create a new branch for you and switch to it:

    ```bash
    git switch -c <branch prefix>/<your branch name>
    ```

8. Create your changes

9. Once you've made all of your changes, run the following command from the root of the repository to ensure the style of your code matches Merlin's standard:

    ```bash
    make fix-style
    ```

10. Ensure all of the tests pass:

    ```bash
    make tests
    ```

11. Summarize your changes in the `[Unreleased]` section in the `CHANGELOG.md` file

12. [Send us a pull request](https://github.com/LLNL/merlin/pulls) from your fork. Make sure you're requesting a pull request from your branch to the `develop` branch on Merlin's home repository.

Happy contributing!
