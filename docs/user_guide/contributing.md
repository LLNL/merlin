# Contributing

Welcome to the Merlin developer documentation! This module provides instructions for contributing to Merlin.

## Getting Started

Follow the [Developer Setup](./installation.md#developer-setup) documentation to setup your Merlin development environment (we recommend using the [Make Setup](./installation.md#make-setup)).

Once your development environment is setup ensure you're on the development branch:

```bash
git checkout develop
```

Then create a new branch for whatever you're working on. Typically branch names will start with one of the following prefixes: `feature`, `bugfix`, `refactor`, or `docs`. The following command will create a new branch for you and switch to it:

```bash
git switch -c <branch prefix>/<your branch name>
```

Merlin follows a [gitflow workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow). Updates to the develop branch are made via pull requests.

## Developer Guide

This section provides Merlin's guide for contributing features/bugfixes to Merlin.

### Pull Request Checklist

!!! warning

    All pull requests must pass `make tests` prior to consideration!

To expedite review, please ensure that pull requests...

- Are from a meaningful branch name (e.g. `feature/cool_thing`)

- Are being merged into the [appropriate branch](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) (likely [Merlin's develop branch](https://github.com/LLNL/merlin/tree/develop))

- Include testing for any new features

    - unit tests in `tests/unit`
    - integration tests in `tests/integration`

- Include descriptions of the changes

    - a summary in the pull request
    - details in the `[Unreleased]` section of the `CHANGELOG.md`

- Ran `make fix-style` to adhere to style guidelines

    - it's best practice to run `make check-style` too to ensure no further linter changes need to be manually fixed

- Pass `make tests`; output included in pull request

### Testing

All pull requests must pass unit and integration tests. To ensure that they do run:

```bash
make tests
```

All pull requests that include bugfixes or new features must have new tests in `tests/unit` and/or `tests/integration`. See the [README](https://github.com/LLNL/merlin/blob/develop/tests/README.md) in the test suite for instructions on how to write your tests. You can also view the [Reference Guide](../api_reference/index.md) to see API docs for the test suite.

<!-- TODO: get the API docs for the test suite set up -->

### Python Code Style Guide

This section documents Merlin's style guide. Unless otherwise specified, [PEP-8](https://www.python.org/dev/peps/pep-0008/) is the preferred coding style and [PEP-0257](https://www.python.org/dev/peps/pep-0257/) for docstrings.

!!! note

    Running the following command from the root of the Merlin repository should automatically fix most styling issues:

    ```bash
    make fix-style
    ```

Merlin has style checkers configured. They can be run from the Makefile:

```bash
make check-style
```

### Adding New Features to YAML Spec File

!!! note "Block vs Property"

    To provide clarity for the following section, we need to discuss what's meant by a "block" vs a "property" of the [YAML spec](./specification.md).

    **Block:** A block is anything at the first level of a YAML spec. Merlin comes equipped with 7 blocks: `description`, `environment`, `global.parameters`, `batch`, `study`, `merlin`, and `user`.

    **Property:** A property is any keyword defined within a block.

    Here's an example:

    ```yaml
    description:  # Block - description
        name: hello  # Property - name
        description: a very simple merlin workflow  # Property - description

    global.parameters:  # Block - global.parameters
        GREET:  # Property - GREET
            values : ["hello","hola"]  # Property - values
            label  : GREET.%%  # Property - label
        WORLD:  # Property - WORLD
            values : ["world","mundo"]  # Property - values
            label  : WORLD.%%  # Property - label
    ```

In order to conform to Maestro's verification format introduced in Maestro v1.1.7, we now use [json schema](https://json-schema.org/) validation to verify our spec file. 

If you are adding a new feature to Merlin that requires a new block within the yaml spec file or a new property within a block, then you are going to need to update the `merlinspec.json` file located in the `merlin/spec/` directory. You also may want to add additional verifications within the `specification.py` file located in the same directory.

!!! note

    If you add custom verifications beyond the pattern checking that the json schema checks for, then you should also add tests for this verification in the `test_specification.py` file located in the `merlin/tests/unit/spec/` directory.

#### Adding a New Property

To add a new property to a block in the yaml file, you need to create a template for that property and place it in the correct block in `merlinspec.json`. 

!!! tip

    For help with json schema formatting, check out the [step-by-step getting started guide](https://json-schema.org/learn/getting-started-step-by-step.html).

!!! example

    Say I wanted to add a new property called `example` that's an integer within the `description` block. I would modify the `description` block in the `merlinspec.json` file to look like so:

    ```json title="merlinspec.json" hl_lines="7"
    {
        "DESCRIPTION": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "description": {"type": "string", "minLength": 1},
                "example": {"type": "integer", "minimum": 1}
            },
            "required": ["name", "description"]
        }
        .
        .
        .
    }
    ```

That's all that's required of adding a new property. If you want to add your own custom verifications make sure to create [unit tests](#testing) for them.

#### Adding a New Block

Adding a new block is slightly more complicated than adding a new property. You will not only have to update the `merlinspec.json` schema file but also add calls to verify that block within `specification.py`.

To add a block to the json schema, you will need to define the template for that entire block.

!!! tip

    For help with json schema formatting, check out the [step-by-step getting started guide](https://json-schema.org/learn/getting-started-step-by-step.html).

!!! example

    Say I wanted to create a block called `country` with two properties labeled `name` and `population` that are both required. It would look like so:

    ```json title="merlinspec.json"
    {
        .
        .
        .
        "COUNTRY": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "population": {
                    "anyOf": [
                        {"type": "string", "minLength": 1},
                        {"type": "integer", "minimum": 1}
                    ]
                }
            },
            "required": ["name", "capital"]
        }
    }
    ```
    
    Here, `name` can only be a string but `population` can be both a string and an integer. 

The next step is to enable this block in the schema validation process of `specification.py`. To do this we need to:

1. Create a new method called `verify_<your_block_name>()` within the `MerlinSpec` class
2. Call the `YAMLSpecification.validate_schema()` method provided to us via [Maestro](https://github.com/LLNL/maestrowf/blob/develop/maestrowf/specification/yamlspecification.py#L400) in your new method
3. Add a call to `verify_<your_block_name>()` inside the `verify()` method

If you add your own custom verifications on top of this, please add [unit tests](#testing) for them.
