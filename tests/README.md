# Tests

This directory utilizes pytest to create and run our test suite.

This directory is organized like so:
- `conftest.py` - The script containing common fixtures for our tests
- `constants.py` - Constant values to be used throughout the test suite.
- `fixture_data_classes.py` - Dataclasses to help group pytest fixtures together, reducing the required number of imports.
- `fixture_types.py` - Aliases for type hinting fixtures.
- `context_managers/` - The directory containing context managers used for testing
    - `celery_workers_manager.py` - A context manager used to manage celery workers for integration testing
    - `server_manager.py` - A context manager used to manage the redis server used for integration testing
- `fixtures/` - The directory containing specific test module fixtures
    - `<test_module_name>.py` - Fixtures for specific test modules
- `integration/` - The directory containing integration tests
    - `definitions.py` - The test definitions
    - `run_tests.py` - The script to run the tests defined in `definitions.py`
    - `conditions.py` - The conditions to test against
    - `commands/` - The directory containing tests for commands of the Merlin library.
    - `workflows/` The directory containing tests for entire workflow runs.
- `unit/` - The directory containing unit tests
    - `test_*.py` - The actual test scripts to run

## How to Run

Before running any tests:

1. Activate your virtual environment with Merlin's dev requirements installed
2. Navigate to the tests folder where this README is located

To run the entire test suite:

```
python -m pytest
```

To run a specific test file:

```
python -m pytest /path/to/test_specific_file.py
```

To run a certain test class within a specific test file:

```
python -m pytest /path/to/test_specific_file.py::TestCertainClass
```

To run one unique test:

```
python -m pytest /path/to/test_specific_file.py::TestCertainClass::test_unique_test
```

## Viewing Results

Test results will be written to `/tmp/$(whoami)/pytest-of-$(whoami)/pytest-current/python_{major}.{minor}.{micro}_current/`.

It's good practice to set up a subdirectory in this temporary output folder for each module that you're testing. You can see an example of how this is set up in the files within the module-specific fixture directory. For instance, you can see this in the `examples_testing_dir` fixture from the `tests/fixtures/examples.py` file:

```
@pytest.fixture(scope="session")
def examples_testing_dir(temp_output_dir: str) -> str:
    """
    Fixture to create a temporary output directory for tests related to the examples functionality.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :returns: The path to the temporary testing directory for examples tests
    """
    testing_dir = f"{temp_output_dir}/examples_testing"
    if not os.path.exists(testing_dir):
        os.mkdir(testing_dir)

    return testing_dir
```

## Killing the Test Server

In case of an issue with the test suite, or if you stop the tests with `ctrl+C`, you may need to stop
the server manually. This can be done with:

```
redis-cli
127.0.0.1:6379> AUTH merlin-test-server
127.0.0.1:6379> shutdown
not connected> quit
```

## The Fixture Process Explained

In the world of pytest testing, fixtures are like the building blocks that create a sturdy foundation for your tests. They ensure that every test starts from the same fresh ground, leading to reliable and consistent results. This section will dive into the nitty-gritty of these fixtures, showing you how they're architected in this test suite, how to use them in your tests here, how to combine them for more complex scenarios, how long they stick around during testing, and what it means to yield a fixture.

### Fixture Architecture

Fixtures can be defined in two locations within this test suite:

1. `tests/conftest.py`: This file located at the root of the test suite houses common fixtures that are utilized across various test modules
2. `tests/fixtures/`: This directory contains specific test module fixtures. Each fixture file is named according to the module(s) that the fixtures defined within are for.

Credit for this setup must be given to [this Medium article](https://medium.com/@nicolaikozel/modularizing-pytest-fixtures-fd40315c5a93).

#### Fixture Naming Conventions

For fixtures defined within the `tests/fixtures/` directory, the fixture name should be prefixed by the name of the fixture file they are defined in.

#### Importing Fixtures as Plugins

Fixtures located in the `tests/fixtures/` directory are technically plugins. Therefore, to use them we must register them as plugins within the `conftest.py` file (see the top of said file for the implementation). This allows them to be discovered and used by test modules throughout the suite.

**You do not have to register the fixtures you define as plugins in `conftest.py` since the registration there uses `glob` to grab everything from the `tests/fixtures/` directory automatically.**

### How to Integrate Fixtures Into Tests

Probably the most important part of fixtures is understanding how to use them. Luckily, this process is very simple and can be dumbed down to just a couple steps:

0. **[Module-specific fixtures only]** If you're creating a module-specific fixture (i.e. a fixture that won't be used throughout the entire test suite), then create a file in the `tests/fixtures/` directory.

1. Create a fixture in either the `conftest.py` file or the file you created in the `tests/fixtures/` directory by using the `@pytest.fixture` decorator. For example:

```
@pytest.fixture
def dummy_fixture() -> str:
    return "hello world"
```

2. Use it as an argument in a test function (you don't even need to import it!):

```
def test_dummy(dummy_fixture: str):
    assert dummy_fixture == "hello world"
```

For more information, see [Pytest's documentation](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#how-to-use-fixtures).

### Fixtureception

One of the coolest and most useful aspects of fixtures that we utilize in this test suite is the ability for fixtures to be used within other fixtures. For more info on this from pytest, see [here](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#fixtures-can-request-other-fixtures).

Pytest will handle fixtures within fixtures in a stack-based way. Let's look at how creating the `redis_pass` fixture from our `conftest.py` file works in order to illustrate the process.

1. First, we start by telling pytest that we want to use the `redis_pass` fixture by providing it as an argument to a test/fixture:

```
def test_example(redis_pass):
    ...
```

2. Now pytest will find the `redis_pass` fixture and put it at the top of the stack to be created. However, it'll see that this fixture requires another fixture `merlin_server_dir` as an argument:

```
@pytest.fixture(scope="session")
def redis_pass(merlin_server_dir):
    ...
```

3. Pytest then puts the `merlin_server_dir` fixture at the top of the stack, but similarly it sees that this fixture requires yet another fixture `temp_output_dir`:

```
@pytest.fixture(scope="session")
def merlin_server_dir(temp_output_dir: str) -> str:
    ...
```

4. This process continues until it reaches a fixture that doesn't require any more fixtures. At this point the base fixture is created and pytest will start working its way back up the stack to the first fixture it looked at (in this case `redis_pass`).

5. Once all required fixtures are created, execution will be returned to the test which can now access the fixture that was requested (`redis_pass`).

As you can see, if we have to re-do this process for every test it could get pretty time intensive. This is where fixture scopes come to save the day.

### Fixture Scopes

There are several different scopes that you can set for fixtures. The majority of our fixtures in `conftest.py` use a `session` scope so that we only have to create the fixtures one time (as some of them can take a few seconds to set up). The goal for fixtures defined in `conftest.py` is to create fixtures with the most general use-case in mind so that we can re-use them for larger scopes, which helps with efficiency.

For fixtures that need to be reset on each run, we generally try to place these in the module-specific fixture directory `tests/fixtures/`.

For more info on scopes, see [Pytest's Fixture Scope documentation](https://docs.pytest.org/en/6.2.x/fixture.html#scope-sharing-fixtures-across-classes-modules-packages-or-session).

### Yielding Fixtures

In several fixtures throughout our test suite, we need to run some sort of teardown for the fixture. For example, once we no longer need the `redis_server` fixture, we need to shut the server down so it stops using resources. This is where yielding fixtures becomes extremely useful.

Using the `yield` keyword allows execution to be returned to a test that needs the fixture once the feature has been set up. After all tests using the fixture have been ran, execution will return to the fixture for us to run our teardown code.

For more information on yielding fixtures, see [Pytest's documentation](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#teardown-cleanup-aka-fixture-finalization).