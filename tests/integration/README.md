# Integration Tests

This directory utilizes pytest to create and run our integration test suite.
Here we use pytest fixtures to create a local redis server and a celery app for testing.

This directory is organized like so:
- `conftest.py` - script containing all fixtures for our tests
- `test_*.py` - the actual test scripts to run

## How to Run

Before running any tests:

1. Activate your virtual environment with Merlin's dev requirements installed
2. Navigate to the integration tests folder where this README is located

To run the entire test suite:

`python -m pytest`

To run a specific test file:

`python -m pytest test_specific_file.py`

To run a certain test class within a specific test file:

`python -m pytest test_specific_file.py::TestCertainClass`

To run one unique test:

`python -m pytest test_specific_file.py::TestCertainClass::test_unique_test`

## The Fixture Process Explained

Pytest fixtures play a fundamental role in establishing a consistent foundation for test execution,
thus ensuring reliable and predictable test outcomes. This section will delve into essential aspects
of these fixtures, including how to integrate fixtures into tests, the utilization of fixtures within other fixtures,
their scope, and the yielding of fixture results.

### How to Integrate Fixtures Into Tests

Probably the most important part of fixtures is understanding how to use them. Luckily, this process is very
simple and can be dumbed down to 2 steps:

1. Create a fixture in the `conftest.py` file by using the `@pytest.fixture` decorator. For example:

```
@pytest.fixture
def dummy_fixture():
    return "hello world"
```

2. Import the fixture and use it as an argument in a test function:

```
from tests.integration.conftest import dummy_fixture

def test_dummy(dummy_fixture):
    assert dummy_fixture == "hello world"
```

For more information, see [Pytest's documentation](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#how-to-use-fixtures).

### Fixtureception

One of the coolest and most useful aspects of fixtures that we utilize in this test suite is the ability for
fixtures to be used within other fixtures. For more info on this from pytest, see
[here](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#fixtures-can-request-other-fixtures).

Pytest will handle fixtures within fixtures in a stack-based way. Let's look at how creating the `redis_pass`
fixture from our `conftest.py` file works in order to illustrate the process.
1. First, we start by telling pytest that we want to use the `redis_pass` fixture by providing it as an argument
to a test/fixture:

```
def test_example(redis_pass):
    ...
```

2. Now pytest will find the `redis_pass` fixture and put it at the top of the stack to be created. However,
it'll see that this fixture requires another fixture `merlin_server_dir` as an argument:

```
@pytest.fixture(scope="session")
def redis_pass(merlin_server_dir):
    ...
```

3. Pytest then puts the `merlin_server_dir` fixture at the top of the stack, but similarly it sees that this fixture
requires yet another fixture `temp_output_dir`:

```
@pytest.fixture(scope="session")
def temp_output_dir(tmp_path_factory: "TempPathFactory"):
    ...
```

4. This process continues until it reaches a fixture that doesn't require any more fixtures. At this point the base
fixture is created and pytest will start working its way back up the stack to the first fixture it looked at (in this
case `redis_pass`).

5. Once all required fixtures are created, execution will be returned to the test which can now access the fixture
that was requested (`redis_pass`).

As you can see, if we have to re-do this process for every test it could get pretty time intensive. This is where fixture
scopes come to save the day.

### Fixture Scopes

There are several different scopes that you can set for fixtures. The majority of our fixtures use a `session`
scope so that we only have to create the fixtures one time (as some of them can take a few seconds to set up).
The goal is to create fixtures with the most general use-case in mind so that we can re-use them for larger
scopes, which helps with efficiency.

For more info on scopes, see
[Pytest's Fixture Scope documentation](https://docs.pytest.org/en/6.2.x/fixture.html#scope-sharing-fixtures-across-classes-modules-packages-or-session).

### Yielding Fixtures

In several fixtures throughout our test suite, we need to run some sort of teardown for the fixture. For example,
once we no longer need the `redis_server` fixture, we need to shut the server down so it stops using resources.
This is where yielding fixtures becomes extremely useful.

Using the `yield` keyword allows execution to be returned to a test that needs the fixture once the feature has
been set up. After all tests using the fixture have been ran, execution will return to the fixture for us to run
our teardown code.

For more information on yielding fixtures, see [Pytest's documentation](https://docs.pytest.org/en/7.1.x/how-to/fixtures.html#teardown-cleanup-aka-fixture-finalization).

# Old Integration Test Suite

This directory contains 3 key files for testing:
1. `run_tests.py` - script to launch tests
2. `test_definitions.py` - test definitions
3. `conditions.py` - test conditions

## How to Run

To run command-line-level tests of Merlin, follow these steps:

1. activate the Merlin virtual environment
2. navigate to the top-level `merlin` directory
3. run `python tests/integration/run_tests.py`

This will run all tests found in the `define_tests` function located within `test_definitions.py`.

## How Tests are Defined

A test is a python dict where the key is the test name and the
value is another dict. The value dict can currently have 5 keys:

Required:

1. `cmds`
    - Type: Str or List[Str]
    - Functionality: Defines the CLI commands to run for a test
    - Limitations: The number of strings here should be equal to `num procs` (see `5.num procs` below)
2. `conditions`
    - Type: Condition or List[Condition]
    - Functionality: Defines the conditions to check against for this test
    - Condition classes can be found in `conditions.py`

Optional:

3. `run type`
    - Type: Str
    - Functionality: Defines the type of run (either `local` or `distributed`)
    - Default: None
4. `cleanup`
    - Type: Str
    - Functionality: Defines a CLI command to run that will clean the output of your test
    - Default: None
5. `num procs`
    - Type: int
    - Functionality: Defines the number of subprocesses required for a test
    - Default: 1
    - Limitations:
        - Currently the value here can only be 1 or 2
        - The number of `cmds` must be equal to `num procs` (i.e. one command will be run per subprocess launched)

## Examples

This section will show both valid and invalid test definitions.

### Valid Test Definitions

The most basic test you can provide can be written 4 ways since `cmds` and `conditions` can both be 2 different types:

    "cmds as string, conditions as Condition": {
        "cmds": "echo hello",
        "conditions": HasReturnCode(),
    }

    "cmds as list, conditions as Condition": {
        "cmds": ["echo hello"],
        "conditions": HasReturnCode(),
    }

    "cmds as string, conditions as list": {
        "cmds": "echo hello",
        "conditions": [HasReturnCode()],
    }

    "cmds as list, conditions as list": {
        "cmds": ["echo hello"],
        "conditions": [HasReturnCode()],
    }

Adding slightly more complexity, we provide a run type:

    "basic test with run type": {
        "cmds": "echo hello",
        "conditions": HasReturnCode(),
        "run type": "local" # This could also be "distributed"
    }

Now we'll add a cleanup command:

    "basic test with cleanup": {
        "cmds": "mkdir output_dir/",
        "conditions": HasReturnCode(),
        "run type": "local",
        "cleanup": "rm -rf output_dir/"
    }

Finally we'll play with the number of processes to start:

    "test with 1 process": {
        "cmds": "mkdir output_dir/",
        "conditions": HasReturnCode(),
        "run type": "local",
        "cleanup": "rm -rf output_dir/",
        "num procs": 1
    }

    "test with 2 processes": {
        "cmds": ["mkdir output_dir/", "touch output_dir/new_file.txt"],
        "conditions": HasReturnCode(),
        "run type": "local",
        "cleanup": "rm -rf output_dir/",
        "num procs": 2
    }

Similarly, the test with 2 processes can be condensed to a test with 1 process
by placing them in the same string and separating them with a semi-colon:

    "condensing test with 2 processes into 1 process": {
        "cmds": ["mkdir output_dir/ ; touch output_dir/new_file.txt"],
        "conditions": HasReturnCode(),
        "run type": "local",
        "cleanup": "rm -rf output_dir/",
        "num procs": 1
    }


### Invalid Test Definitions

No `cmds` provided:

    "no cmd": {
        "conditions": HasReturnCode(),
    }

No `conditions` provided:

    "no conditions": {
        "cmds": "echo hello",
    }

Number of `cmds` does not match `num procs`:

    "num cmds != num procs": {
        "cmds": ["echo hello; echo goodbye"],
        "conditions": HasReturnCode(),
        "num procs": 2
    }

Note: Technically 2 commands were provided here ("echo hello" and "echo goodbye")
but since they were placed in one string it will be viewed as one command. 
Changing the `cmds` section here to be:

    "cmds": ["echo hello", "echo goodbye"]

would fix this issue and create a valid test definition.


## Continuous Integration

Merlin's CI is currently done through [GitHub Actions](https://github.com/features/actions). If you're needing to modify this CI, you'll need to update `/.github/workflows/push-pr_workflow.yml`.
