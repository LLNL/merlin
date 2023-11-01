# Tests

This directory utilizes pytest to create and run our test suite.
Here we use pytest fixtures to create a local redis server and a celery app for testing.

This directory is organized like so:
- `conftest.py` - The script containing all fixtures for our tests
- `unit/` - The directory containing unit tests
    - `test_*.py` - The actual test scripts to run
- `integration/` - The directory containing integration tests
    <!-- - `test_*.py` - The actual test scripts to run -->
    - `definitions.py` - The test definitions
    - `run_tests.py` - The script to run the tests defined in `definitions.py`
    - `conditions.py` - The conditions ot test against

## How to Run

Before running any tests:

1. Activate your virtual environment with Merlin's dev requirements installed
2. Navigate to the integration tests folder where this README is located

To run the entire test suite:

`python -m pytest`

To run a specific test file:

`python -m pytest /path/to/test_specific_file.py`

To run a certain test class within a specific test file:

`python -m pytest /path/to/test_specific_file.py::TestCertainClass`

To run one unique test:

`python -m pytest /path/to/test_specific_file.py::TestCertainClass::test_unique_test`

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
from tests.conftest import dummy_fixture

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
def merlin_server_dir(temp_output_dir: str) -> str:
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