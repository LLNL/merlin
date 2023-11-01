# Integration Tests

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
