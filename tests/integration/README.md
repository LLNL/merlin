# Integration test script: run_tests.py

To run command line-level tests of Merlin, follow these steps:

1. activate the Merlin virtual environment
2. navigate to the top-level `merlin` directory
3. run `python tests/integration/run_tests.py`

This will run all tests found in the `define_tests` function.
A test is a python dict where the key is the test name, and the
value is a tuple holding the test shell command, and a regexp string
to search for, if any. Without a regexp string, the script will
output 'FAIL' on non-zero return codes. With a regexp string, the
script outputs 'FAIL' if the string cannot be found in the
test's stdout.


### Continuous integration
Currently run from [Bamboo](https://lc.llnl.gov/bamboo/chain/admin/config/defaultStages.action?buildKey=MLSI-TES).

Our Bamboo agents make the virtual environment, staying at the `merlin/` location, then run: `python tests/integration/run_tests.py`.
