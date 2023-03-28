###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.9.1.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""
This module defines all the integration tests to be ran through run_tests.py.

Each test looks like:
"test name": {
    "cmds": the commands to run,
    "conditions": the conditions to check for,
    "run type": the type of test (local or distributed),
    "cleanup": the command to run after your test in order to clean output,
    "num procs": the number of processes you need to start for a test (1 or 2)
}
"""

import shutil

# Pylint complains that we didn't install this module but it's defined locally so ignore
from conditions import (  # pylint: disable=E0401
    FileHasNoRegex,
    FileHasRegex,
    HasRegex,
    HasReturnCode,
    PathExists,
    ProvenanceYAMLFileHasRegex,
    StepFileExists,
    StepFileHasRegex,
)

from merlin.utils import get_flux_alloc, get_flux_cmd


OUTPUT_DIR = "cli_test_studies"
CLEAN_MERLIN_SERVER = "rm -rf appendonly.aof dump.rdb merlin_server/"
KILL_WORKERS = "pkill -9 -f '.*merlin_test_worker'"


def define_tests():  # pylint: disable=R0914
    """
    Returns a dictionary of tests, where the key
    is the test's name, and the value is a tuple
    of (shell command, condition(s) to satisfy).
    """
    flux_alloc = (get_flux_alloc("flux", no_errors=True),)
    celery_slurm_regex = r"(srun\s+.*)?celery\s+(-A|--app)\s+merlin\s+worker\s+.*"
    celery_flux_regex = rf"({flux_alloc}\s+.*)?celery\s+(-A|--app)\s+merlin\s+worker\s+.*"
    celery_pbs_regex = r"(qsub\s+.*)?celery\s+(-A|--app)\s+merlin\s+worker\s+.*"

    # shortcut string variables
    err_lvl = "-lvl error"
    workers = f"merlin {err_lvl} run-workers"
    run = f"merlin {err_lvl} run"
    restart = f"merlin {err_lvl} restart"
    purge = "merlin purge"
    examples = "merlin/examples/workflows"
    dev_examples = "merlin/examples/dev_workflows"
    demo = f"{examples}/feature_demo/feature_demo.yaml"
    remote_demo = f"{examples}/remote_feature_demo/remote_feature_demo.yaml"
    demo_pgen = f"{examples}/feature_demo/scripts/pgen.py"
    simple = f"{examples}/simple_chain/simple_chain.yaml"
    slurm = f"{examples}/slurm/slurm_test.yaml"
    slurm_restart = f"{examples}/slurm/slurm_par_restart.yaml"
    flux = f"{examples}/flux/flux_test.yaml"
    flux_restart = f"{examples}/flux/flux_par_restart.yaml"
    flux_native = f"{examples}/flux/flux_par_native_test.yaml"
    workers_flux = f"merlin {err_lvl} run-workers"
    fake_cmds_path = "tests/integration/fake_commands"
    if not shutil.which("flux"):
        # Use bogus flux to test if no flux is present
        workers_flux = f"""PATH="{fake_cmds_path}:$PATH";merlin {err_lvl} run-workers"""
    workers_pbs = f"merlin {err_lvl} run-workers" ""
    if not shutil.which("qsub"):
        # Use bogus qsub to test if no pbs scheduler is present
        workers_pbs = f"""PATH="{fake_cmds_path}:$PATH";merlin {err_lvl} run-workers"""
    lsf = f"{examples}/lsf/lsf_par.yaml"
    mul_workers_demo = f"{dev_examples}/multiple_workers.yaml"
    black = "black --check --target-version py36"
    config_dir = "./CLI_TEST_MERLIN_CONFIG"
    release_dependencies = "./requirements/release.txt"

    basic_checks = {
        "merlin": {"cmds": "merlin", "conditions": HasReturnCode(1), "run type": "local"},
        "merlin help": {"cmds": "merlin --help", "conditions": HasReturnCode(), "run type": "local"},
        "merlin version": {"cmds": "merlin --version", "conditions": HasReturnCode(), "run type": "local"},
        "merlin config": {
            "cmds": f"merlin config -o {config_dir}",
            "conditions": HasReturnCode(),
            "run type": "local",
            "cleanup": f"rm -rf {config_dir}",
        },
    }
    server_basic_tests = {
        "merlin server init": {
            "cmds": "merlin server init",
            "conditions": HasRegex(".*successful"),
            "run type": "local",
            "cleanup": CLEAN_MERLIN_SERVER,
        },
        "merlin server start/stop": {
            "cmds": """merlin server init ;
            merlin server start ;
            merlin server status ;
            merlin server stop""",
            "conditions": [
                HasRegex("Server started with PID [0-9]*"),
                HasRegex("Merlin server is running"),
                HasRegex("Merlin server terminated"),
            ],
            "run type": "local",
            "cleanup": CLEAN_MERLIN_SERVER,
        },
        "merlin server restart": {
            "cmds": """merlin server init;
            merlin server start;
            merlin server restart;
            merlin server status;
            merlin server stop;""",
            "conditions": [
                HasRegex("Server started with PID [0-9]*"),
                HasRegex("Merlin server is running"),
                HasRegex("Merlin server terminated"),
            ],
            "run type": "local",
            "cleanup": CLEAN_MERLIN_SERVER,
        },
    }
    server_config_tests = {
        "merlin server change config": {
            "cmds": """merlin server init;
            merlin server config -p 8888 -pwd new_password -d ./config_dir -ss 80 -sc 8 -sf new_sf -am always -af new_af.aof;
            merlin server start;
            merlin server stop;""",
            "conditions": [
                FileHasRegex("merlin_server/redis.conf", "port 8888"),
                FileHasRegex("merlin_server/redis.conf", "requirepass new_password"),
                FileHasRegex("merlin_server/redis.conf", "dir ./config_dir"),
                FileHasRegex("merlin_server/redis.conf", "save 80 8"),
                FileHasRegex("merlin_server/redis.conf", "dbfilename new_sf"),
                FileHasRegex("merlin_server/redis.conf", "appendfsync always"),
                FileHasRegex("merlin_server/redis.conf", 'appendfilename "new_af.aof"'),
                PathExists("./config_dir/new_sf"),
                PathExists("./config_dir/appendonlydir"),
                HasRegex("Server started with PID [0-9]*"),
                HasRegex("Merlin server terminated"),
            ],
            "run type": "local",
            "cleanup": "rm -rf appendonly.aof dump.rdb merlin_server/ config_dir/",
        },
        "merlin server config add/remove user": {
            "cmds": """merlin server init;
            merlin server start;
            merlin server config --add-user new_user new_password;
            merlin server stop;
            scp ./merlin_server/redis.users ./merlin_server/redis.users_new
            merlin server start;
            merlin server config --remove-user new_user;
            merlin server stop;
            """,
            "conditions": [
                FileHasRegex("./merlin_server/redis.users_new", "new_user"),
                FileHasNoRegex("./merlin_server/redis.users", "new_user"),
            ],
            "run type": "local",
            "cleanup": CLEAN_MERLIN_SERVER,
        },
    }
    examples_check = {
        "example list": {
            "cmds": "merlin example list",
            "conditions": HasReturnCode(),
            "run type": "local",
        },
    }
    run_workers_echo_tests = {
        "run-workers echo simple_chain": {
            "cmds": f"{workers} {simple} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_slurm_regex)],
            "run type": "local",
        },
        "run-workers echo feature_demo": {
            "cmds": f"{workers} {demo} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_slurm_regex)],
            "run type": "local",
        },
        "run-workers echo slurm_test": {
            "cmds": f"{workers} {slurm} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_slurm_regex)],
            "run type": "local",
        },
        "run-workers echo flux_test": {
            "cmds": f"{workers} {flux} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_slurm_regex)],
            "run type": "local",
        },
        "run-workers echo flux_native_test": {
            "cmds": f"{workers_flux} {flux_native} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_flux_regex)],
            "run type": "local",
        },
        "run-workers echo pbs_test": {
            "cmds": f"{workers_pbs} {flux_native} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_pbs_regex)],
            "run type": "local",
        },
        "run-workers echo override feature_demo": {
            "cmds": f"{workers} {demo} --echo --vars VERIFY_QUEUE=custom_verify_queue",
            "conditions": [HasReturnCode(), HasRegex("custom_verify_queue")],
            "run type": "local",
        },
    }
    wf_format_tests = {
        "local minimum_format": {
            "cmds": f"mkdir {OUTPUT_DIR}; cd {OUTPUT_DIR}; merlin run ../{dev_examples}/minimum_format.yaml --local",
            "conditions": StepFileExists(
                "step1",
                "MERLIN_FINISHED",
                "minimum_format",
                OUTPUT_DIR,
                params=False,
            ),
            "run type": "local",
        },
        "local no_description": {
            "cmds": f"""mkdir {OUTPUT_DIR}; cd {OUTPUT_DIR};
                    merlin run ../merlin/examples/dev_workflows/no_description.yaml --local""",
            "conditions": HasReturnCode(1),
            "run type": "local",
        },
        "local no_steps": {
            "cmds": f"mkdir {OUTPUT_DIR}; cd {OUTPUT_DIR}; merlin run ../merlin/examples/dev_workflows/no_steps.yaml --local",
            "conditions": HasReturnCode(1),
            "run type": "local",
        },
        "local no_study": {
            "cmds": f"mkdir {OUTPUT_DIR}; cd {OUTPUT_DIR}; merlin run ../merlin/examples/dev_workflows/no_study.yaml --local",
            "conditions": HasReturnCode(1),
            "run type": "local",
        },
    }
    example_tests = {
        "example failure": {"cmds": "merlin example failure", "conditions": HasRegex("not found"), "run type": "local"},
        "example simple_chain": {
            "cmds": f"""merlin example simple_chain;
                    {run} simple_chain.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR}; rm simple_chain.yaml""",
            "conditions": HasReturnCode(),
            "run type": "local",
        },
    }
    restart_step_tests = {
        "local restart_shell": {
            "cmds": f"{run} {dev_examples}/restart_shell.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileExists(
                "step2",
                "MERLIN_FINISHED",
                "restart_shell",
                OUTPUT_DIR,
                params=False,
            ),
            "run type": "local",
        },
        "local restart": {
            "cmds": f"{run} {dev_examples}/restart.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileExists(
                "final_check_for_no_hard_fails",
                "MERLIN_FINISHED",
                "restart",
                OUTPUT_DIR,
                params=False,
            ),
            "run type": "local",
        },
    }
    restart_wf_tests = {
        "restart local simple_chain": {
            "cmds": f"""{run} {simple} --local --vars OUTPUT_PATH=./{OUTPUT_DIR};
                    {restart} $(find ./{OUTPUT_DIR} -type d -name 'simple_chain_*') --local""",
            "conditions": HasReturnCode(),
            "run type": "local",
        },
    }
    dry_run_tests = {
        "dry feature_demo": {
            "cmds": f"{run} {demo} --local --dry --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": [
                StepFileExists(
                    "verify",
                    "verify_*.sh",
                    "feature_demo",
                    OUTPUT_DIR,
                    params=True,
                ),
                HasReturnCode(),
            ],
            "run type": "local",
        },
        "dry launch slurm": {
            "cmds": f"{run} {slurm} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileHasRegex("runs", "*/runs.slurm.sh", "slurm_test", OUTPUT_DIR, "srun "),
            "run type": "local",
        },
        "dry launch flux": {
            "cmds": f"{run} {flux} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileHasRegex(
                "runs",
                "*/runs.slurm.sh",
                "flux_test",
                OUTPUT_DIR,
                get_flux_cmd("flux", no_errors=True),
            ),
            "run type": "local",
        },
        "dry launch lsf": {
            "cmds": f"{run} {lsf} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileHasRegex("runs", "*/runs.slurm.sh", "lsf_par", OUTPUT_DIR, "jsrun "),
            "run type": "local",
        },
        "dry launch slurm restart": {
            "cmds": f"{run} {slurm_restart} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileHasRegex(
                "runs",
                "*/runs.restart.slurm.sh",
                "slurm_par_restart",
                OUTPUT_DIR,
                "srun ",
            ),
            "run type": "local",
        },
        "dry launch flux restart": {
            "cmds": f"{run} {flux_restart} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": StepFileHasRegex(
                "runs_rs",
                "*/runs_rs.restart.slurm.sh",
                "flux_par_restart",
                OUTPUT_DIR,
                get_flux_cmd("flux", no_errors=True),
            ),
            "run type": "local",
        },
    }
    other_local_tests = {
        "local simple_chain": {
            "cmds": f"{run} {simple} --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            "conditions": HasReturnCode(),
            "run type": "local",
        },
        "local override feature_demo": {
            "cmds": f"{run} {demo} --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR} --local",
            "conditions": [
                HasReturnCode(),
                ProvenanceYAMLFileHasRegex(
                    regex=r"HELLO: \$\(SCRIPTS\)/hello_world.py",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="orig",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex=r"name: \$\(NAME\)",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="partial",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex="studies/feature_demo_",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="partial",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex="name: feature_demo",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex=r"\$\(NAME\)",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                    negate=True,
                ),
                StepFileExists(
                    "verify",
                    "MERLIN_FINISHED",
                    "feature_demo",
                    OUTPUT_DIR,
                    params=True,
                ),
            ],
            "run type": "local",
        },
        # "local restart expand name": (
        #    f"""{run} {demo} --local --vars OUTPUT_PATH=./{OUTPUT_DIR} NAME=test_demo;
        #    {restart} $(find ./{OUTPUT_DIR} -type d -name 'test_demo_*') --local""",
        #    [
        #        HasReturnCode(),
        #        ProvenanceYAMLFileHasRegex(
        #            regex="name: test_demo",
        #            name="test_demo",
        #            output_path=OUTPUT_DIR,
        #            provenance_type="expanded",
        #        ),
        #        StepFileExists(
        #            "merlin_info", "test_demo.expanded.yaml", "test_demo", OUTPUT_DIR, params=True,
        #        ),
        #    ],
        #    "local",
        # ),
        "local csv feature_demo": {
            "cmds": f"""echo 42.0,47.0 > foo_testing_temp.csv;
                    merlin run {demo} --samplesfile foo_testing_temp.csv --vars OUTPUT_PATH=./{OUTPUT_DIR} --local;
                    rm -f foo_testing_temp.csv""",
            "conditions": [HasRegex("1 sample loaded."), HasReturnCode()],
            "run type": "local",
        },
        "local tab feature_demo": {
            "cmds": f"""echo '42.0\t47.0\n7.0 5.3' > foo_testing_temp.tab;
                    merlin run {demo} --samplesfile foo_testing_temp.tab --vars OUTPUT_PATH=./{OUTPUT_DIR} --local;
                    rm -f foo_testing_temp.tab""",
            "conditions": [HasRegex("2 samples loaded."), HasReturnCode()],
            "run type": "local",
        },
        "local pgen feature_demo": {
            "cmds": f"{run} {demo} --pgen {demo_pgen} --vars OUTPUT_PATH=./{OUTPUT_DIR} --local",
            "conditions": [
                ProvenanceYAMLFileHasRegex(
                    regex=r"\[0.3333333",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex=r"\[0.5",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                    negate=True,
                ),
                HasReturnCode(),
            ],
            "run type": "local",
        },
    }
    provenence_equality_checks = {  # noqa: F841 pylint: disable=W0612
        "local provenance spec equality": {
            "cmds": f"""{run} {simple} --vars OUTPUT_PATH=./{OUTPUT_DIR} --local;
                    cp $(find ./{OUTPUT_DIR}/simple_chain_*/merlin_info -type f -name 'simple_chain.expanded.yaml') ./{OUTPUT_DIR}/FILE1;
                    rm -rf ./{OUTPUT_DIR}/simple_chain_*;
                    {run} ./{OUTPUT_DIR}/FILE1 --vars OUTPUT_PATH=./{OUTPUT_DIR} --local;
                    cmp ./{OUTPUT_DIR}/FILE1 $(find ./{OUTPUT_DIR}/simple_chain_*/merlin_info -type f -name 'simple_chain.expanded.yaml')""",  # pylint: disable=C0301
            "conditions": HasReturnCode(),
            "run type": "local",
        },
    }
    style_checks = {  # noqa: F841 pylint: disable=W0612
        "black check merlin": {"cmds": f"{black} merlin/", "conditions": HasReturnCode(), "run type": "local"},
        "black check tests": {"cmds": f"{black} tests/", "conditions": HasReturnCode(), "run type": "local"},
    }
    dependency_checks = {
        "deplic no GNU": {
            "cmds": f"deplic {release_dependencies}",
            "conditions": [HasRegex("GNU", negate=True), HasRegex("GPL", negate=True)],
            "run type": "local",
        },
    }
    stop_workers_tests = {
        "stop workers no workers": {
            "cmds": "merlin stop-workers",
            "conditions": [
                HasReturnCode(),
                HasRegex("No workers found to stop"),
                HasRegex("step_1_merlin_test_worker", negate=True),
                HasRegex("step_2_merlin_test_worker", negate=True),
                HasRegex("other_merlin_test_worker", negate=True),
            ],
            "run type": "distributed",
        },
        "stop workers no flags": {
            "cmds": [
                f"{workers} {mul_workers_demo}",
                "merlin stop-workers",
            ],
            "conditions": [
                HasReturnCode(),
                HasRegex("No workers found to stop", negate=True),
                HasRegex("step_1_merlin_test_worker"),
                HasRegex("step_2_merlin_test_worker"),
                HasRegex("other_merlin_test_worker"),
            ],
            "run type": "distributed",
            "cleanup": KILL_WORKERS,
            "num procs": 2,
        },
        "stop workers with spec flag": {
            "cmds": [
                f"{workers} {mul_workers_demo}",
                f"merlin stop-workers --spec {mul_workers_demo}",
            ],
            "conditions": [
                HasReturnCode(),
                HasRegex("No workers found to stop", negate=True),
                HasRegex("step_1_merlin_test_worker"),
                HasRegex("step_2_merlin_test_worker"),
                HasRegex("other_merlin_test_worker"),
            ],
            "run type": "distributed",
            "cleanup": KILL_WORKERS,
            "num procs": 2,
        },
        "stop workers with workers flag": {
            "cmds": [
                f"{workers} {mul_workers_demo}",
                "merlin stop-workers --workers step_1_merlin_test_worker step_2_merlin_test_worker",
            ],
            "conditions": [
                HasReturnCode(),
                HasRegex("No workers found to stop", negate=True),
                HasRegex("step_1_merlin_test_worker"),
                HasRegex("step_2_merlin_test_worker"),
                HasRegex("other_merlin_test_worker", negate=True),
            ],
            "run type": "distributed",
            "cleanup": KILL_WORKERS,
            "num procs": 2,
        },
        "stop workers with queues flag": {
            "cmds": [
                f"{workers} {mul_workers_demo}",
                "merlin stop-workers --queues hello_queue",
            ],
            "conditions": [
                HasReturnCode(),
                HasRegex("No workers found to stop", negate=True),
                HasRegex("step_1_merlin_test_worker"),
                HasRegex("step_2_merlin_test_worker", negate=True),
                HasRegex("other_merlin_test_worker", negate=True),
            ],
            "run type": "distributed",
            "cleanup": KILL_WORKERS,
            "num procs": 2,
        },
    }
    distributed_tests = {  # noqa: F841
        "run and purge feature_demo": {
            "cmds": f"{run} {demo}; {purge} {demo} -f",
            "conditions": HasReturnCode(),
            "run type": "distributed",
        },
        "remote feature_demo": {
            "cmds": f"""{run} {remote_demo} --vars OUTPUT_PATH=./{OUTPUT_DIR} WORKER_NAME=cli_test_demo_workers;
                    {workers} {remote_demo} --vars OUTPUT_PATH=./{OUTPUT_DIR} WORKER_NAME=cli_test_demo_workers""",
            "conditions": [
                HasReturnCode(),
                ProvenanceYAMLFileHasRegex(
                    regex="cli_test_demo_workers:",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                StepFileExists(
                    "verify",
                    "MERLIN_FINISHED",
                    "feature_demo",
                    OUTPUT_DIR,
                    params=True,
                ),
            ],
            "run type": "distributed",
        },
    }

    # combine and return test dictionaries
    all_tests = {}
    for test_dict in [
        basic_checks,
        server_basic_tests,
        server_config_tests,
        examples_check,
        run_workers_echo_tests,
        wf_format_tests,
        example_tests,
        restart_step_tests,
        restart_wf_tests,
        dry_run_tests,
        other_local_tests,
        # provenence_equality_checks, # omitting provenance equality check because it is broken
        # style_checks, # omitting style checks due to different results on different machines
        dependency_checks,
        stop_workers_tests,
        distributed_tests,
    ]:
        all_tests.update(test_dict)

    return all_tests
