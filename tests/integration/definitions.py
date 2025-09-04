##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

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

import os
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

from merlin.study.batch import check_for_scheduler
from merlin.utils import get_flux_alloc, get_flux_cmd


OUTPUT_DIR = "cli_test_studies"
CLEAN_MERLIN_SERVER = "rm -rf appendonly.aof dump.rdb merlin_server/"
KILL_WORKERS = "pkill -9 -f '.*merlin_test_worker'"


def get_worker_by_cmd(cmd: str, default: str) -> str:
    """
    Given a command used by a scheduler (e.g. flux for flux, jsrun for lsf, etc.)
    check if that command is found and if it isn't, use a fake scheduler command.
    :param `cmd`: A string representing the command used by a scheduler
    :param `default`: The default worker launch command to use if a scheduler is found
    :returns: The appropriate worker launch command
    """
    workers_cmd = default
    fake_cmds_path = f"tests/integration/fake_commands/{cmd}_fake_command"
    bogus_cmd = f"""PATH="{fake_cmds_path}:$PATH";{default}"""
    scheduler_legend = {"flux": {"check cmd": ["flux", "resource", "list"], "expected check output": b"Nodes"}}

    # Use bogus flux/qsub to test if no flux/qsub is present
    if not shutil.which(cmd):
        workers_cmd = bogus_cmd
    # Use bogus flux if flux is present but slurm is the main scheduler
    elif cmd == "flux" and not check_for_scheduler(cmd, scheduler_legend):
        workers_cmd = bogus_cmd

    return workers_cmd


def define_tests():  # pylint: disable=R0914,R0915
    """
    Returns a dictionary of tests, where the key
    is the test's name, and the value is a tuple
    of (shell command, condition(s) to satisfy).
    """
    # Shortcuts for regexs to check against
    flux_alloc = (get_flux_alloc("flux", no_errors=True),)
    celery_regex = r"celery\s+(-A|--app)\s+merlin\s+worker\s+.*"
    celery_slurm_regex = rf"(srun\s+.*)?{celery_regex}"
    celery_lsf_regex = rf"(jsrun\s+.*)?{celery_regex}"
    celery_flux_regex = rf"({flux_alloc}\s+.*)?{celery_regex}"
    celery_pbs_regex = rf"(qsub\s+.*)?{celery_regex}"

    # Shortcuts for Merlin commands
    err_lvl = "-lvl debug"
    workers = f"merlin {err_lvl} run-workers"
    workers_flux = get_worker_by_cmd("flux", workers)
    workers_pbs = get_worker_by_cmd("qsub", workers)
    workers_lsf = get_worker_by_cmd("jsrun", workers)
    run = f"merlin {err_lvl} run"
    restart = f"merlin {err_lvl} restart"

    # Shortcuts for example workflow paths
    examples = "merlin/examples/workflows"
    dev_examples = "merlin/examples/dev_workflows"
    test_specs = "tests/integration/test_specs"
    demo = f"{examples}/feature_demo/feature_demo.yaml"
    demo_pgen = f"{examples}/feature_demo/scripts/pgen.py"
    simple = f"{examples}/simple_chain/simple_chain.yaml"
    slurm = f"{test_specs}/slurm_test.yaml"
    slurm_restart = f"{examples}/slurm/slurm_par_restart.yaml"
    flux = f"{test_specs}/flux_test.yaml"
    flux_restart = f"{examples}/flux/flux_par_restart.yaml"
    flux_native = f"{test_specs}/flux_par_native_test.yaml"
    lsf = f"{examples}/lsf/lsf_par.yaml"
    cli_substitution_wf = f"{test_specs}/cli_substitution_test.yaml"

    # Other shortcuts
    black = "black --check --target-version py36"
    config_dir = "./CLI_TEST_MERLIN_CONFIG"
    release_dependencies = "./requirements/release.txt"

    app_yaml_path = os.path.join(config_dir, "app.yaml")

    basic_checks = {
        "merlin": {"cmds": "merlin", "conditions": HasReturnCode(1), "run type": "local"},
        "merlin help": {"cmds": "merlin --help", "conditions": HasReturnCode(), "run type": "local"},
        "merlin version": {"cmds": "merlin --version", "conditions": HasReturnCode(), "run type": "local"},
        "merlin config create": {
            "cmds": f"merlin config --test create -o {app_yaml_path}",
            "conditions": [
                HasReturnCode(),
                PathExists(app_yaml_path),
                PathExists(os.path.join(config_dir, "config_path.txt")),
                FileHasRegex(os.path.join(config_dir, "config_path.txt"), app_yaml_path),
            ],
            "run type": "local",
            "cleanup": f"rm -rf {config_dir}",
        },
        "merlin config update-broker": {
            "cmds": f"""merlin config --test create -o {app_yaml_path}; merlin config update-broker \
                    -t rabbitmq --cf {app_yaml_path} -u rabbitmq_user --pf rabbitmq_password_file \
                    -s rabbitmq_server -p 5672 -v rabbitmq_vhost""",
            "conditions": [
                HasReturnCode(),
                FileHasRegex(app_yaml_path, "name: rabbitmq"),
                FileHasRegex(app_yaml_path, "username: rabbitmq_user"),
                FileHasRegex(app_yaml_path, "password: rabbitmq_password_file"),
                FileHasRegex(app_yaml_path, "server: rabbitmq_server"),
                FileHasRegex(app_yaml_path, "port: 5672"),
                FileHasRegex(app_yaml_path, "vhost: rabbitmq_vhost"),
            ],
            "run type": "local",
            "cleanup": f"rm -rf {config_dir}",
        },
        "merlin config update-backend": {
            "cmds": f"""merlin config --test create -o {app_yaml_path}; merlin config update-backend \
                    -t redis --cf {app_yaml_path} --pf redis_password_file -s redis_server -p 6379""",
            "conditions": [
                HasReturnCode(),
                FileHasRegex(app_yaml_path, "name: rediss"),
                FileHasRegex(app_yaml_path, "password: redis_password_file"),
                FileHasRegex(app_yaml_path, "server: redis_server"),
                FileHasRegex(app_yaml_path, "port: 6379"),
            ],
            "run type": "local",
            "cleanup": f"rm -rf {config_dir}",
        },
        "merlin config use": {  # create two app.yaml files then choose to use the first one
            "cmds": f"""merlin config --test create -o {app_yaml_path};
                    merlin config --test create -o {os.path.join(config_dir, "second_app.yaml")};
                    merlin config --test use {app_yaml_path}""",
            "conditions": [
                HasReturnCode(),
                PathExists(app_yaml_path),
                PathExists(os.path.join(config_dir, "second_app.yaml")),
                PathExists(os.path.join(config_dir, "config_path.txt")),
                FileHasRegex(os.path.join(config_dir, "config_path.txt"), app_yaml_path),
            ],
            "run type": "local",
            "cleanup": f"rm -rf {config_dir}",
        },
        "merlin info": {
            "cmds": "merlin info",
            "conditions": HasReturnCode(),
            "run type": "local",
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
            # Making sure walltime isn't set to an integer w/ last two conditions here
            "conditions": [
                HasReturnCode(),
                HasRegex(celery_slurm_regex),
                HasRegex(r"-t 36000", negate=True),
                HasRegex(r"-t 10:00:00"),
            ],
            "run type": "local",
        },
        "run-workers echo lsf_test": {
            "cmds": f"{workers_lsf} {lsf} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_lsf_regex)],
            "run type": "local",
        },
        "run-workers echo flux_test": {
            "cmds": f"{workers} {flux} --echo",
            "conditions": [HasReturnCode(), HasRegex(celery_slurm_regex)],
            "run type": "local",
        },
        "run-workers echo flux_native_test": {
            "cmds": f"{workers_flux} {flux_native} --echo",
            "conditions": [
                HasReturnCode(),
                HasRegex(celery_flux_regex),
                HasRegex(r"-t 36000"),
                HasRegex(r"-t 10:00:00", negate=True),
            ],
            "run type": "local",
        },
        "run-workers echo pbs_test": {
            "cmds": f"{workers_pbs} {flux_native} --echo",
            "conditions": [
                HasReturnCode(),
                HasRegex(celery_pbs_regex),
                HasRegex(r"-l walltime=36000", negate=True),
                HasRegex(r"-l walltime=10:00:00"),
            ],
            "run type": "local",
        },
        "run-workers echo override feature_demo": {
            "cmds": f"{workers} {demo} --echo --vars VERIFY_QUEUE=custom_verify_queue",
            "conditions": [HasReturnCode(), HasRegex("custom_verify_queue")],
            "run type": "local",
        },
        "default_worker assigned": {
            "cmds": f"{workers} {test_specs}/default_worker_test.yaml --echo",
            "conditions": [HasReturnCode(), HasRegex(r"default_worker.*-Q '\[merlin\]_step_4_queue'")],
            "run type": "local",
        },
        "no default_worker assigned": {
            "cmds": f"{workers} {test_specs}/no_default_worker_test.yaml --echo",
            "conditions": [HasReturnCode(), HasRegex(r"celery -A merlin .* default_worker", negate=True)],
            "run type": "local",
        },
        "run-workers echo variable for worker nodes": {
            "cmds": f"{workers_flux} {flux_native} --echo",
            "conditions": [HasReturnCode(), HasRegex(r"-N 4")],
            "run type": "local",
        },
    }
    wf_format_tests = {
        "local minimum_format": {
            "cmds": f"mkdir {OUTPUT_DIR}; cd {OUTPUT_DIR}; {run} ../{dev_examples}/minimum_format.yaml --local",
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
    cli_substitution_tests = {
        "no substitutions": {
            "cmds": f"merlin run {cli_substitution_wf} --local",
            "conditions": [HasReturnCode(), HasRegex(r"OUTPUT_PATH: output_path_no_substitution")],
            "run type": "local",
            "cleanup": "rm -r output_path_no_substitution",
        },
        "output_path substitution": {
            "cmds": f"merlin run {cli_substitution_wf} --local --vars OUTPUT_PATH=output_path_substitution",
            "conditions": [HasReturnCode(), HasRegex(r"OUTPUT_PATH: output_path_substitution")],
            "run type": "local",
            "cleanup": "rm -r output_path_substitution",
        },
        "output_path w/ variable substitution": {
            "cmds": f"merlin run {cli_substitution_wf} --local --vars SUB=variable_sub",
            "conditions": [HasReturnCode(), HasRegex(r"OUTPUT_PATH: output_path_variable_sub")],
            "run type": "local",
            "cleanup": "rm -r output_path_variable_sub",
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
            "conditions": [
                StepFileHasRegex(
                    "runs",
                    "*/runs.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    get_flux_cmd("flux", no_errors=True),
                ),
                ##################
                # VLAUNCHER TESTS
                ##################
                StepFileHasRegex(
                    "vlauncher_test",
                    "vlauncher_test.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"flux run -n \$\{MERLIN_PROCS\} -N \$\{MERLIN_NODES\} -c \$\{MERLIN_CORES\}",
                ),
                StepFileHasRegex(
                    "vlauncher_test_step_defaults",
                    "vlauncher_test_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_GPUS=1",
                ),
                StepFileHasRegex(
                    "vlauncher_test_step_defaults",
                    "vlauncher_test_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_NODES=6",
                ),
                StepFileHasRegex(
                    "vlauncher_test_step_defaults",
                    "vlauncher_test_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_PROCS=3",
                ),
                StepFileHasRegex(
                    "vlauncher_test_step_defaults",
                    "vlauncher_test_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_CORES=2",
                ),
                StepFileHasRegex(
                    "vlauncher_test_no_step_defaults",
                    "vlauncher_test_no_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_GPUS=0",
                ),
                StepFileHasRegex(
                    "vlauncher_test_no_step_defaults",
                    "vlauncher_test_no_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_NODES=1",
                ),
                StepFileHasRegex(
                    "vlauncher_test_no_step_defaults",
                    "vlauncher_test_no_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_PROCS=1",
                ),
                StepFileHasRegex(
                    "vlauncher_test_no_step_defaults",
                    "vlauncher_test_no_step_defaults.slurm.sh",
                    "flux_test",
                    OUTPUT_DIR,
                    r"MERLIN_CORES=1",
                ),
            ],
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
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="orig",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex=r"name: \$\(NAME\)",
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="partial",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex="studies/feature_demo_",
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="partial",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex="name: feature_demo",
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex=r"\$\(NAME\)",
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
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
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex=r"\[0.5",
                    spec_file_name="feature_demo",
                    study_name="feature_demo",
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

    # combine and return test dictionaries
    all_tests = {}
    for test_dict in [
        basic_checks,
        server_basic_tests,
        server_config_tests,
        examples_check,
        run_workers_echo_tests,
        wf_format_tests,
        cli_substitution_tests,
        example_tests,
        restart_step_tests,
        restart_wf_tests,
        dry_run_tests,
        other_local_tests,
        # provenence_equality_checks, # omitting provenance equality check because it is broken
        # style_checks, # omitting style checks due to different results on different machines
        dependency_checks,
    ]:
        all_tests.update(test_dict)

    return all_tests
