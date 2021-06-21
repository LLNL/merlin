from conditions import (
    HasRegex,
    HasReturnCode,
    ProvenanceYAMLFileHasRegex,
    StepFileExists,
    StepFileHasRegex,
)
from merlin.utils import get_flux_cmd


OUTPUT_DIR = "cli_test_studies"


def define_tests():
    """
    Returns a dictionary of tests, where the key
    is the test's name, and the value is a tuple
    of (shell command, condition(s) to satisfy).
    """
    celery_regex = r"(srun\s+.*)?celery\s+(-A|--app)\s+merlin\s+worker\s+.*"

    # shortcut string variables
    workers = "merlin run-workers"
    run = "merlin run"
    restart = "merlin restart"
    purge = "merlin purge"
    examples = "merlin/examples/workflows"
    dev_examples = "merlin/examples/dev_workflows"
    demo = f"{examples}/feature_demo/feature_demo.yaml"
    demo_pgen = f"{examples}/feature_demo/scripts/pgen.py"
    simple = f"{examples}/simple_chain/simple_chain.yaml"
    slurm = f"{examples}/slurm/slurm_test.yaml"
    slurm_restart = f"{examples}/slurm/slurm_par_restart.yaml"
    flux = f"{examples}/flux/flux_test.yaml"
    flux_restart = f"{examples}/flux/flux_par_restart.yaml"
    lsf = f"{examples}/lsf/lsf_par.yaml"
    black = "black --check --target-version py36"
    config_dir = "./CLI_TEST_MERLIN_CONFIG"

    return {
        "merlin": ("merlin", HasReturnCode(1), "local"),
        "merlin help": ("merlin --help", HasReturnCode(), "local"),
        "merlin version": ("merlin --version", HasReturnCode(), "local"),
        "merlin config": (
            f"merlin config -o {config_dir}; rm -rf {config_dir}",
            HasReturnCode(),
            "local",
        ),
        "local minimum_format": (
            f"mkdir {OUTPUT_DIR} ; cd {OUTPUT_DIR} ; merlin run ../{dev_examples}/minimum_format.yaml --local",
            StepFileExists(
                "step1",
                "MERLIN_FINISHED",
                "minimum_format",
                OUTPUT_DIR,
                params=False,
            ),
            "local",
        ),
        "local no_description": (
            f"mkdir {OUTPUT_DIR} ; cd {OUTPUT_DIR} ; merlin run ../merlin/examples/dev_workflows/no_description.yaml --local",
            HasReturnCode(1),
            "local",
        ),
        "local no_steps": (
            f"mkdir {OUTPUT_DIR} ; cd {OUTPUT_DIR} ; merlin run ../merlin/examples/dev_workflows/no_steps.yaml --local",
            HasReturnCode(1),
            "local",
        ),
        "local no_study": (
            f"mkdir {OUTPUT_DIR} ; cd {OUTPUT_DIR} ; merlin run ../merlin/examples/dev_workflows/no_study.yaml --local",
            HasReturnCode(1),
            "local",
        ),
        "run-workers echo simple_chain": (
            f"{workers} {simple} --echo",
            [HasReturnCode(), HasRegex(celery_regex)],
            "local",
        ),
        "run-workers echo feature_demo": (
            f"{workers} {demo} --echo",
            [HasReturnCode(), HasRegex(celery_regex)],
            "local",
        ),
        "run-workers echo slurm_test": (
            f"{workers} {slurm} --echo",
            [HasReturnCode(), HasRegex(celery_regex)],
            "local",
        ),
        "run-workers echo flux_test": (
            f"{workers} {flux} --echo",
            [HasReturnCode(), HasRegex(celery_regex)],
            "local",
        ),
        "run-workers echo override feature_demo": (
            f"{workers} {demo} --echo --vars VERIFY_QUEUE=custom_verify_queue",
            [HasReturnCode(), HasRegex("custom_verify_queue")],
            "local",
        ),
        "run feature_demo": (f"{run} {demo}", HasReturnCode()),
        "purge feature_demo": (f"{purge} {demo} -f", HasReturnCode()),
        "dry feature_demo": (
            f"{run} {demo} --local --dry --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            [
                StepFileExists(
                    "verify",
                    "verify_*.sh",
                    "feature_demo",
                    OUTPUT_DIR,
                    params=True,
                ),
                HasReturnCode(),
            ],
            "local",
        ),
        "restart local simple_chain": (
            f"{run} {simple} --local --vars OUTPUT_PATH=./{OUTPUT_DIR} ; {restart} $(find ./{OUTPUT_DIR} -type d -name 'simple_chain_*') --local",
            HasReturnCode(),
            "local",
        ),
        "local simple_chain": (
            f"{run} {simple} --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            HasReturnCode(),
            "local",
        ),
        "local restart": (
            f"{run} {dev_examples}/restart.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileExists(
                "final_check_for_no_hard_fails",
                "MERLIN_FINISHED",
                "restart",
                OUTPUT_DIR,
                params=False,
            ),
            "local",
        ),
        "local restart_shell": (
            f"{run} {dev_examples}/restart_shell.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileExists(
                "step2",
                "MERLIN_FINISHED",
                "restart_shell",
                OUTPUT_DIR,
                params=False,
            ),
            "local",
        ),
        "example failure": (f"merlin example failure", HasRegex("not found"), "local"),
        "example simple_chain": (
            f"merlin example simple_chain ; {run} simple_chain.yaml --local --vars OUTPUT_PATH=./{OUTPUT_DIR} ; rm simple_chain.yaml",
            HasReturnCode(),
            "local",
        ),
        "dry launch slurm": (
            f"{run} {slurm} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileHasRegex(
                "runs", "*/runs.slurm.sh", "slurm_test", OUTPUT_DIR, "srun "
            ),
            "local",
        ),
        "dry launch flux": (
            f"{run} {flux} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileHasRegex(
                "runs",
                "*/runs.slurm.sh",
                "flux_test",
                OUTPUT_DIR,
                get_flux_cmd("flux", no_errors=True),
            ),
            "local",
        ),
        "dry launch lsf": (
            f"{run} {lsf} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileHasRegex(
                "runs", "*/runs.slurm.sh", "lsf_par", OUTPUT_DIR, "jsrun "
            ),
            "local",
        ),
        "dry launch slurm restart": (
            f"{run} {slurm_restart} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileHasRegex(
                "runs",
                "*/runs.restart.slurm.sh",
                "slurm_par_restart",
                OUTPUT_DIR,
                "srun ",
            ),
            "local",
        ),
        "dry launch flux restart": (
            f"{run} {flux_restart} --dry --local --no-errors --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR}",
            StepFileHasRegex(
                "runs_rs",
                "*/runs_rs.restart.slurm.sh",
                "flux_par_restart",
                OUTPUT_DIR,
                get_flux_cmd("flux", no_errors=True),
            ),
            "local",
        ),
        "local override feature_demo": (
            f"{run} {demo} --vars N_SAMPLES=2 OUTPUT_PATH=./{OUTPUT_DIR} --local",
            [
                HasReturnCode(),
                ProvenanceYAMLFileHasRegex(
                    regex="HELLO: \$\(SCRIPTS\)/hello_world.py",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="orig",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex="name: \$\(NAME\)",
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
                    regex="\$\(NAME\)",
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
            "local",
        ),
        # "local restart expand name": (
        #    f"{run} {demo} --local --vars OUTPUT_PATH=./{OUTPUT_DIR} NAME=test_demo ; {restart} $(find ./{OUTPUT_DIR} -type d -name 'test_demo_*') --local",
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
        "local csv feature_demo": (
            f"echo 42.0,47.0 > foo_testing_temp.csv; {run} {demo} --samplesfile foo_testing_temp.csv --vars OUTPUT_PATH=./{OUTPUT_DIR} --local; rm -f foo_testing_temp.csv",
            [HasRegex("1 sample loaded."), HasReturnCode()],
            "local",
        ),
        "local tab feature_demo": (
            f"echo '42.0\t47.0\n7.0 5.3' > foo_testing_temp.tab; {run} {demo} --samplesfile foo_testing_temp.tab --vars OUTPUT_PATH=./{OUTPUT_DIR} --local; rm -f foo_testing_temp.tab",
            [HasRegex("2 samples loaded."), HasReturnCode()],
            "local",
        ),
        "local pgen feature_demo": (
            f"{run} {demo} --pgen {demo_pgen} --vars OUTPUT_PATH=./{OUTPUT_DIR} --local",
            [
                ProvenanceYAMLFileHasRegex(
                    regex="\[0.3333333",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                ),
                ProvenanceYAMLFileHasRegex(
                    regex="\[0.5",
                    name="feature_demo",
                    output_path=OUTPUT_DIR,
                    provenance_type="expanded",
                    negate=True,
                ),
                HasReturnCode(),
            ],
            "local",
        ),
        # "local provenance spec equality": (
        #     f"{run} {simple} --vars OUTPUT_PATH=./{OUTPUT_DIR} --local ; cp $(find ./{OUTPUT_DIR}/simple_chain_*/merlin_info -type f -name 'simple_chain.expanded.yaml') ./{OUTPUT_DIR}/FILE1 ; rm -rf ./{OUTPUT_DIR}/simple_chain_* ; {run} ./{OUTPUT_DIR}/FILE1 --vars OUTPUT_PATH=./{OUTPUT_DIR} --local ; cmp ./{OUTPUT_DIR}/FILE1 $(find ./{OUTPUT_DIR}/simple_chain_*/merlin_info -type f -name 'simple_chain.expanded.yaml')",
        #     HasReturnCode(),
        #     "local",
        # ),
        # "black check merlin": (f"{black} merlin/", HasReturnCode(), "local"),
        # "black check tests": (f"{black} tests/", HasReturnCode(), "local"),
        "deplic no GNU": (
            f"deplic ./",
            [HasRegex("GNU", negate=True), HasRegex("GPL", negate=True)],
            "local",
        ),
        "distributed feature_demo": (
            f"{run} {demo} --vars OUTPUT_PATH=./{OUTPUT_DIR} WORKER_NAME=cli_test_demo_workers ; {workers} {demo} --vars OUTPUT_PATH=./{OUTPUT_DIR} WORKER_NAME=cli_test_demo_workers",
            [
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
        ),
    }
