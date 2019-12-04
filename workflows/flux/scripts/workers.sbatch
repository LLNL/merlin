#!/bin/bash
#SBATCH -N 1
#SBATCH --ntasks-per-node=36
#SBATCH -J Merlin
#SBATCH -t 10:00
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

# Turn off core files to work aroung flux exec issue.
ulimit -c 0

YAML=flux_par.yaml

MERLIN_PATH=<set the path to merlin here>
VENV_NAME=venv_merlin_py3_6

source ${MERLIN_PATH}/${VENV_NAME}/bin/activate

merlin run ${YAML}

merlin run-workers ${YAML} --echo

merlin run-workers ${YAML}

sleep inf
