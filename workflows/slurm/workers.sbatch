#!/bin/bash
#SBATCH -N 2
#SBATCH -J Merlin
#SBATCH -t 5:00
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

YAML=slurm_par.yaml

MERLIN_PATH=<set the path to merlin here>
VENV_NAME=venv_merlin_py3_6

source ${MERLIN_PATH}/${VENV_NAME}/bin/activate

merlin run ${YAML}

merlin run-workers ${YAML} --echo

merlin run-workers ${YAML}

sleep inf
