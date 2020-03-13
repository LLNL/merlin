#!/bin/bash
#SBATCH -N 2
#SBATCH -J Merlin
#SBATCH -t 5:00
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

YAML=slurm_par.yaml

MERLIN_PATH=<set the path to the merlin venv here>
VENV_NAME=venv_merlin_py3_6

# Activate the virtual environment
source ${MERLIN_PATH}/${VENV_NAME}/bin/activate

# Send all tasks to the broker
merlin run ${YAML}

# Show the workers command
merlin run-workers ${YAML} --echo

# Start workers to run the tasks in the broker
merlin run-workers ${YAML}

# Keep the allocation alive until all workers stop
merlin monitor
