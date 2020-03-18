#!/bin/bash
#SBATCH -N 2
#SBATCH -J Merlin
#SBATCH -t 5:00
#SBATCH -A ibronze
#SBATCH -p pdebug
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

YAML=slurm_test.yaml

# When using the spack install, set an environment
# variable called MERLIN_PATH with the path to merlin
export PATH=${MERLIN_PATH}:$PATH

# Send all tasks to the broker
merlin run ${YAML}

# Show the workers command
merlin run-workers ${YAML} --echo

# Start workers to run the tasks in the broker
merlin run-workers ${YAML}

# Keep the allocation alive until all workers stop
merlin monitor
