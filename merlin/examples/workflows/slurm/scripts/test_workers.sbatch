#!/bin/bash
#SBATCH -N 2
#SBATCH -J Merlin
#SBATCH -t 5:00
#SBATCH -A ibronze
#SBATCH -p pdebug
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

YAML=slurm_test.yaml

export PATH=/collab/usr/gapps/merlin/${SYS_TYPE}/bin:$PATH

merlin run ${YAML}

merlin run-workers ${YAML} --echo

merlin run-workers ${YAML}

sleep inf
