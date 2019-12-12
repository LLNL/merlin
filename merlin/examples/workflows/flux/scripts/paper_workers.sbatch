#!/bin/bash
#SBATCH -N 4
#SBATCH -J Merlin
#SBATCH -t 10:00
#SBATCH -A ibronze
#SBATCH -p pdebug
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

# Turn off core files to work aroung flux exec issue.
ulimit -c 0

YAML=paper.yaml

export PATH=/collab/usr/gapps/merlin/${SYS_TYPE}/bin:$PATH

merlin run ${YAML}

merlin run-workers ${YAML} --echo

merlin run-workers ${YAML}

sleep inf
