#!/bin/sh

#SBATCH -N 1
#SBATCH -J MerlinBaseline
#SBATCH -t 10
#SBATCH -p pdebug
#SBATCH -A lbpm
#SBATCH --output=merlin%j.out
#SBATCH --error=merlin%j.err
#SBATCH --exclusive
#SBATCH --mpibind=off

YAML_FILE=null_chain.yaml

# print message that a new run is starting
echo "Starting new run: $SLURM_JOBID"
date

# run program
time merlin run ${YAML_FILE} --vars N_SAMPLES=$1 CONC=$2 RUN_ID=$3 N_WORK=$4 QUEUE=queue_c${4}_r${3}_chain TIME=$5
merlin run-workers ${YAML_FILE} --echo --vars N_SAMPLES=$1 CONC=$2 RUN_ID=$3 N_WORK=$4 QUEUE=queue_c${4}_r${3}_chain TIME=$5
merlin run-workers ${YAML_FILE} --vars N_SAMPLES=$1 CONC=$2 RUN_ID=$3 N_WORK=$4 QUEUE=queue_c${4}_r${3}_chain TIME=$5

sleep inf
#merlin monitor ${YAML_FILE} --vars N_SAMPLES=$1 CONC=$2 RUN_ID=$3 N_WORK=$4 QUEUE=queue_c${4}_r${3}_chain TIME=$5
