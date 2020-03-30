#!/bin/sh

#SBATCH -N 2
#SBATCH -J MerlinBaseline
#SBATCH -t 0:10:00
#SBATCH -p pdebug
#SBATCH -A lbpm
#SBATCH --output=merlin%j.out
#SBATCH --error=merlin%j.err
#SBATCH --exclusive
#SBATCH --mpibind=off

SCRIPTPATH=`pwd`
YAML_FILE=${SCRIPTPATH}/../../../../../null_spec.yaml

# print message that a new run is starting
echo "Starting new run: $SLURM_JOBID"
date

#merlin stop-workers ;
#merlin purge ${YAML_FILE} -f ;

# run program
time merlin run ${YAML_FILE} --vars N_SAMPLES=$1 CONC=$2
merlin run-workers ${YAML_FILE} --vars N_SAMPLES=$1 CONC=$2

merlin monitor
