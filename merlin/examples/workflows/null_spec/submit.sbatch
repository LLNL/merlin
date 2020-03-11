#!/bin/sh

#SBATCH -N 1
#SBATCH -J MerlinBaseline
#SBATCH -t 0:10:00
#SBATCH -p pdebug
#SBATCH -A lbpm
#SBATCH --output=merlin%j.out
#SBATCH --error=merlin%j.err
#SBATCH --signal TERM@1200
# Submit this script as:
#   sbatch submit.sbatch --exclusive

YAML_DIR=/g/g13/bay1/merlin/merlin/examples/workflows/null_spec
YAML_FILE=${YAML_DIR}/null_spec.yaml
N_SAMPLES=10000

# print message that a new run is starting
echo "Starting new run: $SLURM_JOBID"
date

merlin stop-workers ;
merlin purge ${YAML_FILE} -f ;

# run program
echo "merlin run ${YAML_FILE} --vars N_SAMPLES=${N_SAMPLES} OUTPUT_PATH=${N_SAMPLES}"
merlin run ${YAML_FILE} --vars N_SAMPLES=${N_SAMPLES} OUTPUT_PATH=${N_SAMPLES}
echo "merlin run-workers ${YAML_FILE} --vars N_SAMPLES=${N_SAMPLES} OUTPUT_PATH=${N_SAMPLES}"
merlin run-workers ${YAML_FILE} --vars N_SAMPLES=${N_SAMPLES} OUTPUT_PATH=${N_SAMPLES}

sleep inf
