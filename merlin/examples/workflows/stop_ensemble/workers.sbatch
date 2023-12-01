#!/bin/bash

#SBATCH -N 1
#SBATCH --ntasks-per-node=36
#SBATCH -J stopEnsembleExample
#SBATCH -t 00:10:00
#SBATCH -o merlin_workers_%j.out
#SBATCH -p pdebug

# Turn off core files to work around flux exec issue.
ulimit -c 0

doDaisyChain=false
YAML=default

# Get this filename
WORKER_SCRIPT=$(scontrol show job $SLURM_JOB_ID | grep -hi "Command=" | xargs -0 basename)

if [[ $# -gt 0 ]]
then
    YAML=$1
    
    if [[ $# -gt 1 ]]
    then
        if [[ $2 == 1 || $2 == true || $2 == True || $2 == yes || $2 == Yes ]]
        then
	    doDaisyChain=true
        fi
    fi
fi
       

echo "Spec Filename: $YAML"

MERLIN_PATH=/usr/workspace/cliche1/MERLIN
VENV_NAME=merlin_venv_toss_3_x86_64_ib_py_3_8

# Activate the virtual environment
source ${MERLIN_PATH}/${VENV_NAME}/bin/activate

# Show the workers command
merlin run-workers ${YAML} --echo

# Start workers to run the tasks in the broker
merlin run-workers ${YAML}

# Check that the YAML file was correct. Otherwise cancel job
outFile=$(scontrol show job $SLURM_JOB_ID | grep -i 'stdout=')
outFile=${outFile#*=}
isBadYAML=$(head -n 1 $outFile | grep -i "is not a valid filepath" | wc -l)
if [[ isBadYAML -eq 1 ]]
then
    echo "Incorrect YAML File: $YAML"
    echo "Canceling Job"
    scancel $SLURM_JOB_ID
fi


# Start dependency job if this one times out
# The new job name will contain the job id of the job that
# created the depenendency
if [[ $doDaisyChain = true ]]
then
    JOB_NAME_BASE=${SLURM_JOB_NAME%%-d-*}
    JOB_NAME="$JOB_NAME_BASE-d-${SLURM_JOB_ID}"
    sbatch -J $JOB_NAME --depend=afterany:${SLURM_JOB_ID} ${WORKER_SCRIPT} $YAML $doDaisyChain
fi
    
# Keep the allocation alive
sleep inf


