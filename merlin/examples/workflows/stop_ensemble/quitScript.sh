#!/bin/bash

#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -J stopWorkers
#SBATCH -t 00:00:05
#SBATCH -o merlin_StopWorker_%j.out

# Turn off core files to work around flux exec issue.
ulimit -c 0


specRoot=$1
targetSpecFile=$2

for JOB2CHECK in $(squeue --me --format="%F" | grep -v "ARRAY")
do
    if [[ $JOB2CHECK != $SLURM_JOB_ID ]]
    then
        if [[ ! -z $(scontrol show job $JOB2CHECK | \
	                 grep "Command=$specRoot" | \
	       	         grep -w $targetSpecFile) ]]
        then
            cancelJOB_NAME=$(squeue --me --job=$JOB2CHECK --format=%j | sed -n '2p')
            echo $cancelJOB_NAME
            echo "Job to Cancel::::Job Name: $cancelJOB_NAME  Job ID: $JOB2CHECK"
            echo "canceling job"
            scancel $JOB2CHECK
        fi
    fi
done
