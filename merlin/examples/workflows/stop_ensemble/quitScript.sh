#!/bin/bash

#SBATCH -N 1
#SBATCH --ntasks-per-node=1
#SBATCH -J stopWorkers
#SBATCH -t 00:01:00
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
	                 grep -w "WorkDir=$specRoot") ]]
        then
            outFile=$(scontrol show job $JOB2CHECK | \
	                  grep "StdOut=")
            outFile=${outFile##*=}

            foundSpec=$(grep 'Specification File: ' $outFile | sed 's/^.*: //')

            if [[ $targetSpecFile == $foundSpec ]]
            then
	        cancelJOB_NAME=$(squeue --me --job=$JOB2CHECK --format=%j | sed -n '2p')
                echo $cancelJOB_NAME
                echo "Job to Cancel::::Job Name: $cancelJOB_NAME  Job ID: $JOB2CHECK"
                echo "canceling job"
                scancel $JOB2CHECK
            fi
        fi
    fi
done
