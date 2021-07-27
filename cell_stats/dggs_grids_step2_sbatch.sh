#!/bin/bash

#The job should run on the testing partition
#SBATCH -p main

#The name of the job is test_job
#SBATCH -J soil_lulc_overlay

#number of different compute nodes required by the job
#SBATCH -N 2

## number of tasks to run, ie number of instances of your command executed in parallel
## off SBATCH --ntasks=36

#The job requires tasks per node
#SBATCH --ntasks-per-node=12

# CPUs per task
#SBATCH --cpus-per-task=1

#memory required per node (MB, but alternatively eg 64G)
#task*12GB (soil layer in mem)
#SBATCH --mem=12GB

#The maximum walltime of the job is x minutes/hours
#SBATCH -t 02:00:00

#Notify user by email when certain events BEGIN,  END,  FAIL, REQUEUE, and ALL
#SBATCH --mail-type=ALL

#he email address where to send the notifications.
#SBATCH --mail-user=alexander.kmoch@ut.ee

module load python-3.7.1

# source activate daskgeo2020a

$HOME/.conda/envs/daskgeo2020a/bin/python grid_soil_lulc_reduce.py

# run with sbatch --array=1-24 grid_soil_lulc_reduce_batch.sh

# 8 task per node,
# 1 cpu per task
# over 7*8 mem per node

# 3 nodes

# 