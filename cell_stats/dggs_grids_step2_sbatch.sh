#!/bin/bash

#The job should run on the testing partition
#SBATCH -p main

#The name of the job is test_job
#SBATCH -J compute_area_zsc_hpc

## number of tasks to run, ie number of instances of your command executed in parallel
#SBATCH --ntasks=100

# CPUs per task
#SBATCH --cpus-per-task=1

#memory required per cpu/task
#SBATCH --mem-per-cpu=16GB

#The maximum walltime of the job is x minutes/hours
#SBATCH -t 48:00:00

#Notify user by email when certain events BEGIN,  END,  FAIL, REQUEUE, and ALL
#SBATCH --mail-type=ALL

#he email address where to send the notifications.
#SBATCH --mail-user=alexander.kmoch@ut.ee

module load python-3.7.1

# source activate daskgeo2020a

$HOME/.conda/envs/daskgeo2020a/bin/python compute_area_zsc_hpc.py -worklist worklist.csv -workdir /gpfs/rocket/samba/gis/kmoch/datacube_data/parquet_src

# run with sbatch --array=0-1553 dggs_grids_step2_sbatch.sh

# 8 task per node,
# 1 cpu per task
# over 7*8 mem per node

# 3 nodes
