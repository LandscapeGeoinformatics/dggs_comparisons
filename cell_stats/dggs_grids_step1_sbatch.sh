#!/bin/bash

#The job should run on the testing partition
#SBATCH -p main

#The name of the job is test_job
#SBATCH -J generate_cells_healpy

## number of tasks to run, ie number of instances of your command executed in parallel
#SBATCH --ntasks=1

# CPUs per task
#SBATCH --cpus-per-task=1

#memory required per cpu/task
#SBATCH --mem-per-cpu=16GB

#The maximum walltime of the job is x minutes/hours
#SBATCH -t 12:00:00

#Notify user by email when certain events BEGIN,  END,  FAIL, REQUEUE, and ALL
#SBATCH --mail-type=ALL

#he email address where to send the notifications.
#SBATCH --mail-user=alexander.kmoch@ut.ee

module load any/python/3.8.3-conda

#  conda activate geo2024

GOOGLE_APPLICATION_CREDENTIALS=/data/home/ubuntu/install/stac_stuff/stac_testdata/lgeo-bucket-access.json

CONDA_PROMPT_MODIFIER=(geo2024)
CONDA_EXE=/gpfs/space/software/cluster_software/manual/any/python/conda/3.8/bin/conda
CONDA_PREFIX=/gpfs/space/home/kmoch/.conda/envs/geo2024
CONDA_PYTHON_EXE=/gpfs/space/software/cluster_software/manual/any/python/conda/3.8/bin/python
CONDA_DEFAULT_ENV=geo2024
GDAL_DRIVER_PATH=/gpfs/space/home/kmoch/.conda/envs/geo2024/lib/gdalplugins
GDAL_DATA=/gpfs/space/home/kmoch/.conda/envs/geo2024/share/gdal
PROJ_DATA=/gpfs/space/home/kmoch/.conda/envs/geo2024/share/proj
PROJ_NETWORK=ON

WORKDIR=/gpfs/space/home/kmoch/dggs-dev/dggs_comparisons/cell_stats
cd $WORKDIR

$HOME/.conda/envs/geo2024/bin/python generate_cells_parquet.py -action cells -config config-healpy.json -cpu 1 -dggrid "none" -out results_healpy


# run with sbatch --array=0-1553 dggs_grids_step1_sbatch.sh

# 8 task per node,
# 1 cpu per task
# over 7*8 mem per node

# 3 nodes
