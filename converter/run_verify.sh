#!/bin/bash
#PBS -l wd,walltime=5:00:00,mem=186GB,ncpus=48,jobfs=1GB
#PBS -P v10
#PBS -q normal
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify OUTPUT_DIR and ROOT_DIR using qsub -v option
## eg qsub -v OUTPUT_DIR=/outdir/ls7,ROOT_DIR=/g/data/foo run_verify.sh

set -xe

source "$HOME/.bashrc"
module use /g/data/v10/public/modules/modulefiles/
module load dea
module load openmpi/3.1.2

mpirun --tag-output python3 "${ROOT_DIR}"/cog_conv_app.py verify --rm-broken "${OUTPUT_DIR}"
