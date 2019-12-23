#!/bin/bash
#PBS -l wd,walltime=10:00:00,mem=4000GB,ncpus=1200,jobfs=5GB
#PBS -P v10
#PBS -q normal
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify PRODUCT, OUTPUT_DIR, ROOT_DIR, and FILE_LIST using qsub -v option
## eg qsub -v PRODUCT=ls7_fc_albers,OUTPUT_DIR=/odir/ls7,FILE_LIST=/odir/ls7_fc_albers.txt,ROOT_DIR=/g/foo run_cogger.sh

set -xe

source "$HOME/.bashrc"
module use /g/data/v10/public/modules/modulefiles/
module load dea
module load openmpi/4.0.1

mpirun --tag-output python3 "${ROOT_DIR}"/cog_conv_app.py mpi-convert -p "${PRODUCT}" -o "${OUTPUT_DIR}" "${FILE_LIST}"
