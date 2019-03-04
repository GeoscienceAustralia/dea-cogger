#!/bin/bash
#PBS -l wd,walltime=1:00:00,mem=480GB,ncpus=160
#PBS -P v10
#PBS -q normal
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

# specify PRODUCT,OUTPUT_DIR and FILE_LIST using qsub -v option
# eg qsub -v PRODUCT=ls7_fc_albers,OUTPUT_DIR=/g/data/v10/work/cog_conversion/development/ls7,FILE_LIST=/g/data/v10/work/cog_conversion/development/ls7_fc_albers_file_list.txt run_cogger.sh

set -xe

source $HOME/.bashrc
module use /g/data/v10/public/modules/modulefiles/
module load dea/20181015
module load openmpi/3.1.2

mpirun  python cog_conv_app.py mpi-convert-3 -p ${PRODUCT} -o ${OUTPUT_DIR} ${FILE_LIST}
# /g/data/v10/work/cog_conversion/development/ls5_fc_albers_file_list.txt
