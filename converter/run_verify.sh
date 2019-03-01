#!/bin/bash
#PBS -l wd,walltime=1:00:00,mem=186GB,ncpus=48
#PBS -P v10
#PBS -q express
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

# specify PRODUCT,OUTPUT_DIR and FILE_LIST using qsub -v option
# eg qsub -v PRODUCT=ls7_fc_albers,OUTPUT_DIR=/g/data/v10/work/cog_conversion/development/ls7,FILE_LIST=/g/data/v10/work/cog_conversion/development/ls7_fc_albers_file_list.txt run_cogger.sh


source $HOME/.bashrc
module use /g/data/v10/public/modules/modulefiles/
module load dea/20190214
module load openmpi/3.1.2

mpirun  python cog_conv_app.py verify ${FILE_LIST}
# /g/data/v10/work/cog_conversion/development/ls5_fc_albers_file_list.txt
