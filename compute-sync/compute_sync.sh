#!/bin/bash
#PBS -q copyq
#PBS -l walltime=10:00:00
#PBS -l ncpus=1,mem=31GB
#PBS -l wd

export ME=/g/data/u46/users/hr8696
module use /g/data/v10/public/modules/modulefiles/

module load agdc-py3-prod

aws s3 sync /g/data/u46/users/hr8696/netcdf-wofs-conv/ewater/cambodia_cube/output_2017/ls_level2/ s3://dea-public-data/ewater/cambodia_cube/output_2017/ls_level2/ --exclude '*.aux.xml'


