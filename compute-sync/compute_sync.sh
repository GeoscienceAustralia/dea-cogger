#!/bin/bash
#PBS -q copyq
#PBS -l walltime=10:00:00
#PBS -l ncpus=1,mem=31GB
#PBS -l wd


module use /g/data/v10/public/modules/modulefiles/
module load agdc-py3-prod

aws s3 sync /g/data/u46/ s3://dea-public-data-dev/ --exclude '*.aux.xml'


