#!/bin/bash
#PBS -l wd,walltime=5:00:00,mem=3GB,jobfs=1GB
#PBS -P v10
#PBS -q copyq
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify PRODUCT, S3_INV, and OUT_DIR using qsub -v option
## eg qsub -v PRODUCT=ls8_fc_albers,OUT_DIR='/tempdir/',S3_INV='s3://bucket/' run_save_task.sh

set -xe

source "$HOME"/.bashrc
module use /g/data/v10/public/modules/modulefiles
module load dea

python3 cog_conv_app.py save-s3-inventory -p "${PRODUCT}" -o "${OUT_DIR}" -c aws_products_config.yaml
