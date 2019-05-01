#!/bin/bash
#PBS -l wd,walltime=5:00:00,mem=3GB,jobfs=1GB
#PBS -P v10
#PBS -q copyq
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify PRODUCT, S3_INV, ROOT_DIR, and OUT_DIR using qsub -v option
## eg qsub -v PRODUCT=ls8_fc_albers,OUT_DIR='/tempdir/',S3_INV='s3://bucket/',ROOT_DIR=/g/data/foo run_save_task.sh

set -xe

source "$HOME"/.bashrc
module use /g/data/v10/public/modules/modulefiles
module load dea

python3 "${ROOT_DIR}"/cog_conv_app.py save-s3-inventory -p "${PRODUCT}" -o "${OUT_DIR}" \
-c "${ROOT_DIR}"/aws_products_config.yaml
