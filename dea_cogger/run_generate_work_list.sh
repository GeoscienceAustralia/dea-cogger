#!/bin/bash
#PBS -l wd,walltime=5:00:00,mem=31GB,ncpus=16,jobfs=1GB
#PBS -P v10
#PBS -q normal
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify PRODUCT_NAME, OUTPUT_DIR, TIME_RANGE, ROOT_DIR, and PICKLE_FILE using qsub -v option
## eg qsub -v PRODUCT_NAME=ls7_fc_albers,OUTPUT_DIR=/outdir/ls7,TIME_RANGE='1987-01-01 < time < 2019-03-10',ROOT_DIR=/COG-Conversion/dea_cogger/,PICKLE_FILE=/dir/ls7_fc_albers_s3_inv_list.pickle run_generate_work_list.sh

set -xe

source "$HOME/.bashrc"
module use /g/data/v10/public/modules/modulefiles/
module load dea
module load openmpi/3.1.2

cd "$OUTPUT_DIR" || {
  echo "$OUTPUT_DIR" path does not exists
  exit 1
}

python3 "${ROOT_DIR}"/cog_conv_app.py generate-work-list --config "${ROOT_DIR}"/aws_products_config.yaml \
--product-name "$PRODUCT_NAME" --time-range "$TIME_RANGE" --output-dir "$OUTPUT_DIR" --pickle-file "$PICKLE_FILE"
