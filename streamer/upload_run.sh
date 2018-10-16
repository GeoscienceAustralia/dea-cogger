#!/usr/bin/env bash

module use /g/data/v10/public/modules/modulefiles
module load dea/20181015

python /g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py upload \
    --output-dir /g/data/u46/users/aj9439/aws/tmp \
    --upload-destination s3://dea-public-data-dev/WOfS/filtered_summary/v2.1.0/combined
