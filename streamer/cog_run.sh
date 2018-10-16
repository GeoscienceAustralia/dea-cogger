#!/usr/bin/env bash

module use /g/data/v10/public/modules/modulefiles
module load dea/20181015

cat file_list | xargs python /g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py convert-cog \
    --num-procs 4 --output-dir /g/data/u46/users/aj9439/aws/tmp --product ls8_fc_albers

