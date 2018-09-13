#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea
python streamer.py -p wofs-wofls \
                   -q /g/data/u46/users/aj9439/aws/queue \
                   -b s3://dea-public-data-dev/streamer \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -y 2018 \
                   -m 5 \
                   -l 10 \
                   --reuse_full_list
