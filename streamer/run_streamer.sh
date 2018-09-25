#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea
python streamer.py -p ls7_fc_albers \
                   -q /g/data/u46/users/aj9439/aws/queue \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -f 0 3 \
                   -y 2005 \
                   --use_datacube \
                   --reuse_full_list
