#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea
python streamer.py -p ls8_fc_albers \
                   -q /short/v10/streamer_queue/ \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -f 0 3 \
                   -y 2018 \
                   -m 5 \
                   --use_datacube \
                   --datacube_env dea_prod \
                   --reuse_full_list \
                   --cog_only &
python streamer.py -p ls8_fc_albers \
                   -q /short/v10/streamer_queue/ \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -f 0 3 \
                   -y 2018 \
                   -m 5 \
                   --use_datacube \
                   --datacube_env dea_prod \
                   --reuse_full_list \
                   --upload_only
