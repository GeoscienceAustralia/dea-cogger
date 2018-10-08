#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea
python streamer.py -p wofs_albers \
                   -q /g/data/u46/users/aj9439/aws/queue \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -f 0 10 \
                   -y 2018 \
                   -m 6 \
                   --use_datacube \
                   --datacube_env dea-prod \
                   --reuse_full_list \
                   --cog_only &
python streamer.py -p wofs_albers \
                   -q /g/data/u46/users/aj9439/aws/queue \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -f 0 10 \
                   -y 2018 \
                   -m 6 \
                   --use_datacube \
                   --datacube_env dea-prod \
                   --reuse_full_list \
                   --upload_only
