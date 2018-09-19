#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea
python streamer.py -p wofs_albers \
                   -q /g/data/u46/users/aj9439/aws/queue \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -y 2018 \
                   -m 5 \
                   -f 0 4 \
                   --restart
