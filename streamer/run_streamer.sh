#!/bin/bash

python streamer.py -p fc-ls5 \
                   -q /g/data/u46/users/aj9439/aws/queue \
                   -b s3://dea-public-data-dev/streamer \
                   -j /g/data/u46/users/aj9439/aws/job \
                   -y 2018 \
                   -m 4 \
                   -s /g/data/u46/users/aj9439/aws/tests