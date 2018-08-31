#!/bin/bash

python streamer.py -q /g/data/u46/users/aj9439/aws/queue \
                   -d s3://dea-public-data-dev/streamer \
                   -j /g/data/u46/users/aj9439/aws/job \
                   /g/data/u46/users/aj9439/aws/src