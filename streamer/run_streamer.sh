#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea/20181015

# Submit qsub
qsub /g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/job_with_gnu_parallel.pbs


# The directory that keeps COG converted datasets
OUTPUTDIR=/g/data/u46/users/aj9439/aws/tmp


# Start the long running upload process
python /g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py upload \
    --output-dir $OUTPUTDIR --u s3://dea-public-data-dev/fractional-cover/fc/v2.2.0/ls8 &