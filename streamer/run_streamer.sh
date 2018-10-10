#!/bin/bash
module use /g/data/v10/public/modules/modulefiles
module load dea

python streamer.py generate-work-list -p ls8_fc_albers -y 2018 -m 02 | \
xargs python streamer.py convert_cog --num-procs 3 --output-dir $TMPDIR/test1 --product ls8_nbar_albers

python streamer.py upload -p  wofs_albers