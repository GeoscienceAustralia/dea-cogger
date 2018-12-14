#!/bin/bash
JOBDIR=/g/data/u46/users/sm9911/tmp/wofls_cog
PRDDIR=/g/data/u46/users/sm9911/tmp/wofls_cog
SRCDIR=/g/data/u46/users/sm9911/dea_code_testing/
COGS=/g/data/u46/users/sm9911/python-env/COG-Conversion/streamer/streamer.py
YAMLFILE=/g/data/u46/users/sm9911/dea_code_testing/cog.yaml
FILEL=$JOBDIR/wofs_list_
FILEN=$JOBDIR/wofs_empty_list
NCPUS=16
MEM=31GB
JOBFS=32GB

i=1
j=1
file_count=0
find $SRCDIR -name "*.nc" | \
    while read file
    do
        SIZE=$(stat -c%s $file)
        if [ $((SIZE)) -eq 0 ]
        then
            echo $file >> $FILEN
        else
            echo $file >> $FILEL$j
            i=$((i+1))
        fi
        if [ $((i)) -gt 100000 ]
        then
            i=1
            j=$((j+1))
        fi
    done
source /g/data/u46/users/sm9911/python-env/setup_default.sh datacube_config.conf
cd $JOBDIR

j=1
f_j=$(qsub -V -P v10 -q express \
      -l walltime=1:00:00,mem=$MEM,jobfs=$JOBFS,ncpus=$NCPUS,wd \
      -- mpirun --oversubscribe -n 2 python3 $COGS mpi-convert-cog -c $YAMLFILE --output-dir $PRDDIR --product wofls --numprocs $((NCPUS-1)) $FILEL$j)

j=2
while [ -s  $FILEL$j ]; do
    n_j=$(qsub -V -W depend=afterok:$f_j -P v10 -q express \
          -l walltime=1:00:00,mem=$MEM,jobfs=$JOBFS,ncpus=$NCPUS,wd \
          -- mpirun --oversubscribe -n 2 python3 $COGS mpi-convert-cog -c $YAMLFILE --output-dir $PRDDIR --product wofls --numprocs $((NCPUS-1)) $FILEL$j)
    f_j=$n_j
    j=$((j+1))
done

#aws s3 cp $PRDDIR s3://dea-public-data-dev --recursive --exclude "*" --include "*.yaml" --include "*.tiff" --include "*.json"
