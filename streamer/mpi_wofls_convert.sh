#!/bin/bash
JOBDIR=$PWD
PRDDIR=/g/data/v10/users/ea6141/wofls_cog
SRCDIR=/g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/
COGS=/home/547/ea6141/datafile/COG-Conversion/streamer/streamer.py
FILEL=wofs_list
FILEN=wofs_empty_list
NCPUS=128
MEM=512GB
JOBFS=32GB

i=1
j=1
find $SRCDIR -name "*.nc" | \
    while read file
    do
        SIZE=$(stat -c%s $file) 
        if [ $((SIZE)) -eq 0 ]
        then
            echo $file >> $FILEN
        else
            echo $file >> $FILEL$((j))
            i=$((i+1))
            #echo $((i))
        fi
        if [ $((i)) -gt 100000 ]
        then
            i=1
            j=$((j+1))
        fi
    done

j=1
f_j=$(qsub -P v10 -q normal -l walltime=48:00:00,mem=$MEM,jobfs=$JOBFS,ncpus=$NCPUS,wd --  \
    bash -l -c "\
    source $HOME/.bashrc; cd $JOBDIR;\
    mpirun --oversubscribe -n 1 python3 $COGS mpi_convert_cog -c cog_fcp.yaml --output-dir $PRDDIR --product wofls --numprocs $((NCPUS-1)) --cog-path $COGS $FILEL$((j))")

j=2
while
    n_j=$(qsub -W depend=afterany:$f_j -P v10 -q normal -l walltime=48:00:00,mem=$MEM,jobfs=$JOBFS,ncpus=$NCPUS,wd -- \
        bash -l -c "\
        source $HOME/.bashrc; cd $JOBDIR;\
        mpirun --oversubscribe -n 1 python3 $COGS mpi_convert_cog -c cog_fcp.yaml --output-dir $PRDDIR --product wofls --numprocs $((NCPUS-1)) --cog-path $COGS $FILEL$((j))")
    f_j=$n_j
    j=$((j+1))
    [ -s  $FILEL$((j)) ]
do :; done
