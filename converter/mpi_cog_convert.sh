#!/bin/bash
set -eu

while [[ "$#" -gt 0 ]]; do
    key="$1"
    case "${key}" in
        --dea-module )          shift
                                MODULE="$1"
                                ;;
        --queue )               shift
                                QUEUE="$1"
                                ;;
        --project )             shift
                                PROJECT="$1"
                                ;;
        --product )             shift
                                PRODUCT="$1"
                                ;;
        --nc-path )             shift
                                SRCDIR="$1"
                                ;;
        --output-path )         shift
                                OUTDIR="$1"
                                ;;
        --config-path )         shift
                                YAMLFILE="$1"
                                ;;
        --cog-converter-path )  shift
                                COGS="$1"
                                ;;
        * )
          echo "Input key, '$key', did not match the expected input argument key"
          exit 1
          ;;
    esac
    shift
done

FILEL=$OUTDIR/file_list_
FILEN=$OUTDIR/file_empty_list
NNODES=31
NCPUS=$((NNODES*16))
MEM=$((NNODES*16*4))GB
JOBFS=32GB

i=1
j=1
echo "" > "$FILEN"
echo "" > "$FILEL$j"
find "$SRCDIR" -name "*.nc" | \
    while read -r file
    do
        SIZE=$(stat -c%s "$file")
        if [ $((SIZE)) -eq 0 ]
        then
            echo "$file" >> "$FILEN"
        else
            echo "$file" >> "$FILEL$j"
            i=$((i+1))
        fi
        if [ $((i)) -gt $((NCPUS*20)) ]
        then
            i=1
            j=$((j+1))
            echo "" > "$FILEL$j"
        fi
    done

cd "$OUTDIR" || exit 1

j=1
f_j=$(qsub -P "$PROJECT" -q "$QUEUE" \
      -l walltime=1:00:00,mem=$MEM,jobfs=$JOBFS,ncpus=$NCPUS,wd \
      -- /bin/bash -l -c "source $HOME/.bashrc; \
      module use /g/data/v10/public/modules/modulefiles/; \
      module load ${MODULE}; \
      module load openmpi/3.1.2; \
      mpirun --tag-output --report-bindings python3 $COGS mpi-convert-cog -c $YAMLFILE --output-dir $OUTDIR \
      --product-name $PRODUCT $FILEL$j")

j=2
while [ -s  "$FILEL$j" ]; do
    n_j=$(qsub -W depend=afterany:"$f_j" -P "$PROJECT" -q "$QUEUE" \
          -l walltime=1:00:00,mem=$MEM,jobfs=$JOBFS,ncpus=$NCPUS,wd \
          -- /bin/bash -l -c "source $HOME/.bashrc; \
          module use /g/data/v10/public/modules/modulefiles/; \
          module load ${MODULE}; \
          module load openmpi/3.1.2; \
          mpirun --tag-output --report-bindings python3 $COGS mpi-convert-cog -c $YAMLFILE \
          --output-dir $OUTDIR --product-name $PRODUCT $FILEL$j")
    f_j=$n_j
    j=$((j+1))
done
