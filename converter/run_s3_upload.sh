#!/bin/bash
#PBS -l wd,walltime=10:00:00,mem=3GB,jobfs=1GB
#PBS -P v10
#PBS -q copyq
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify AWS_PROFILE, OUT_DIR, and S3_BUCKET using qsub -v option
## eg qsub -v AWS_PROFILE='default',OUT_DIR=/tempdir/,S3_BUCKET='s3://bucketname/' run_s3_upload.sh

set -xe

source "$HOME"/.bashrc
module use /g/data/v10/public/modules/modulefiles
module load dea

# If aws cli is not installed, exit with command command not found status
aws --version 2> /dev/null || exit 1;

filename=~/.aws/credentials

# -s: Returns true if file exists and has a size > 0
if [[ -s ~/.aws/credentials ]]; then
    exists=
    while read -ra line;
    do
        for word in "${line[@]}";
        do
            [[ "$word" = "[$AWS_PROFILE]" ]] && exists="$word"
        done;
    done < "$filename"

    if [[ -n "$exists" ]]
    then
        aws configure list --profile "$AWS_PROFILE"

        cd "$OUT_DIR" || {
        echo "$OUT_DIR" path does not exists
        exit 1
        }

        # Recursively sync all the files under a specified directory to S3 bucket excluding specified file formats
        aws s3 sync "$OUT_DIR" "$S3_BUCKET" --exclude "*.pickle" --exclude "*.txt" --exclude "*file_list*" \
        --exclude "*.log"

        # Remove cog converted files after aws s3 sync
        rm -r "$OUT_DIR"
    else
        echo "'$AWS_PROFILE' profile does not exist in '~/.aws/credentials' file"
        exit 1
    fi
else
    echo "'~/.aws/credentials' file not found"
    exit 1
fi
