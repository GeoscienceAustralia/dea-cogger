#!/usr/bin/env bash
set -eu

while [[ "$#" -gt 0 ]]; do
    key="$1"
    case "${key}" in
        --dea-module )          shift
                                MODULE="$1"
                                ;;
        --aws-profile )         shift
                                AWS_PROFILE="$1"
                                ;;
        --streamer-file )       shift
                                STREAMER_FILE="$1"
                                ;;
        --output-dir )          shift
                                OUTPUT_DIR="$1"
                                ;;
        --inventory-manifest )  shift
                                S3_INVENTORY="$1"
                                ;;
        * )
          echo "Input key, '$key', did not match the expected input argument key"
          exit 1
          ;;

    esac
    shift
done

module use /g/data/v10/public/modules/modulefiles
module load "$MODULE"

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
            [ "$word" = "[$AWS_PROFILE]" ] && exists="$word"
        done;
    done < "$filename"

    if [ -n "$exists" ]
    then
        aws configure list --profile "$AWS_PROFILE"

        cd "$OUTPUT_DIR" || {
        echo "$OUTPUT_DIR" path does not exists
        exit 1
        }

        python3 "$STREAMER_FILE" verify-cog-files --path "$OUTPUT_DIR"

        aws s3 sync "$OUTPUT_DIR" "$S3_INVENTORY" --exclude "*.pickle" --exclude "*.txt" --exclude "*file_list*"
    else
        echo "'$AWS_PROFILE' profile does not exist in '~/.aws/credentials' file"
        exit 1
    fi
else
    echo "'~/.aws/credentials' file not found"
    exit 1
fi
