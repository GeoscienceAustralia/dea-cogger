#!/usr/bin/env bash
set -eu

while [[ "$#" -gt 0 ]]; do
    key="$1"
    case "${key}" in
        --dea-module )          shift
                                MODULE="$1"
                                ;;
        --config-file )         shift
                                YAML_FILE="$1"
                                ;;
        --streamer-file )       shift
                                STREAMER_FILE="$1"
                                ;;
        --product-name )        shift
                                PRODUCT_NAME="$1"
                                ;;
        --output-dir )          shift
                                OUTPUT_DIR="$1"
                                ;;
        --datacube-env )        shift
                                DATACUBE_ENV="$1"
                                ;;
        --s3-list )             shift
                                PICKLE_FILE="$1"
                                ;;
        --time-range )          shift
                                TIME_RANGE="$*"
                                break # Last input argument and hence exiting while loop
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

cd "$OUTPUT_DIR" || {
  echo "$OUTPUT_DIR" path does not exists
  exit 1
}

python3 "$STREAMER_FILE" list-datasets -c "$YAML_FILE" --product-name "$PRODUCT_NAME" \
--output-dir "$OUTPUT_DIR" -E "$DATACUBE_ENV" --s3-list "$PICKLE_FILE" --time-range "$TIME_RANGE"
