#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

shopt -s globstar

pylint -j 2 --reports no \
    COG-Conversion \
    streamer/*.py

# E122: 'continuation line' has too many spurious errors.
# E711: "is None" instead of "= None". Duplicates pylint check.
# E701: "multiple statements on one line" is buggy as it doesn't understand py 3 types
# E501: "line too long" duplicates pylint check
pycodestyle --ignore=E122,E711,E701,E501 --max-line-length 120  \
    streamer/*.py

# Finds shell scripts based on #!
readarray -t SHELL_SCRIPTS < <(find streamer/*.sh -type f -exec file {} \; | grep "streamer" | cut -d: -f1)
shellcheck -e SC1071,SC1090,SC1091 "${SHELL_SCRIPTS[@]}"

# If yamllint is available, validate yaml documents
if command -v yamllint;
then
    set -x
    readarray -t YAML_FILES < <(find . \( -iname '*.yaml' -o -iname '*.yml' \) )
    yamllint "${YAML_FILES[@]}"
    set +x
fi
