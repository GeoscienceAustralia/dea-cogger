#!/usr/bin/env bash
# Convenience script for running CI checks.

set -eu
set -x

shopt -s globstar

pylint -j 2 --reports no \
    COG-Conversion \
    dea_cogger/*.py

# E122: 'continuation line' has too many spurious errors.
# E711: "is None" instead of "= None". Duplicates pylint check.
# E701: "multiple statements on one line" is buggy as it doesn't understand py 3 types
# E501: "line too long" duplicates pylint check
# E226: "missing whitespace around arithmetic operator" used throughout vendered cog validator
pycodestyle --ignore=E122,E711,E701,E501,E226 --max-line-length 120  \
    dea_cogger/*.py

# If yamllint is available, validate yaml documents
if command -v yamllint;
then
    set -x
    readarray -t YAML_FILES < <(find . \( -iname '*.yaml' -o -iname '*.yml' \) )
    yamllint "${YAML_FILES[@]}"
    set +x
fi
