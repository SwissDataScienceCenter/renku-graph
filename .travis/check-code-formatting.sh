#!/bin/bash

RESULTS_FILE=$(mktemp)
DIFF=$(git diff --exit-code > $RESULTS_FILE; echo $?)

echo "Running git diff on $(pwd) to check if scalariform changed code..."

if [ $DIFF -eq 0 ]; then
    echo "No diff on source code"
    rm $RESULTS_FILE
else
    echo "Changes detected, code was probably not formatted before commit:"
    cat $RESULTS_FILE
    rm $RESULTS_FILE
    exit 1
fi
