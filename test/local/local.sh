#!/bin/bash -e

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"

source $CRNT_DIR/manifest_test.sh
manifest_test

source $CRNT_DIR/fs_test_cases.sh
fs_test_cases
