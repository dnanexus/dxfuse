#!/bin/bash -e

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"

$CRNT_DIR/xattr_test.sh

$CRNT_DIR/manifest_test.sh

$CRNT_DIR/dx_download_compare.sh

$CRNT_DIR/file_write_slow.sh

$CRNT_DIR/fs_test_cases.sh

$CRNT_DIR/faux_dirs.sh

$CRNT_DIR/file_overwrite.sh
