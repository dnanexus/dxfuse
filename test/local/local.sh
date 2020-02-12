#!/bin/bash -e

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "current dir=$CRNT_DIR"

#source $CRNT_DIR/xattr_test.sh
#xattr_test
#
#source $CRNT_DIR/manifest_test.sh
#manifest_test
#
#source $CRNT_DIR/dx_download_compare.sh
#dx_download_compare
#
#source $CRNT_DIR/file_write_slow.sh
#file_write_slow
#
#source $CRNT_DIR/fs_test_cases.sh
#fs_test_cases
#
#source $CRNT_DIR/faux_dirs.sh
#faux_dirs

source $CRNT_DIR/file_overwrite.sh
file_overwrite
