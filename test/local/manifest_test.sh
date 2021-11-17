#!/bin/bash 
######################################################################
# global variables

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mountpoint=${HOME}/MNT
projName="dxfuse_test_data"
dxfuse="$CRNT_DIR/../../dxfuse"
teardown_complete=0

######################################################################

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1

    rm -f cmd_results.txt

    echo "unmounting dxfuse"
    cd $HOME
    fusermount -u $mountpoint
}

# trap any errors and cleanup
trap teardown EXIT

function manifest_test {
    mkdir -p $mountpoint

    $dxfuse $mountpoint $CRNT_DIR/two_files.json
    sleep 1

    tree $mountpoint
    full_path=/correctness/small/A.txt
    local content=$(cat $mountpoint/A.txt)
    local content_dx=$(dx cat $projName:$full_path)

    if [[ "$content" == "$content_dx" ]]; then
        echo "$full_path +"
    else
        echo "file $full_path has incorrect content"
        exit 1
    fi

    teardown
}
