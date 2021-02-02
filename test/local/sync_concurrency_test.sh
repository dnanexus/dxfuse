#!/bin/bash
######################################################################
# global variables

CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

mountpoint=${HOME}/MNT
projName="dxfuse_test_data"
dxfuse="$GOPATH/bin/dxfuse"
teardown_complete=0

writeable_dir=""
######################################################################

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1
    rm -f 15MB
    dx rm -r $projName:/$writeable_dir
    echo "unmounting dxfuse"
    cd $HOME
    fusermount -u $mountpoint
}

# trap any errors and cleanup
trap teardown EXIT

# Ensure that only one sync command may run at a time
function sync_concurrency_test {
    mkdir -p $mountpoint
    $dxfuse -readWrite $mountpoint $projName
    sleep 1
    dd if=/dev/urandom of=15MB bs=1M count=15
    writeable_dir=$(cat /dev/urandom | env LC_CTYPE=C LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    dx mkdir $projName:/$writeable_dir
    # Start copy and dxfuse sync
    cp 15MB $mountpoint/$projName/$writeable_dir &
    $dxfuse -sync &
    SYNC_PID=$!
    sleep 1
    # Additional sync commands fail until the above sync completes
    sync_output=$($dxfuse -sync 2>&1)

    if echo "$sync_output" | grep -q "another sync operation is already running"; then
        wait $SYNC_PID
        echo "$sync_output"
        # Sync should function again now that the first sync command has completed
        $dxfuse -sync
    else
        echo "Second sync operation did not error out"
        exit 1
    fi

    teardown
}

sync_concurrency_test