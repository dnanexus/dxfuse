#!/bin/bash -e

######################################################################

teardown_complete=0

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1

    echo "syncing filesystem"
    sync

    echo "unmounting dxfuse"
    cd $HOME
    sudo umount $mountpoint
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

function main {
    local mountpoint=${HOME}/MNT
    mkdir -p $mountpoint

    sudo rm -f /var/log/dxfuse.log
    sudo -E /go/bin/dxfuse -verbose 2 -uid $(id -u) -gid $(id -g) $mountpoint manifest/two_files.json

    tree $mountpoint
    cat $mountpoint/A.txt

    teardown
}

main
