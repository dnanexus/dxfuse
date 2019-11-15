#!/bin/bash -e

mountpoint="/tmp/MNT"
projName="dxfuse_test_data"
target_dir="write_test_dir"
top_dir="$mountpoint/$projName"


function file_create_existing {
    local write_dir=$1
    cd $write_dir

    echo "happy days" > hello.txt

    set +e
    (echo "nothing much" > hello.txt) >& /tmp/cmd_results.txt
    rc=$?
    set -e

    if [[ $rc == 0 ]]; then
        echo "Error, could modify an existing file"
        exit 1
    fi
    result=$(cat /tmp/cmd_results.txt)
    if [[ ( ! $result =~ "Permission denied" ) && ( ! $result =~ "Operation not permitted" ) ]]; then
        echo "Error, incorrect command results"
        cat /tmp/cmd_results.txt
        exit 1
    fi

    rm -f hello.txt
}

function file_remove_non_exist {
    local write_dir=$1
    cd $write_dir

    set +e
    (rm hello.txt) >& /tmp/cmd_results.txt
    rc=$?
    set -e

    if [[ $rc == 0 ]]; then
        echo "Error, could remove a non-existent file"
        exit 1
    fi
    result=$(cat /tmp/cmd_results.txt)
    if [[ ! $result =~ "No such file or directory" ]]; then
        echo "Error, incorrect command results"
        cat /tmp/cmd_results.txt
        exit 1
    fi

}

# Get all the DX environment variables, so that dxfuse can use them
echo "loading the dx environment"

# don't leak the token to stdout
rm -f ENV
dx env --bash > ENV
source ENV >& /dev/null

dx rm -r $projName:/$target_dir >& /dev/null || true
dx mkdir $projName:/$target_dir
dx rm -f $projName:/hello.txt >& /dev/null || true

# create a fresh mountpoint
mkdir -p $mountpoint

# Start the dxfuse daemon in the background, and wait for it to initilize.
echo "Mounting dxfuse"
sudo -E /go/bin/dxfuse -verbose 1 $mountpoint $projName &
dxfuse_pid=$!
sleep 2

echo "file create remove"
file_create_existing "$mountpoint/$projName"
file_remove_non_exist "$mountpoint/$projName"

# unmount cleanly
cd $HOME
sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid
