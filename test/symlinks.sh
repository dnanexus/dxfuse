#!/bin/bash

baseDir="$HOME/dxfuse_test"
mountpoint="${baseDir}/MNT"
projName="dxfuse_test_data"
top_dir="$mountpoint/$projName"

# Get all the DX environment variables, so that dxfuse can use them
echo "loading the dx environment"

# don't leak the token to stdout
rm -f ENV
dx env --bash > ENV
source ENV >& /dev/null

# create a fresh mountpoint
mkdir -p $mountpoint

# Start the dxfuse daemon in the background, and wait for it to initilize.
echo "Mounting dxfuse"
sudo -E /go/bin/dxfuse -verbose 2 $mountpoint $projName &
dxfuse_pid=$!
sleep 2


files=$(find $top_dir/symlinks -type f)
for f in $files; do
    echo "comparing $f"
    b_name=$(basename $f)

    rm -f /tmp/B
    dx download $projName:/symlinks/$b_name -o /tmp/B
    diff $f /tmp/B
    rc=$!
    if [[ $rc == 0 ]]; then
        echo "Files match"
    else
        echo "Error: files do not match"
    fi
done

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid
