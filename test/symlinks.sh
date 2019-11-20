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


trg_dir="${baseDir}/dxCopySymlinks"
dxfuseDir="$top_dir/symlinks"
rm -rf $trg_dir
mkdir -p $trg_dir

dx download --no-progress -o $trg_dir -r  $projName:/symlinks
diff -r --brief $dxfuseDir/symlinks $trg_dir > diff.txt || true
if [[ -s diff.txt ]]; then
    echo "Difference in symlink content"
    cat diff.txt
    exit 1
fi

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid
