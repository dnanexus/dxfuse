#!/bin/bash -ex

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


echo "stream one file"
cat $top_dir/symlinks/1000G_2504_high_coverage.sequence.index > /tmp/A
dx download $projName:/symlinks/1000G_2504_high_coverage.sequence.index -o /tmp/B

diff /tmp/A /tmp/B

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid
