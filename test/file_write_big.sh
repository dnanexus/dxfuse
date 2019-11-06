#!/bin/bash -ex

mountpoint="/tmp/MNT"
projectName="dxfuse_test_data"
target_dir="write_test_dir"

dx rm -r $projectName:/$target_dir || true
dx mkdir -p $projectName:/$target_dir

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
sudo -E /go/bin/dxfuse -verbose 1 $mountpoint $projectName &
dxfuse_pid=$!
sleep 2

baseDir="$mountpoint/$projectName"

# copy files
echo "copying small files"
cp $baseDir/correctness/small/*  $baseDir/$target_dir/

echo "copying large files"
cp $baseDir/correctness/large/*  $baseDir/$target_dir/

ls -l $baseDir/$target_dir

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid

dx ls -l $projectName:/$target_dir
