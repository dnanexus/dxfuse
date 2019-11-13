#!/bin/bash -ex

mountpoint="/tmp/MNT"
projectName="dxfuse_test_data"
target_dir="write_test_dir"
top_dir="$mountpoint/$projectName"
write_dir=$top_dir/$target_dir

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

# create directory on mounted FS
mkdir $write_dir
rmdir $write_dir
mkdir $write_dir

# copy files
echo "copying small files"
cp $top_dir/correctness/small/*  $write_dir/

# compare resulting files
echo "comparing files"
files=$(find $top_dir/correctness/small -type f)
for f in $files; do
    b_name=$(basename $f)
    diff $f $write_dir/$b_name
done

echo "making empty new sub-directories"
mkdir $write_dir/E
mkdir $write_dir/F
echo "catch 22" > $write_dir/E/Z.txt

tree $write_dir

echo "letting the files complete uploading"
sleep 10
dx ls -l $projectName:/$target_dir

echo "removing directory recursively"
rm -rf $write_dir

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid
