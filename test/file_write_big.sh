#!/bin/bash -ex

mountpoint="/tmp/MNT"
projectName="dxfuse_test_data"
target_dir="write_test_dir"
top_dir="$mountpoint/$projectName"
write_dir=$top_dir/$target_dir

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
sudo -E /go/bin/dxfuse -verbose 2 $mountpoint $projectName &
dxfuse_pid=$!
sleep 2

# copy files
#echo "copying small files"
#cp $top_dir/correctness/small/*  $write_dir/

# compare resulting files
#echo "comparing files"
#files=$(find $top_dir/correctness/small -type f)
#for f in $files; do
#    b_name=$(basename $f)
#    diff $f $write_dir/$b_name
#done

echo "copying large files"
#cp $top_dir/correctness/large/*  $write_dir/
cp $top_dir/correctness/large/ubuntu.tar.gz  $write_dir/

# compare resulting files
echo "comparing files"
#files=$(find $top_dir/correctness/large -type f)
#for f in $files; do
#    b_name=$(basename $f)
#    diff $f $write_dir/$b_name
#done
#diff $top_dir/correctness/large/ubuntu.tar.gz  $write_dir/ubuntu.tar.gz

#ls -l $top_dir/$target_dir

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid

dx ls -l $projectName:/$target_dir
