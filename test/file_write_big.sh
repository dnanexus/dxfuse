#!/bin/bash -ex

baseDir="$HOME/dxfuse_test"
mountpoint="${baseDir}/MNT"
projName="dxfuse_test_data"
target_dir="write_test_dir"
top_dir="$mountpoint/$projName"
write_dir=$top_dir/$target_dir

dx rm -r $projName:/$target_dir || true
dx mkdir -p $projName:/$target_dir

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
cat $top_dir/symlinks/wgEncodeUwRepliSeqBg02esS1AlnRep1.bam.bai >& /tmp/wgEncodeUwRepliSeqBg02esS1AlnRep1.bam.bai
#dx download $projName:/symlinks/wgEncodeUwRepliSeqBg02esS1AlnRep1.bam.bai


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

echo "copying large files"
#cp $top_dir/correctness/large/*  $write_dir/
t_file=wgEncodeUwRepliSeqBg02esG1bAlnRep1.bam.bai
cp $top_dir/correctness/large/$t_file $write_dir/$t_file

# compare resulting files
#echo "comparing files"
#files=$(find $top_dir/correctness/large -type f)
#for f in $files; do
#    b_name=$(basename $f)
#    diff $f $write_dir/$b_name
#done

diff $top_dir/correctness/large/$t_file $write_dir/$t_file


ls -l $top_dir/$target_dir

sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid

dx ls -l $projName:/$target_dir
