#!/bin/bash -e

mountpoint="/tmp/MNT"
projName="dxfuse_test_data"
target_dir="write_test_dir"
top_dir="$mountpoint/$projName"

function run_sambamba {
    local bamfile=$1

    sambamba flagstat -t `nproc` $bamfile
}



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
sudo -E /go/bin/dxfuse -verbose 1 $mountpoint $projName &
dxfuse_pid=$!
sleep 2

run_sambamba $top_dir/symlinks/wgEncodeUwRepliSeqBg02esS1AlnRep1.bam

# unmount cleanly
cd $HOME
sudo umount $mountpoint

# wait until the filesystem is done running
wait $dxfuse_pid
