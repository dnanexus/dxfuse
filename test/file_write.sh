#!/bin/bash -ex

mountpoint="/tmp/MNT"
projectName="dxfuse_test_data"
unmountSeqDone=0

function finish {
    if [[ unmountSeqDone -eq 0 ]]; then
        echo "unmounting dxfuse"
        sudo umount $mountpoint
        unmountSeqDone=1
    fi
}
#trap finish EXIT


#main() {
# Get all the DX environment variables, so that dxfuse can use them
echo "loading the dx environment"

# don't leak the token to stdout
rm -f ENV
dx env --bash > ENV
source ENV >& /dev/null

# create a fresh mountpoint
mkdir -p $mountpoint

#    dx mkdir -p "$projectName:/write_test_dir"

# Start the dxfuse daemon in the background, and wait for it to initilize.
echo "Mounting dxfuse"
sudo -E /go/bin/dxfuse $mountpoint $projectName &
sleep 2

baseDir="$mountpoint/$projectName"

echo "nothing much" > $baseDir/A.txt
sleep 1
ls -l $baseDir/A.txt

#    sleep 3
#    finish
#}

#main
