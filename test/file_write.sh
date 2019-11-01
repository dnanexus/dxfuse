#!/bin/bash -ex

mountpoint="/tmp/MNT"
projectName="dxfuse_test_data"
content="nothing much"

dx rm $projectName:/A.txt || true

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

echo $content > $baseDir/A.txt
ls -l $baseDir/A.txt

sudo umount $mountpoint

# 1. wait for the file to achieve the closed state
file_state=$(dx describe dxfuse_test_data:/A.txt --json | grep state | awk '{ gsub("[,\"]", "", $2); print $2 }')
if [ "$file_state" != "closed" ]; then
    sleep 2
fi

dx ls -l $projectName:/A.txt

# 2. compare the data
content2=$(dx cat $projectName:/A.txt)
if [[ "$content" == "$content2" ]]; then
    echo "correct"
else
    echo "bad content"
    echo "should be: $content"
    echo "found: $content2"
fi
dx rm $projectName:/A.txt
