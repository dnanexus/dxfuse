#!/bin/bash -ex

TOTAL_NUM_FILES=4

mountpoint="/tmp/MNT"
projectName="dxfuse_test_data"
target_dir="/tmp/write_test_dir"
baseDir="$mountpoint/$projectName"

# copy a bunch of files in parallel
function check_parallel_cat {
    top_dir="$baseDir/reference_data"
    all_files=$(find $top_dir -type f)

    # limit the number of files in the test
    num_files=0
    files=""
    for f in $all_files; do
        # limit the number of files in the test
        num_files=$((num_files + 1))
        files="$files $f"
        if [[ $num_files == $TOTAL_NUM_FILES ]]; then
            break
        fi
    done

    # copy the chosen files in parallel
    pids=()
    for f in $files; do
        echo "copying $f"
        b_name=$(basename $f)
        cat $f > $target_dir/$b_name &
        pids="$pids $!"
    done

    # wait for jobs to complete
    for pid in ${pids[@]}; do
        wait $pid
    done

    # compare resulting files
    for f in $files; do
        diff $f $target_dir/$b_name
    done
}

# Get all the DX environment variables, so that dxfuse can use them
echo "loading the dx environment"

# don't leak the token to stdout
rm -f ENV
dx env --bash > ENV
source ENV >& /dev/null

#dx rm -r $projectName:/$target_dir || true
#dx mkdir -p $projectName:/$target_dir

# create a fresh mountpoint
mkdir -p $mountpoint

rm -r $target_dir || true
mkdir -p $target_dir

# Start the dxfuse daemon in the background, and wait for it to initilize.
echo "Mounting dxfuse"
sudo -E /go/bin/dxfuse $mountpoint $projectName &
sleep 5
ls -l "$mountpoint/$projectName"

# run test
check_parallel_cat

sudo umount $mountpoint
