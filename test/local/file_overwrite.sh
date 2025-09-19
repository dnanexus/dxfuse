#!/bin/bash
######################################################################
## constants
CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
projName="dxfuse_test_data"
dxfuse="$CRNT_DIR/../../dxfuse"
baseDir=$HOME/dxfuse_test
mountpoint=${baseDir}/MNT

# Directories created during the test
writeable_dirs=()

line1="K2 is the most dangerous mountain to climb in the Himalayas"
line2="One would also like to climb Kilimanjaro and the Everest"

######################################################################

teardown_complete=0

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1

    rm -f cmd_results.txt

    echo "unmounting dxfuse"
    cd $HOME
    fusermount -u $mountpoint

    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

# copy a file and check that platform has the correct content
#
function check_file_write_content {
    local dx_dest_dir=$1
    local write_dir=$2

    mkdir -p $write_dir
    # create a small file through the filesystem interface
    echo $line1 > $write_dir/A.txt
    ls -l $write_dir/A.txt


    dx wait $projName:/$dx_dest_dir/A.txt

    # compare the data
    local content
    content=$(dx cat $projName:/$dx_dest_dir/A.txt)
    if [[ "$content" == "$line1" ]]; then
        echo "correct"
    else
        echo "bad content"
        echo "should be: $line1"
        echo "found: $content"
    fi
}

function verify_replacement {
    local write_dir=$1
    local dx_dest_dir=$2

    # Compare local file content
    local content
    content=$(cat $write_dir/A.txt)
    if [[ "$content" == "$line2" ]]; then
        echo "correct, line content is replaced"
    else
        echo "bad content"
        echo "should be: $line2"
        echo "found: $content"
    fi

    # Compare remote file content
    local content2
    content2=$(dx cat $projName:/$dx_dest_dir/A.txt)
    if [[ "$content2" == "$line2" ]]; then
        echo "correct"
    else
        echo "bad content"
        echo "should be: $line2"
        echo "found: $content2"
    fi

    # Check the dx file is replaced
    num_files=$(dx ls $projName:/$dx_dest_dir | grep A.txt -c)
    if [[ $num_files != 1 ]]; then
        echo "Should see 1 files. Instead, can see $num_files files."
        exit 1
    else
        echo "correct, can see 1 files"
    fi
}

function check_echo {
    local test_dir="echo"
    local dx_dest_dir=$base_dir/$test_dir
    local write_dir="$mountpoint/$projName/$dx_dest_dir"

    echo "writing a small file"
    check_file_write_content $dx_dest_dir $write_dir

    echo "write_dir = $write_dir"
    set +e
    echo $line2 > $write_dir/A.txt
    rc=$?
    set -e
    if [[ $rc != 0 ]]; then
        echo "Error, overwriting existing file with echo failed"
        exit 1
    fi

    echo "verify replacement result: check_echo_existing"
    verify_replacement "$write_dir" "$dx_dest_dir" "$line2"

}

function check_tee {
    local test_dir="tee"
    local dx_dest_dir=$base_dir/$test_dir
    local write_dir="$mountpoint/$projName/$dx_dest_dir"

    echo "writing a small file"
    check_file_write_content $dx_dest_dir $write_dir

    echo "write_dir = $write_dir"
    set +e
    echo $line2 | tee $write_dir/A.txt
    rc=$?
    set -e
    if [[ $rc != 0 ]]; then
        echo "Error, overwriting existing file with tee failed"
        exit 1
    fi

    echo "verify replacement result: check_tee"
    verify_replacement "$write_dir" "$dx_dest_dir" "$line2"

}

function check_tail {
    local test_dir="tail"
    local dx_dest_dir=$base_dir/$test_dir
    local write_dir="$mountpoint/$projName/$dx_dest_dir"

    echo "writing a small file"
    check_file_write_content $dx_dest_dir $write_dir

    echo "write_dir = $write_dir"
    set +e
    touch /tmp/B.txt

    # tail will trigger openat(AT_FDCWD, "$write_dir/A.txt", O_WRONLY|O_CREAT|O_TRUNC, 0666) = 3
    tail -f /tmp/B.txt > $write_dir/A.txt &
    TAIL_PID=$!
    echo $line2 > /tmp/B.txt
    sleep 5  # Give tail time to process
    kill $TAIL_PID 2>/dev/null
    wait $TAIL_PID
    set -e

    echo "verify replacement result: check_tail"
    verify_replacement "$write_dir" "$dx_dest_dir" "$line2"
}

function file_overwrite {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # local machine
    rm -f ENV
    dx env --bash > ENV
    source ENV >& /dev/null
    rm -f ENV

    # clean and make fresh directories
    mkdir -p $mountpoint

    # generate random alphanumeric strings
    base_dir=$(dd if=/dev/urandom bs=15 count=1 2>/dev/null| base64 | tr -dc 'a-zA-Z0-9'|fold -w 12|head -n1)
    base_dir="base_$base_dir"
    writeable_dirs=($base_dir)
    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done

    dx mkdir $projName:/$base_dir

    # Start the dxfuse daemon in the background, and wait for it to initialize.
    echo "Mounting dxfuse"
    flags="-limitedWrite -allowOverwrite"
    if [[ $verbose != "" ]]; then
        flags="$flags -verbose 2"
    fi

    # now we are ready for an overwrite experiment
    $dxfuse $flags $mountpoint dxfuse_test_data

    echo "Overwrite a file: echo"
    check_echo $mountpoint/$projName $base_dir
    
    echo "Overwrite a file: tee"
    check_tee $mountpoint/$projName $base_dir
    
    echo "Overwrite a file: tail"
    check_tail $mountpoint/$projName $base_dir

    teardown
}
