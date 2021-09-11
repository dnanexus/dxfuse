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
    local top_dir=$1
    local target_dir=$2
    local write_dir=$top_dir/$target_dir

    echo "write_dir = $write_dir"

    # create a small file through the filesystem interface
    echo $line1 > $write_dir/A.txt
    ls -l $write_dir/A.txt


    dx wait $projName:/$target_dir/A.txt

    # compare the data
    local content=$(dx cat $projName:/$target_dir/A.txt)
    if [[ "$content" == "$line1" ]]; then
        echo "correct"
    else
        echo "bad content"
        echo "should be: $line1"
        echo "found: $content"
    fi
}

function check_overwrite_fails {
    local top_dir=$1
    local target_dir=$2
    local write_dir=$top_dir/$target_dir

    echo "write_dir = $write_dir"
    set +e
    echo $line2 >> $write_dir/A.txt
    rc=$?
    set -e
    if [[ $rc == 0 ]]; then
        echo "Error, appending to remote file should fail"
        exit 1
    fi
    cat $write_dir/A.txt
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

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags="-limitedWrite"
    if [[ $verbose != "" ]]; then
        flags="$flags -verbose 2"
    fi
    $dxfuse $flags $mountpoint dxfuse_test_data
    sleep 1

    echo "writing a small file"
    check_file_write_content $mountpoint/$projName $base_dir

    fusermount -u $mountpoint

    # now we are ready for an overwrite experiment
    $dxfuse $flags $mountpoint dxfuse_test_data

    echo "Rewriting a file is not allowed"
    check_overwrite_fails $mountpoint/$projName $base_dir

    teardown
}
