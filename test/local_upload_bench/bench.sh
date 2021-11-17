######################################################################
## constants

CRNT_DIR=$(dirname "${BASH_SOURCE[0]}" )
projName="dxfuse_test_data"
dxfuse="$CRNT_DIR/../../dxfuse"
baseDir=$HOME/dxfuse_test
mountpoint=${baseDir}/MNT

#large_file=wgEncodeUwRepliSeqBg02esG1bAlnRep1.bam.bai
#large_file=ubuntu.tar.gz
large_file=ubuntu_18_04_minimal.tar.gz
######################################################################

teardown_complete=0

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1

    echo "unmounting dxfuse"
    cd $HOME
    sudo umount $mountpoint

    dxPath=$projName:/$large_file
    echo "cleaning up platform file $dxPath"
    set -e
    dx ls -l $dxPath
    dx rm $dxPath
    set +e
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

function upload_one_file {
    echo "copy into fuse"
    start=`date +%s`
    cp $large_file $mountpoint/$projName/
    end=`date +%s`
    runtime_cp=$((end-start))
    echo "runtime = ($runtime_cp) seconds"
}

function local_upload_benchmark {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # local machine
    rm -f ENV
    dx env --bash > ENV
    source ENV >& /dev/null
    rm -f ENV

    # clean and make fresh directories
    mkdir -p $mountpoint

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags=""
    if [[ $verbose != "" ]]; then
        flags="-verbose 1"
    fi
    sudo -E $dxfuse -uid $(id -u) -gid $(id -g) $flags $mountpoint $projName
    sleep 1

    ls $mountpoint/$projName/

    upload_one_file

    teardown
}

local_upload_benchmark
