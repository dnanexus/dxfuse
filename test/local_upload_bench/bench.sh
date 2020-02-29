######################################################################
## constants

CRNT_DIR=$(dirname "${BASH_SOURCE[0]}" )
projName="dxfuse_test_data"
dxfuse="$GOPATH/bin/dxfuse"
baseDir=$HOME/dxfuse_test
mountpoint=${baseDir}/MNT

large_file=wgEncodeUwRepliSeqBg02esG1bAlnRep1.bam.bai

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

#    set -e
#    dx rm $projName:/$large_file >& /dev/null
#    set +e
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

function upload_one_file {
    echo "upload with dxfuse"
    start=`date +%s`
    cp $large_file $mountpoint/$projName/
    end=`date +%s`
    runtime_cp=$((end-start))

    echo "start dxfuse sync"
    start=`date +%s`
    $dxfuse -sync
    end=`date +%s`
    runtime_sync=$((end-start))

    echo "runtime = ($runtime_cp, $runtime_sync) seconds"
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
        flags="-verbose 2"
    fi
    sudo -E $dxfuse -uid $(id -u) -gid $(id -g) $flags $mountpoint $projName
    sleep 1

    ls $mountpoint/$projName/

    upload_one_file

    teardown
}

local_upload_benchmark
