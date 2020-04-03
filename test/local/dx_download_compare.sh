#!/bin/bash -e

######################################################################
## constants

projName="dxfuse_test_data"
dxfuse="$GOPATH/bin/dxfuse"
dxDirOnProject="mini"
baseDir=$HOME/dxfuse_test
mountpoint=${baseDir}/MNT

dxfuseDir=$mountpoint/$projName/$dxDirOnProject
dxpyDir=${baseDir}/dxCopy/$dxDirOnProject

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
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

function dx_download_compare_body {
    echo "download recursively with dx download"
    parentDxpyDir=$(dirname $dxpyDir)
    if [[ ! -d $parentDxpyDir ]]; then
        echo "downloading into $parentDxpyDir from $projName:/$dxDirOnProject"
        mkdir -p $parentDxpyDir
        dx download --no-progress -o $parentDxpyDir -r $projName:/$dxDirOnProject
    fi

    # do not exit immediately if there are differences; we want to see the files
    # that aren't the same
    echo "recursive compare"
    diff -r --brief $dxpyDir $dxfuseDir > diff.txt || true
    if [[ -s diff.txt ]]; then
        echo "Difference in basic file structure"
        cat diff.txt
        echo "===== dxpy ==== "
        tree $dxpyDir
        echo
        echo "===== dxfuse ==== "
        tree $dxfuseDir
        exit 1
    fi
}

function dx_download_compare {
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
    sudo -E $dxfuse -uid $(id -u) -gid $(id -g) $flags $mountpoint dxfuse_test_data
    sleep 1

    dx_download_compare_body

    teardown
}

dx_download_compare
