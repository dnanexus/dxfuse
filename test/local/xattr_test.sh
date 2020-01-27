######################################################################
# global variables
mountpoint=${HOME}/MNT
projName="dxfuse_test_data"
dxfuse="$GOPATH/bin/dxfuse"
teardown_complete=0

######################################################################

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1

    rm -f cmd_results.txt

    echo "unmounting dxfuse"
    cd $HOME
    sudo umount $mountpoint

    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

function xattr_test {
    mkdir -p $mountpoint

    sudo -E $dxfuse -verbose 2 -uid $(id -u) -gid $(id -g) $mountpoint $projName

    local baseDir=$mountpoint/$projName/xattrs
    tree $baseDir

    # Get a list of all the attributes
    cd $baseDir
    getfattr -d -m - HoneyBadger.txt
    getfattr -d -m - whale.txt
    getfattr -d -m - bat.txt
    cd $HOME

    teardown
}
