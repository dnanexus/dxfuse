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

    # This seems to be needed on MacOS
    sleep 1

    local baseDir=$mountpoint/$projName/xattrs
    tree $baseDir

    # Get a list of all the attributes
    cd $baseDir

    local bat_all_attrs=$(xattr bat.txt | sort | tr '\n' ' ')
    local bat_all_expected="base.archivalState base.id base.state props.eat props.family props.fly "
    if [[ $bat_all_attrs != $bat_all_expected ]]; then
       echo "bat attributes are incorrect"
       echo "   got:       $bat_all_attrs"
       echo "   expecting: $bat_all_expected"
       exit 1
    fi

    local bat_family=$(xattr -p props.family bat.txt)
    local bat_family_expected="mammal"
    if [[ $bat_family != $bat_family_expected ]]; then
        echo "bat family is wrong"
        echo "   got:       $bat_family"
        echo "   expecting: $bat_family_expected"
        exit 1
    fi

    local whale_all_attrs=$(xattr whale.txt | sort | tr '\n' ' ')
    local whale_all_expected="base.archivalState base.id base.state "
    if [[ $whale_all_attrs != $whale_all_expected ]]; then
       echo "whale attributes are incorrect"
       echo "   got:       $whale_all_attrs"
       echo "   expecting: $whale_all_expected"
       exit 1
    fi

    cd $HOME
    teardown
}
