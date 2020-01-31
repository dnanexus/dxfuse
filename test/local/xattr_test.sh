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

function check_bat {
    local base_dir=$1

    # Get a list of all the attributes
    local bat_all_attrs=$(xattr $base_dir/bat.txt | sort | tr '\n' ' ')
    local bat_all_expected="base.archivalState base.id base.state prop.eat prop.family prop.fly "
    if [[ $bat_all_attrs != $bat_all_expected ]]; then
        echo "bat attributes are incorrect"
        echo "   got:       $bat_all_attrs"
        echo "   expecting: $bat_all_expected"
        exit 1
    fi

    local bat_family=$(xattr -p prop.family $base_dir/bat.txt)
    local bat_family_expected="mammal"
    if [[ $bat_family != $bat_family_expected ]]; then
        echo "bat family is wrong"
        echo "   got:       $bat_family"
        echo "   expecting: $bat_family_expected"
        exit 1
    fi


    xattr -w prop.family carnivore $base_dir/bat.txt
    xattr -w prop.family mammal $base_dir/bat.txt
}

function check_whale {
    local base_dir=$1

    local whale_all_attrs=$(xattr $base_dir/whale.txt | sort | tr '\n' ' ')
    local whale_all_expected="base.archivalState base.id base.state "
    if [[ $whale_all_attrs != $whale_all_expected ]]; then
       echo "whale attributes are incorrect"
       echo "   got:       $whale_all_attrs"
       echo "   expecting: $whale_all_expected"
       exit 1
    fi
}

function check_new {
    local base_dir=$1
    local f=$base_dir/Mountains.txt

    xattr -w prop.family geography $f
    xattr -w tag.high X $f

    local family=$(xattr -p prop.family $f)
    local expected="geography"
    if [[ $family != $expected ]]; then
        echo "$f family property is wrong"
        echo "   got:       $family"
        echo "   expecting: $expected"
        exit 1
    fi

    local all_attrs=$(xattr $f | sort | tr '\n' ' ')
    local all_expected="base.archivalState base.id base.state prop.family tag.high "
    if [[ $all_attrs != $all_expected ]]; then
        echo "$f attributes are incorrect"
        echo "   got:       $all_attrs"
        echo "   expecting: $all_expected"
        exit 1
    fi

    xattr -d prop.family $f
    xattr -d tag.high $f
    xattr  $f

    local all_attrs2=$(xattr $f | sort | tr '\n' ' ')
    local all_expected2="base.archivalState base.id base.state "
    if [[ $all_attrs2 != $all_expected2 ]]; then
        echo "$f attributes are incorrect"
        echo "   got:       $all_attrs2"
        echo "   expecting: $all_expected2"
        exit 1
    fi
}

function xattr_test {
    mkdir -p $mountpoint

    sudo -E $dxfuse -verbose 2 -uid $(id -u) -gid $(id -g) $mountpoint $projName

    # This seems to be needed on MacOS
    sleep 1

    local base_dir=$mountpoint/$projName/xattrs
    tree $base_dir

    check_bat $base_dir
    check_whale $base_dir
    check_new $base_dir

    sudo $dxfuse -sync

    teardown
}
