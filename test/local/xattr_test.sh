#!/bin/bash -ex

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
    fusermount -u $mountpoint

    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

# wait for the file to achieve the closed state
function close_file {
    local path=$1
    while true; do
        file_state=$(dx describe $path --json | grep state | awk '{ gsub("[,\"]", "", $2); print $2 }')
        if [[ "$file_state" == "closed" ]]; then
            break
        fi
        sleep 1
    done
}

function setup {
    local base_dir=$1

    # bat.txt
    local f=$projName:/$base_dir/bat.txt
    echo "flying mammal" | dx upload --destination $f -
    dx set_properties $f fly=yes family=mammal eat=omnivore
    close_file $f

    # whale.txt
    local f=$projName:/$base_dir/whale.txt
    echo "The largest mammal on earth" | dx upload --destination $f -
    close_file $f

    # Mountains.txt
    local f=$projName:/$base_dir/Mountains.txt
    echo "K2, Kilimanjaro, Everest, Mckinly" | dx upload --destination $f -
    close_file $f

    dx ls -l $projName:/$base_dir
}

function check_bat {
    local base_dir=$1
    local test_dir=$mountpoint/$projName/$base_dir

    # Get a list of all the attributes
    local bat_all_attrs=$(xattr $test_dir/bat.txt | sort | tr '\n' ' ')
    local bat_all_expected="base.archivalState base.id base.state prop.eat prop.family prop.fly "
    if [[ $bat_all_attrs != $bat_all_expected ]]; then
        echo "bat attributes are incorrect"
        echo "   got:       $bat_all_attrs"
        echo "   expecting: $bat_all_expected"
        exit 1
    fi

    local bat_family=$(xattr -p prop.family $test_dir/bat.txt)
    local bat_family_expected="mammal"
    if [[ $bat_family != $bat_family_expected ]]; then
        echo "bat family is wrong"
        echo "   got:       $bat_family"
        echo "   expecting: $bat_family_expected"
        exit 1
    fi

    xattr -w prop.family carnivore $test_dir/bat.txt
    xattr -w prop.family mammal $test_dir/bat.txt
}

function check_whale {
    local base_dir=$1
    local test_dir=$mountpoint/$projName/$base_dir

    local whale_all_attrs=$(xattr $test_dir/whale.txt | sort | tr '\n' ' ')
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
    local test_dir=$mountpoint/$projName/$base_dir
    local f=$test_dir/Mountains.txt
    local dnaxF=$projName:/$base_dir/Mountains.txt

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

    echo "synchronizing filesystem"
    $dxfuse -sync

    props=$(dx describe $dnaxF --json | jq -cMS .properties | tr '[]' ' ')
    echo "props on platform: $props"
    local props_expected='{"family":"geography"}'
    if [[ $props != $props_expected ]]; then
        echo "$f properties mismatch"
        echo "   got:        $props"
        echo "   expecting:  $props_expected"
        exit 1
    fi

    tags=$(dx describe $dnaxF --json | jq -cMS .tags | tr '[]' ' ')
    echo "tags on platform: $tags"
    local tags_expected=' "high" '
    if [[ $tags != $tags_expected ]]; then
        echo "$f tags mismatch"
        echo "   got:        $tags"
        echo "   expecting:  $tags_expected"
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
    base_dir=$(cat /dev/urandom | env LC_CTYPE=C LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    base_dir="base_$base_dir"
    writeable_dirs=($base_dir)
    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done
    dx mkdir $projName:/$base_dir

    setup $base_dir

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags="-readWrite"
    if [[ $verbose != "" ]]; then
        flags="$flags verbose 2"
    fi
    $dxfuse $flags $mountpoint $projName
    sleep 1

    tree $mountpoint/$projName/$base_dir

    check_bat $base_dir
    check_whale $base_dir
    check_new $base_dir

    teardown
}
