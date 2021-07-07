######################################################################
## constants

projName="dxfuse_test_data"
dxfuse="$GOPATH/bin/dxfuse"
dxDirOnProject="mini"
baseDir=$HOME/dxfuse_test
mountpoint=${baseDir}/MNT

# Directories created during the test
writeable_dirs=()
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

function faux_dirs_move {
    local root_dir=$1
    cd $root_dir

    tree $root_dir

    # cannot move faux directories
    set +e
    (mv $root_dir/1 $root_dir/2 ) >& /dev/null
    rc=$?
    if [[ $rc == 0 ]]; then
        echo "Error, could not a faux directory"
    fi

    # cannot move files into a faux directory
    (mv -f $root_dir/NewYork.txt $root_dir/1) >& /dev/null
    rc=$?
    if [[ $rc == 0 ]]; then
        echo "Error, could move a file into a faux directory"
    fi
    set -e
}

function faux_dirs_remove {
    local root_dir=$1
    cd $root_dir

    # can move a file out of a faux directory
    mkdir $root_dir/T
    mv $root_dir/1/NewYork.txt $root_dir/T
    rm -rf $root_dir/T

    echo "removing faux dir 1"
    rm -rf $root_dir/1
}


function populate_faux_dir {
    local faux_dir=$1

    echo "deep dish pizza and sky trains" > /tmp/XXX
    echo "nice play chunk" > /tmp/YYY
    echo "no more chewing on shoes!" > /tmp/ZZZ
    echo "you just won a trip to the Caribbean" > /tmp/VVV

    dx upload /tmp/XXX -p --destination $projName:/$faux_dir/Chicago.txt >& /dev/null
    dx upload /tmp/YYY -p --destination $projName:/$faux_dir/Chicago.txt >& /dev/null
    dx upload /tmp/ZZZ -p --destination $projName:/$faux_dir/NewYork.txt >& /dev/null
    dx upload /tmp/VVV -p --destination $projName:/$faux_dir/NewYork.txt >& /dev/null
    rm -f /tmp/XXX /tmp/YYY /tmp/ZZZ /tmp/VVV
}

######################################################################

function faux_dirs {
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
    faux_dir=$(cat /dev/urandom | env LC_CTYPE=C LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    faux_dir="faux_$faux_dir"
    writeable_dirs=($faux_dir)
    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done

    populate_faux_dir $faux_dir

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags="-writeable"
    if [[ $verbose != "" ]]; then
        flags="$flags -verbose 2"
    fi
    $dxfuse $flags $mountpoint dxfuse_test_data dxfuse_test_read_only ArchivedStuff
    sleep 1

    echo "faux dirs cannot be moved"
    faux_dirs_move $mountpoint/$projName/$faux_dir

    echo "faux dir operations"
    faux_dirs_remove $mountpoint/$projName/$faux_dir

    teardown
}
