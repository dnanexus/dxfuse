######################################################################
## constants

projName="dxfuse_test_data"
dxfuse="$GOPATH/bin/dxfuse"
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

# copy a file and check that platform has the correct content
#
function check_file_write_content {
    local top_dir=$1
    local target_dir=$2
    local write_dir=$top_dir/$target_dir
    local content="nothing much"

    echo "write_dir = $write_dir"

    # create a small file through the filesystem interface
    echo $content > $write_dir/A.txt
    ls -l $write_dir/A.txt

    echo "synchronizing the filesystem"
    $dxfuse -sync

    echo "file is closed"
    dx ls -l $projName:/$target_dir/A.txt

    # compare the data
    local content2=$(dx cat $projName:/$target_dir/A.txt)
    if [[ "$content" == "$content2" ]]; then
        echo "correct"
    else
        echo "bad content"
        echo "should be: $content"
        echo "found: $content2"
    fi


    # create an empty file
    touch $write_dir/B.txt
    ls -l $write_dir/B.txt

    echo "synchronizing the filesystem"
    $dxfuse -sync

    echo "file is closed"
    dx ls -l $projName:/$target_dir/B.txt

    # compare the data
    local content3=$(dx cat $projName:/$target_dir/B.txt)
    if [[ "$content3" == "" ]]; then
        echo "correct"
    else
        echo "bad content"
        echo "should be empty"
        echo "found: $content3"
    fi
}

# copy files inside the mounted filesystem
#
function write_files {
    local src_dir=$1
    local write_dir=$2

    echo "write_dir = $write_dir"
    ls -l $write_dir

    echo "copying large files"
    cp $src_dir/*  $write_dir/

    echo "synchronizing the filesystem"
    $dxfuse -sync

    # compare resulting files
    echo "comparing files"
    local files=$(find $src_dir -type f)
    for f in $files; do
        b_name=$(basename $f)
        diff $f $write_dir/$b_name
    done
}

# check that we can't write to VIEW only project
#
function write_to_read_only_project {
    ls $mountpoint/dxfuse_test_read_only
    (echo "hello" > $mountpoint/dxfuse_test_read_only/A.txt) >& cmd_results.txt || true
    local result=$(cat cmd_results.txt)

    if [[  $result =~ "Operation not permitted" ]]; then
        echo "Correct, we should not be able to modify a project to which we have VIEW access"
    else
        echo "Incorrect, we managed to modify a project to which we have VIEW access"
        exit 1
    fi
}


function file_write_slow {
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

    target_dir=$base_dir/T1
    dx mkdir $projName:/$target_dir

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags=""
    if [[ $verbose != "" ]]; then
        flags="-verbose 2"
    fi
    $dxfuse $flags $mountpoint dxfuse_test_data
    sleep 1

    echo "can write to a small file"
    check_file_write_content $mountpoint/$projName $target_dir

    echo "can write several large files to a directory"
    write_files $mountpoint/$projName/large_files $mountpoint/$projName/$target_dir

    teardown
}
