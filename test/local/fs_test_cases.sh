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

# create directory on mounted FS
function create_dir {
    local src_dir=$1
    local write_dir=$2

    echo "create_dir($src_dir, $write_dir)"
    mkdir $write_dir

    # copy files to new directory
    echo "copying small files"
    cp $src_dir/*  $write_dir

    # compare resulting files
    echo "comparing files"
    local files=$(find $src_dir -type f)
    for f in $files; do
        b_name=$(basename $f)
        diff $f $write_dir/$b_name
    done

    echo "making empty new sub-directories"
    mkdir $write_dir/E
    mkdir $write_dir/F
    echo "catch 22" > $write_dir/E/Z.txt

    tree $write_dir
}

# create directory on mounted FS
function create_remove_dir {
    local flag=$1
    local src_dir=$2
    local write_dir=$3

    echo "create_remove_dir($flag, $src_dir, $write_dir)"
    mkdir $write_dir
    rmdir $write_dir
    mkdir $write_dir

    # copy files
    echo "copying small files"
    cp $src_dir/*  $write_dir/

    # compare resulting files
    echo "comparing files"
    local files=$(find $src_dir -type f)
    for f in $files; do
        b_name=$(basename $f)
        diff $f $write_dir/$b_name
    done

    echo "making empty new sub-directories"
    mkdir $write_dir/E
    mkdir $write_dir/F
    echo "catch 22" > $write_dir/E/Z.txt

    tree $write_dir

    if [[ $flag == "yes" ]]; then
        echo "synchronizing the filesystem"
        $dxfuse -sync
    fi

    echo "removing directory recursively"
    rm -rf $write_dir
}

# removing a non-empty directory fails
function rmdir_non_empty {
    local write_dir=$1

    mkdir $write_dir
    cd $write_dir

    mkdir E
    echo "permanente creek" > E/X.txt

    set +e
    rmdir E >& /dev/null
    rc=$?
    set -e
    if [[ $rc == 0 ]]; then
        echo "Error, removing non empty directory should fail"
        exit 1
    fi

    rm -rf $write_dir
}

# removing a non-existent directory fails
function rmdir_not_exist {
    local write_dir=$1

    mkdir $write_dir
    cd $write_dir

    set +e
    rmdir E >& /dev/null
    rc=$?
    set -e
    if [[ $rc == 0 ]]; then
        echo "Error, removing non existent directory should fail"
        exit 1
    fi

    rm -rf $write_dir
}

# create an existing directory fails
function mkdir_existing {
    local write_dir=$1
    mkdir $write_dir

    set +e
    mkdir $write_dir >& /dev/null
    rc=$?
    set -e
    if [[ $rc == 0 ]]; then
        echo "Error, creating an existing directory should fail"
        exit 1
    fi

    rm -rf $write_dir
}

function file_create_existing {
    local write_dir=$1
    cd $write_dir

    echo "happy days" > hello.txt
    chmod 444 hello.txt

    set +e
    (echo "nothing much" > hello.txt) >& /tmp/cmd_results.txt
    rc=$?
    set -e

    if [[ $rc == 0 ]]; then
        echo "Error, could modify an existing file"
        exit 1
    fi
    result=$(cat /tmp/cmd_results.txt)
    if [[ ( ! $result =~ "Permission denied" ) && ( ! $result =~ "Operation not permitted" ) ]]; then
        echo "Error, incorrect command results, writing to hello.txt"
        cat /tmp/cmd_results.txt

        echo "===== log ======="
        cat /var/log/dxfuse.log
        exit 1
    fi

    rm -f hello.txt
}

function file_remove_non_exist {
    local write_dir=$1
    cd $write_dir

    set +e
    (rm hello.txt) >& /tmp/cmd_results.txt
    rc=$?
    set -e

    if [[ $rc == 0 ]]; then
        echo "Error, could remove a non-existent file"
        exit 1
    fi
    result=$(cat /tmp/cmd_results.txt)
    if [[ ! $result =~ "No such file or directory" ]]; then
        echo "Error, incorrect command results"
        cat /tmp/cmd_results.txt
        exit 1
    fi

}

function move_file {
    local write_dir=$1
    cd $write_dir

    rm -f XX.txt
    echo "the jaberwoky is on the loose" > XX.txt
    mv XX.txt ZZ.txt
    mv ZZ.txt XX.txt
    rm -f XX.txt
}

function move_file2 {
    local write_dir=$1
    cd $write_dir

    rm -rf A
    rm -rf B
    mkdir A

    echo "the jaberwoky is on the loose" > A/XX.txt
    mv A/XX.txt A/ZZ.txt

    mkdir B
    mv A/ZZ.txt B/ZZ.txt

    tree A
    tree B

    rm -rf A
    rm -rf B
}

function rename_dir {
    local write_dir=$1
    cd $write_dir

    rm -rf A
    rm -rf B
    mkdir A
    echo "Monroe doctrine" > A/X.txt
    echo "Ted Rosevelt" > A/Y.txt

    mv A B
    tree B
}

function move_dir {
    local write_dir=$1
    cd $write_dir

    rm -rf A
    rm -rf B
    mkdir A
    echo "Monroe doctrine" > A/X.txt
    echo "Ted Rosevelt" > A/Y.txt
    tree A

    mkdir B
    mv A B/

    tree B
    rm -rf B
}

function move_dir_deep {
    local write_dir=$1
    local expNum=$2
    cd $write_dir

    rm -rf A
    rm -rf D

    mkdir A
    echo "Monroe doctrine" > A/X.txt
    echo "Ted Rosevelt" > A/Y.txt
    mkdir A/fruit
    echo "melons" > A/fruit/melon.txt
    echo "grapes" > A/fruit/grapes.txt

    tree A

    mkdir D
    mkdir D/K

    mv A D/K/

    tree D > /tmp/results_$expNum.txt
}

function move_non_existent_dir {
    local write_dir=$1
    cd $write_dir

    set +e
    (mv X Y) >& /tmp/cmd_results.txt
    rc=$?
    set -e

    if [[ $rc == 0 ]]; then
        echo "Error, could move a non-existent directory"
        exit 1
    fi
    result=$(cat /tmp/cmd_results.txt)
    if [[ ! $result =~ "No such file or directory" ]]; then
        echo "Error, incorrect command results"
        cat /tmp/cmd_results.txt
        exit 1
    fi
}

# can't move a directory into a file
function move_dir_to_file {
    local write_dir=$1
    cd $write_dir

    mkdir X
    echo "zz" > Y.txt

    set +e
    (mv -f X Y.txt) >& /tmp/cmd_results.txt
    rc=$?
    set -e

    if [[ $rc == 0 ]]; then
        echo "Error, could move a directory into a file"
        exit 1
    fi
    result=$(cat /tmp/cmd_results.txt)
    echo $result
    if [[ ! $result =~ "cannot overwrite non-directory" ]]; then
        echo "Error, incorrect command results"
        cat /tmp/cmd_results.txt
        exit 1
    fi

    echo "clean up "
    rm -rf X
    rm -f Y.txt
}


function dir_and_file_with_the_same_name {
    local root_dir=$1

    tree $root_dir/same_names
}

function archived_files {
    local root_dir=$1

    num_files=$(ls -1 $root_dir | wc -l)
    if [[ $num_files != 3 ]]; then
        echo "Should see two live files. Instead, can see $num_files files."
        exit 1
    fi
}

function fs_test_cases {
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
    expr_dir=$(cat /dev/urandom | env LC_CTYPE=C LC_ALL=C tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    expr_dir="expr_$expr_dir"
    writeable_dirs=($base_dir $expr_dir)
    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done

    dx mkdir $projName:/$base_dir
    dx mkdir $projName:/$expr_dir

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags="-writeable"
    if [[ $verbose != "" ]]; then
        flags="$flags -verbose 2"
    fi
    $dxfuse $flags $mountpoint dxfuse_test_data dxfuse_test_read_only ArchivedStuff
    sleep 1

    echo "can't write to read-only project"
    write_to_read_only_project

    echo "archived files"
    archived_files $mountpoint/ArchivedStuff

    echo "create directory"
    create_dir $mountpoint/$projName/$dxDirOnProject/small  $mountpoint/$projName/$base_dir/T2

    echo "create/remove directory"
    create_remove_dir "yes" $mountpoint/$projName/$dxDirOnProject/small $mountpoint/$projName/$base_dir/T3
    create_remove_dir "no" $mountpoint/$projName/$dxDirOnProject/small $mountpoint/$projName/$base_dir/T3

    echo "mkdir rmdir"
    rmdir_non_empty $mountpoint/$projName/$base_dir/T4
    rmdir_not_exist $mountpoint/$projName/$base_dir/T4
    mkdir_existing  $mountpoint/$projName/$base_dir/T4

    echo "file create remove"
    file_create_existing "$mountpoint/$projName"
    file_remove_non_exist "$mountpoint/$projName"

    echo "move file I"
    move_file $mountpoint/$projName/$expr_dir

    echo "move file II"
    move_file2 $mountpoint/$projName/$expr_dir

    echo "rename directory"
    rename_dir $mountpoint/$projName/$expr_dir
    rename_dir /tmp
    diff -r /tmp/B $mountpoint/$projName/$expr_dir/B
    rm -rf /tmp/B $mountpoint/$projName/$expr_dir/B

    echo "move directory"
    move_dir $mountpoint/$projName/$expr_dir

    echo "move a deep directory"
    move_dir_deep $mountpoint/$projName/$expr_dir 1
    move_dir_deep /tmp 2
    cd $HOME

    diff /tmp/results_1.txt /tmp/results_2.txt
    diff -r $mountpoint/$projName/$expr_dir/D /tmp/D
    rm -rf $mountpoint/$projName/$expr_dir/D
    rm -rf /tmp/D

    echo "checking illegal directory moves"
    move_non_existent_dir "$mountpoint/$projName"
    move_dir_to_file "$mountpoint/$projName"

    echo "directory and file with the same name"
    dir_and_file_with_the_same_name $mountpoint/$projName

    teardown
}
