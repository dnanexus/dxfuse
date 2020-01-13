#!/bin/bash -e

######################################################################
## constants

projName="dxfuse_test_data"

if [[ $DX_JOB_ID == "" ]]; then
    # small test for a remote laptop
    dxDirOnProject="mini"
else
    # larger test for a cloud worker
    dxDirOnProject="correctness"
fi

baseDir=$HOME/dxfuse_test
mountpoint=${baseDir}/MNT

dxfuseDir=$mountpoint/$projName/$dxDirOnProject
dxpyDir=${baseDir}/dxCopy/$dxDirOnProject

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

    echo "syncing filesystem"
    sync

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

function check_tree {
    tree -n $dxfuseDir -o dxfuse.org.txt
    tree -n $dxpyDir -o dxpy.org.txt

    # The first line is different, we need to get rid of it
    tail --lines=+2 dxfuse.org.txt > dxfuse.txt
    tail --lines=+2 dxpy.org.txt > dxpy.txt

    diff dxpy.txt dxfuse.txt > D.txt || true
    if [[ -s D.txt ]]; then
        echo "tree command was not equivalent"
        cat D.txt
        exit 1
    fi
    rm -f dxfuse*.txt dxpy*.txt D.txt
}

function check_ls {
    d=$(pwd)
    cd $dxfuseDir; ls -R > $d/dxfuse.txt
    cd $dxpyDir; ls -R > $d/dxpy.txt
    cd $d
    diff dxfuse.txt dxpy.txt > D.txt || true
    if [[ -s D.txt ]]; then
        echo "ls -R was not equivalent"
        cat D.txt
        exit 1
    fi
    rm -f dxfuse*.txt dxpy*.txt D.txt
}

function check_cmd_line_utils {
    d=$(pwd)

    cd $dxfuseDir
    files=$(find . -type f)
    cd $d

    for f in $files; do
        # we want to run these checks only on text files.
        if [[ $(file -b $f) != "ASCII TEXT" ]]; then
            continue
        fi

        # Ok, this is a text file
        echo $f

        dxfuse_f=$dxfuseDir/$f
        dxpy_f=$dxpyDir/$f

        # wc should return the same result
        wc < $dxfuse_f > 1.txt
        wc < $dxpy_f > 2.txt
        diff 1.txt 2.txt > D.txt || true
        if [[ -s D.txt ]]; then
            echo "wc for files $dxfuse_f $dxpy_f is not the same"
            cat D.txt
            exit 1
        fi

        # head
        head $dxfuse_f > 1.txt
        head $dxpy_f > 2.txt
        diff 1.txt 2.txt > D.txt || true
        if [[ -s D.txt ]]; then
            echo "head for files $dxfuse_f $dxpy_f is not the same"
            cat D.txt
            exit 1
        fi

        # tail
        tail $dxfuse_f > 1.txt
        tail $dxpy_f > 2.txt
        diff 1.txt 2.txt > D.txt || true
        if [[ -s D.txt ]]; then
            echo "tail for files $dxfuse_f $dxpy_f is not the same"
            cat D.txt
            exit 1
        fi
        rm -f 1.txt 2.txt D.txt
    done
}

function check_find {
    find $dxfuseDir -type f -name "*.wdl" > 1.txt
    find $dxpyDir -type f -name "*.wdl" > 2.txt

    # each line starts with the directory name. those are different, so we normliaze them
    sed -i "s/MNT/dxCopy/g" 1.txt
    sed -i "s/$projName//g" 1.txt
    sed -i "s/\/\//\//g" 1.txt

    sed -i "s/MNT/dxCopy/g" 2.txt
    sed -i "s/$projName//g" 2.txt
    sed -i "s/\/\//\//g" 2.txt


    # line ordering could be different
    sort 1.txt > 1.s.txt
    sort 2.txt > 2.s.txt

    diff 1.s.txt 2.s.txt > D.txt || true
    if [[ -s D.txt ]]; then
        echo "find, when looking for files *.conf, doesn't produce the same results"
        cat D.txt
    fi
}

function check_grep {
    grep -R --include="*.wdl" "task" $dxfuseDir > 1.txt
    grep -R --include="*.wdl" "task" $dxpyDir > 2.txt

    # each line starts with the directory name. those are different, so we normliaze them

    sed -i "s/MNT/dxCopy/g" 1.txt
    sed -i "s/$projName//g" 1.txt
    sed -i "s/\/\//\//g" 1.txt

    sed -i "s/MNT/dxCopy/g" 2.txt
    sed -i "s/$projName//g" 2.txt
    sed -i "s/\/\//\//g" 2.txt

    # line ordering could be different
    sort 1.txt > 1.s.txt
    sort 2.txt > 2.s.txt

    diff 1.s.txt 2.s.txt > D.txt || true
    if [[ -s D.txt ]]; then
        echo "grep -R 'stream' doesn't produce the same results"
        cat D.txt
        exit 1
    fi
}

# copy a bunch of files in parallel
function check_parallel_cat {
    local top_dir="$mountpoint/$projName/reference_data"
    local target_dir="/tmp/write_test_dir"
    local TOTAL_NUM_FILES=3

    mkdir -p $target_dir
    local all_files=$(find $top_dir -type f)

    # limit the number of files in the test
    num_files=0
    files=""
    for f in $all_files; do
        # limit the number of files in the test
        num_files=$((num_files + 1))
        files="$files $f"
        if [[ $num_files == $TOTAL_NUM_FILES ]]; then
            break
        fi
    done

    # copy the chosen files in parallel
    local pids=()
    for f in $files; do
        echo "copying $f"
        b_name=$(basename $f)
        cat $f > $target_dir/$b_name &
        pids="$pids $!"
    done

    # wait for jobs to complete
    for pid in ${pids[@]}; do
        wait $pid
    done

    # compare resulting files
    echo "comparing files"
    for f in $files; do
        b_name=$(basename $f)
        diff $f $target_dir/$b_name
    done

    rm -r $target_dir
}


function compare_with_dx_download {
    echo "download recursively with dx download"
    parentDxpyDir=$(dirname $dxpyDir)
    if [[ ! -d $parentDxpyDir ]]; then
        echo "downloading into $parentDxpyDir from $projName:/$dxDirOnProject"
        mkdir -p $parentDxpyDir
        dx download --no-progress -o $parentDxpyDir -r $projName:/$dxDirOnProject
    fi

    # do not exit immediately if there are differences; we want to see the files
    # that aren't the same
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

   # find
    echo "find"
    check_find

    # grep
    echo "grep"
    check_grep

    # tree
    echo "tree"
    check_tree

    # ls
    echo "ls -R"
    check_ls

    # find
    echo "head, tail, wc"
    check_cmd_line_utils

    echo "parallel downloads"
    check_parallel_cat
}

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

    # wait for the file to achieve the closed state
    while true; do
        file_state=$(dx describe $projName:/$target_dir/A.txt --json | grep state | awk '{ gsub("[,\"]", "", $2); print $2 }')
        if [[ "$file_state" == "closed" ]]; then
            break
        fi
        sleep 2
    done

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

    # wait for the file to achieve the closed state
    while true; do
        file_state=$(dx describe $projName:/$target_dir/B.txt --json | grep state | awk '{ gsub("[,\"]", "", $2); print $2 }')
        if [[ "$file_state" == "closed" ]]; then
            break
        fi
        sleep 2
    done

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

# create directory on mounted FS
function create_dir {
    local src_dir=$1
    local write_dir=$2

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
        echo "letting the files complete uploading"
        sleep 10
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

function compare_symlink_content {
    local trg_dir="${baseDir}/dxCopySymlinks"
    rm -rf $trg_dir
    mkdir -p $trg_dir

    dx download --no-progress -o $trg_dir -r  $projName:/symlinks
    diff -r --brief $mountpoint/$projName/symlinks $trg_dir/symlinks > diff.txt || true
    if [[ -s diff.txt ]]; then
        echo "Difference in symlink content"
        cat diff.txt
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



function hard_links {
    local root_dir=$1
    cd $root_dir

    ln $mountpoint/dxfuse_test_read_only/doc/approaches.md .
    stat approaches.md
    rm -f approaches.md
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

function dir_and_file_with_the_same_name {
    local root_dir=$1

    tree $root_dir/same_names
}

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # don't leak the token to stdout
    if [[ $DX_JOB_ID == "" ]]; then
        # local machine
        rm -f ENV
        dx env --bash > ENV
        source ENV >& /dev/null
        dxfuse="/go/bin/dxfuse"
    else
        # Running on a cloud worker
        source environment >& /dev/null
        dxfuse="dxfuse"
    fi

    # clean and make fresh directories
    mkdir -p $mountpoint

    # generate random alphanumeric strings
    base_dir=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    base_dir="base_$base_dir"
    faux_dir=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    faux_dir="faux_$faux_dir"
    expr_dir=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
    expr_dir="expr_$expr_dir"
    writeable_dirs=($base_dir $faux_dir $expr_dir)
    for d in ${writeable_dirs[@]}; do
        dx rm -r $projName:/$d >& /dev/null || true
    done

    dx mkdir $projName:/$base_dir
    populate_faux_dir $faux_dir
    dx mkdir $projName:/$expr_dir

    target_dir=$base_dir/T1
    dx mkdir $projName:/$target_dir

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags=""
    if [[ $verbose != "" ]]; then
        flags="-verbose 2"
    fi
    sudo -E $dxfuse -uid $(id -u) -gid $(id -g) $flags $mountpoint dxfuse_test_data dxfuse_test_read_only

    echo "comparing symlink content"
    compare_symlink_content

    echo "compare with dx download"
    compare_with_dx_download

    echo "can write to a small file"
    check_file_write_content $mountpoint/$projName $target_dir

    echo "can write several files to a directory"
    write_files $mountpoint/$projName/$dxDirOnProject/large $mountpoint/$projName/$target_dir

    echo "can't write to read-only project"
    write_to_read_only_project

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

    echo "faux dirs cannot be moved"
    faux_dirs_move $mountpoint/$projName/$faux_dir

    echo "faux dir operations"
    faux_dirs_remove $mountpoint/$projName/$faux_dir

    echo "hard links"
    hard_links $mountpoint/$projName/$faux_dir

    echo "directory and file with the same name"
    dir_and_file_with_the_same_name $mountpoint/$projName

    teardown
}
