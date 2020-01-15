#!/bin/bash -ex

######################################################################
## constants

projName="dxfuse_test_data"

# larger test for a cloud worker
#dxDirOnProject="correctness"
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

    echo "syncing filesystem"
    sync

    echo "unmounting dxfuse"
    cd $HOME
    sudo umount $mountpoint

    if [[ $verbose != "" ]]; then
        mkdir -p out/filesystem_log
        cp /var/log/dxfuse.log out/filesystem_log/
        dx-upload-all-outputs
    fi
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

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

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # Running on a cloud worker
    source environment >& /dev/null

    # clean and make fresh directories
    mkdir -p $mountpoint

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags=""
    if [[ $verbose != "" ]]; then
        flags="-verbose 1"
    fi
    sudo -E dxfuse -uid $(id -u) -gid $(id -g) $flags $mountpoint dxfuse_test_data

    echo "comparing symlink content"
    compare_symlink_content

    echo "compare with dx download"
    compare_with_dx_download

    teardown
}
