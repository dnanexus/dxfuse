#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -o pipefail

######################################################################
## constants

projName="dxfuse_test_data"
dxDirOnProject="correctness"

baseDir="$HOME/dxfuse_test"
dxTrgDir="${baseDir}/dxCopy"
mountpoint="${baseDir}/MNT"

dxfuseDir="$mountpoint/$projName/$dxDirOnProject"
dxpyDir="${baseDir}/dxCopy/$dxDirOnProject"

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
    find $dxfuseDir -type f -name "*.conf" > 1.txt
    find $dxpyDir -type f -name "*.conf" > 2.txt

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
    grep --directories=skip -R "stream" $dxfuseDir/dxWDL_source_code/src > 1.txt
    grep --directories=skip -R "stream" $dxpyDir/dxWDL_source_code/src > 2.txt

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

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # don't leak the token to stdout
    source environment >& /dev/null

    # clean and make fresh directories
    for d in $dxTrgDir $mountpoint; do
        mkdir -p $d
    done

    # download with dxfuse
    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    sudo -E dxfuse $mountpoint $DX_PROJECT_CONTEXT_ID &
    sleep 1

    echo "download recursively with dx download"
    dx download --no-progress -o $dxTrgDir -r  "$DX_PROJECT_CONTEXT_ID:/$dxDirOnProject"

    # do not exit immediately if there are differences; we want to see the files
    # that aren't the same
    diff -r --brief $dxpyDir $dxfuseDir > diff.txt || true
    if [[ -s diff.txt ]]; then
        echo "Difference in basic file structure"
        cat diff.txt
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

    echo "unmounting dxfuse"
    sudo umount $mountpoint
}
