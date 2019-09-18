#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -o pipefail

######################################################################
## constants

projName="dxfs2_test_data"
projId="project-FbZ25gj04J9B8FJ3Gb5fVP41"
dxDirOnProject="correctness"

baseDir="$HOME/dxfs2_test"
dxTrgDir="${baseDir}/dxCopy"
mountpoint="${baseDir}/MNT"

dxfs2Dir="$mountpoint/$projName/$dxDirOnProject"
dxpyDir="${baseDir}/dxCopy/$dxDirOnProject"

######################################################################

function check_tree {
    tree -n $dxfs2Dir -o dxfs2.org.txt
    tree -n $dxpyDir -o dxpy.org.txt

    # The first line is different, we need to get rid of it
    tail --lines=+2 dxfs2.org.txt > dxfs2.txt
    tail --lines=+2 dxpy.org.txt > dxpy.txt

    diff dxpy.txt dxfs2.txt > D.txt || true
    if [[ -s D.txt ]]; then
        echo "tree command was not equivalent"
        cat D.txt
        exit 1
    fi
    rm -f dxfs2*.txt dxpy*.txt D.txt
}

function check_ls {
    d=$(pwd)
    cd $dxfs2Dir; ls -R > $d/dxfs2.txt
    cd $dxpyDir; ls -R > $d/dxpy.txt
    cd $d
    diff dxfs2.txt dxpy.txt > D.txt || true
    if [[ -s D.txt ]]; then
        echo "ls -R was not equivalent"
        cat D.txt
        exit 1
    fi
    rm -f dxfs2*.txt dxpy*.txt D.txt
}

function check_cmd_line_utils {
    d=$(pwd)

    cd $dxfs2Dir
    files=$(find . -type f)
    cd $d

    for f in $files; do
        echo $f

        dxfs2_f=$dxfs2Dir/$f
        dxpy_f=$dxpyDir/$f

        # wc should return the same result
        wc < $dxfs2_f > 1.txt
        wc < $dxpy_f > 2.txt
        diff 1.txt 2.txt > D.txt || true
        if [[ -s D.txt ]]; then
            echo "wc for files $dxfs2_f $dxpy_f is not the same"
            cat D.txt
            exit 1
        fi

        # head
        head $dxfs2_f > 1.txt
        head $dxpy_f > 2.txt
        diff 1.txt 2.txt > D.txt || true
        if [[ -s D.txt ]]; then
            echo "head for files $dxfs2_f $dxpy_f is not the same"
            cat D.txt
            exit 1
        fi

        # tail
        tail $dxfs2_f > 1.txt
        tail $dxpy_f > 2.txt
        diff 1.txt 2.txt > D.txt || true
        if [[ -s D.txt ]]; then
            echo "tail for files $dxfs2_f $dxpy_f is not the same"
            cat D.txt
            exit 1
        fi
        rm -f 1.txt 2.txt D.txt
    done
}

main() {
    # Get all the DX environment variables, so that dxfs2 can use them
    echo "loading the dx environment"

    # don't leak the token to stdout
    source environment >& /dev/null

    # clean and make fresh directories
    for d in $dxTrgDir $mountpoint; do
        mkdir -p $d
    done

    # download with dxfs2
    # Start the dxfs2 daemon in the background, and wait for it to initilize.
    echo "Mounting dxfs2"
    sudo -E dxfs2 $mountpoint $projId &
    sleep 1

    echo "download recursively with dx download"
    dx download --no-progress -o $dxTrgDir -r  "$projId:/$dxDirOnProject"

    # do not exit immediately if there are differences; we want to see the files
    # that aren't the same
    diff -r --brief $dxpyDir $dxfs2Dir > diff.txt || true
    if [[ -s diff.txt ]]; then
        echo "Difference in basic file structure"
        cat diff.txt
        exit 1
    fi

    # tree
    echo "Check the tree command"
    check_tree

    # ls
    echo "Checking ls -R"
    check_ls

    # find
    echo "Checking head, tail, wc"
    check_cmd_line_utils

    # stat

    echo "unmounting dxfs2"
    sudo umount $mountpoint
}
