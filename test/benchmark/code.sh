#!/bin/bash -xe --pipefail

######################################################################
## constants

baseDir="$HOME/dxfs2_test"
dxTrgDir="${baseDir}/dxCopy"
dxfs2TrgDir="${baseDir}/dxfs2Copy"
mountpoint="${baseDir}/MNT"
projId="project-FbZ25gj04J9B8FJ3Gb5fVP41"

dxDirOnProject="benchmarks"
#dxDirOnProject="correctness/small"

######################################################################

function copy_with_dx_cat {
    fname=$1
    rm -f /tmp/X
    dx cat "$projId:/$dxDirOnProject/$fname" > /tmp/X
}

function copy_with_dxfs2 {
    fname=$1
    rm -f /tmp/X
    cat "$mountpoint/$dxDirOnProject/$fname" > /tmp/X
}


main() {
    # Get all the DX environment variables, so that dxfs2 can use them
    echo "loading the dx environment"
    source environment

    # clean and make fresh directories
    for d in $dxTrgDir $dxfs2TrgDir $mountpoint; do
        mkdir -p $d
    done

    # download with dxfs2
    # Start the dxfs2 daemon in the background, and wait for it to initilize.
    echo "Mounting dxfs2"
    sudo -E dxfs2 $mountpoint $projId &

    sleep 1

    echo "Discover the benchmark files"
    files=$(ls $mountpoint/$dxDirOnProject)
    echo $files

    mkdir -p $HOME/out/result
    echo "method, time(seconds), filename" > $HOME/out/result/result.txt

    for fname in $files; do
        fname="$(basename $fname)"
        echo $fname

        echo "downloading with dx cat"
        start=`date +%s`
        copy_with_dx_cat $fname
        end=`date +%s`
        runtime1=$((end-start))
        echo "dx-cat, $runtime1, $fname" >> $HOME/out/result/result.txt

        start=`date +%s`
        echo "copying from dxfs2"
        copy_with_dxfs2 $fname
        end=`date +%s`
        runtime2=$((end-start))
        echo "dxfs2, $runtime2, $fname"  >> $HOME/out/result/result.txt
    done

    echo "unmounting dxfs2"
    sudo umount $mountpoint

    dx-upload-all-outputs
}
