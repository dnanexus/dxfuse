#!/bin/bash -xe --pipefail

######################################################################
## constants

projName="dxfs2_test_data"
projId="project-FbZ25gj04J9B8FJ3Gb5fVP41"

baseDir="$HOME/dxfs2_test"
dxTrgDir="${baseDir}/dxCopy"
dxfs2TrgDir="${baseDir}/dxfs2Copy"
mountpoint="${baseDir}/MNT"

dxDirOnProject="benchmarks"
#dxDirOnProject="correctness/small"

######################################################################

function copy_with_dxfs2 {
    fname=$1
    rm -f /tmp/X

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
    files=$(ls $mountpoint/$projName/$dxDirOnProject)
    echo $files

    mkdir -p $HOME/out/result
    echo "method,time(seconds),filename" > $HOME/out/result/result.txt

    for fname in $files; do
        fname="$(basename $fname)"
        echo $fname
        rm -f /tmp/X_dxfs2
        rm -f /tmp/X_dx_cat

        echo "downloading with dx cat"
        start=`date +%s`
        dx cat "$projId:/$dxDirOnProject/$fname" > /tmp/X_dx_cat
        end=`date +%s`
        runtime1=$((end-start))

        start=`date +%s`
        echo "copying from dxfs2"
        cat "$mountpoint/$projName/$dxDirOnProject/$fname" > /tmp/X_dxfs2
        end=`date +%s`
        runtime2=$((end-start))

        echo "sanity check, compare data"
        diff /tmp/X_dxfs2 /tmp/X_dx_cat

        sizeDesc=$(ls -lh /tmp/X_dxfs2 | cut -d ' ' -f 5)
        echo "dx-cat,$runtime1,$sizeDesc"  >> $HOME/out/result/result.txt
        echo "dxfs2,$runtime2,$sizeDesc"  >> $HOME/out/result/result.txt
    done

    echo "unmounting dxfs2"
    sudo umount $mountpoint

    results=$(cat $HOME/out/result/result.txt)
    for line in $results; do
        dx-jobutil-add-output --array --class=array:string result $line
    done

    #dx-upload-all-outputs
}
