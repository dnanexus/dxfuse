#!/bin/bash -xe --pipefail

######################################################################
## constants

projName="dxfuse_test_data"

baseDir="$HOME/dxfuse_test"
dxTrgDir="${baseDir}/dxCopy"
dxfuseTrgDir="${baseDir}/dxfuseCopy"
mountpoint="${baseDir}/MNT"

dxDirOnProject="benchmarks"
#dxDirOnProject="correctness/small"

######################################################################

function copy_with_dxfuse {
    fname=$1
    rm -f /tmp/X

}


main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"
    source environment

    # clean and make fresh directories
    for d in $dxTrgDir $dxfuseTrgDir $mountpoint; do
        mkdir -p $d
    done

    # download with dxfuse
    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    sudo -E dxfuse $mountpoint $DX_PROJECT_CONTEXT_ID &

    sleep 1

    echo "Discover the benchmark files"
    files=$(ls $mountpoint/$projName/$dxDirOnProject)
    echo $files

    mkdir -p $HOME/out/result
    echo "method,time(seconds),filename" > $HOME/out/result/result.txt

    for fname in $files; do
        fname="$(basename $fname)"
        echo $fname
        rm -f /tmp/X_dxfuse
        rm -f /tmp/X_dx_cat

        echo "downloading with dx cat"
        start=`date +%s`
        dx cat "$DX_PROJECT_CONTEXT_ID:/$dxDirOnProject/$fname" > /tmp/X_dx_cat
        end=`date +%s`
        runtime1=$((end-start))

        start=`date +%s`
        echo "copying from dxfuse"
        cat "$mountpoint/$projName/$dxDirOnProject/$fname" > /tmp/X_dxfuse
        end=`date +%s`
        runtime2=$((end-start))

        echo "sanity check, compare data"
        diff /tmp/X_dxfuse /tmp/X_dx_cat

        sizeDesc=$(ls -lh /tmp/X_dxfuse | cut -d ' ' -f 5)
        echo "dx-cat,$runtime1,$sizeDesc"  >> $HOME/out/result/result.txt
        echo "dxfuse,$runtime2,$sizeDesc"  >> $HOME/out/result/result.txt
    done

    echo "unmounting dxfuse"
    sudo umount $mountpoint

    results=$(cat $HOME/out/result/result.txt)
    for line in $results; do
        dx-jobutil-add-output --array --class=array:string result $line
    done

    #dx-upload-all-outputs
}
