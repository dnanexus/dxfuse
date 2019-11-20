#!/bin/bash -xe --pipefail

######################################################################
## constants

baseDir="$HOME/dxfuse_test"
dxTrgDir="${baseDir}/dxCopy"
dxfuseTrgDir="${baseDir}/dxfuseCopy"
mountpoint="${baseDir}/MNT"

######################################################################

function measure_and_compare {
    local top_dir=$1
    local data_dir=$2
    local output_file=$3

    echo "Discover the benchmark files"
    files=$(ls $top_dir/$data_dir)
    echo $files

    mkdir -p $HOME/out/result
    echo "file-size,method1,time(seconds),method2,time(seconds)" > $output_file

    for fname in $files; do
        fname="$(basename $fname)"
        echo $fname
        rm -f /tmp/X_dxfuse
        rm -f /tmp/X_dx_cat

        echo "downloading with dx cat"
        start=`date +%s`
        dx cat "$DX_PROJECT_CONTEXT_ID:/$data_dir/$fname" > /tmp/X_dx_cat
        end=`date +%s`
        runtime1=$((end-start))

        start=`date +%s`
        echo "copying from dxfuse"
        cat "$mountpoint/$projName/$data_dir/$fname" > /tmp/X_dxfuse
        end=`date +%s`
        runtime2=$((end-start))

        echo "sanity check, compare data"
        diff /tmp/X_dxfuse /tmp/X_dx_cat

        sizeDesc=$(ls -lh /tmp/X_dxfuse | cut -d ' ' -f 5)
        echo "$sizeDesc,dx-cat,$runtime1,dxfuse,$runtime2"  >> $output_file
    done
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
    projName=$(ls $mountpoint)
    echo "projName = $projName"

    measure_and_compare $mountpoint/$projName benchmarks $HOME/out/result/result.txt
#    measure_and_compare $mountpoint/$projName benchmarks_symlinks $HOME/out/result/result_symlinks.txt

    echo "unmounting dxfuse"
    sudo umount $mountpoint

    results=$(cat $HOME/out/result/result.txt)
    for line in $results; do
        dx-jobutil-add-output --array --class=array:string result $line
    done

#    result_symlinks=$(cat $HOME/out/result/result_symlinks.txt)
#    for line in $result_symlinks; do
#        dx-jobutil-add-output --array --class=array:string result_symlinks $line
#    done
}
