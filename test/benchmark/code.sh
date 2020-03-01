#!/bin/bash -xe --pipefail

######################################################################
## constants

baseDir="$HOME/dxfuse_test"
mountpoint="${baseDir}/MNT"

######################################################################

function measure_and_compare {
    local top_dir=$1
    local data_dir=$2
    local output_file=$3

    echo "Discover the benchmark files"
    files=$(ls $top_dir/$data_dir)
    echo $files

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


function measure_and_compare_upload {
    local top_dir=$1
    local data_dir=$2
    local output_file=$3

    echo "Discover the benchmark files"
    files=$(ls $top_dir/$data_dir)
    echo $files

    echo "file-size,method1,time(seconds),method2,time(seconds)" > $output_file

    for fname in $files; do
        fileSize=$(ls -l $top_dir/$data_dir/$fname | cut -d ' ' -f 5)
        if [[ $fileSize -gt $((10 * 1000 * 1000 * 1000)) ]]; then
            # limit file size to 1GiB
            echo "skipping $fname, it is too large"
            continue
        fi

        sizeDesc=$(ls -lh $top_dir/$data_dir/$fname | cut -d ' ' -f 5)
        echo "size($fname) = $sizeDesc"

        echo "downloading $fname with dx cat"
        dx cat $DX_PROJECT_CONTEXT_ID:/$data_dir/$fname > /tmp/X

        # Measure upload times
        echo "upload with dx"
        start=`date +%s`
        dx upload /tmp/X --path $DX_PROJECT_CONTEXT_ID:/$data_dir/$fname.1 --wait
        end=`date +%s`
        runtime1=$((end-start))

        echo "upload with dxfuse"
        start=`date +%s`
        cp /tmp/X $mountpoint/$projName/$data_dir/$fname.2
        echo "start dxfuse sync"
        dxfuse -sync
        end=`date +%s`
        runtime2=$((end-start))

        echo "$sizeDesc,dx-upload,$runtime1,dxfuse,$runtime2"  >> $output_file

        echo "cleanup"
        dx rm $DX_PROJECT_CONTEXT_ID:/$data_dir/$fname.1
        rm $mountpoint/$projName/$data_dir/$fname.2
    done
}

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"
    source environment

    mkdir -p $mountpoint

    # download with dxfuse
    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags=""
    if [[ $verbose != "" ]]; then
        flags="-verbose 1"
    fi
    sudo -E dxfuse -uid $(id -u) -gid $(id -g) $flags $mountpoint $DX_PROJECT_CONTEXT_ID &
    sleep 1
    projName=$(ls $mountpoint)
    echo "projName = $projName"

    mkdir -p $HOME/out
#    measure_and_compare $mountpoint/$projName benchmarks $HOME/out/result.txt
#    measure_and_compare $mountpoint/$projName benchmarks_symlinks $HOME/out/result_symlinks.txt
    measure_and_compare_upload $mountpoint/$projName benchmarks $HOME/out/result_upload.txt

    echo "unmounting dxfuse"
    sudo umount $mountpoint

    echo "reporting results"
    if [[ -f $HOME/out/result.txt ]]; then
        results=$(cat $HOME/out/result.txt)
        for line in $results; do
            dx-jobutil-add-output --array --class=array:string result $line
        done
    fi

    if [[ -f $HOME/out/result_symlinks.txt ]]; then
        result_symlinks=$(cat $HOME/out/result_symlinks.txt)
        for line in $result_symlinks; do
            dx-jobutil-add-output --array --class=array:string result_symlinks $line
        done
    fi

    if [[ -f $HOME/out/result_upload.txt ]]; then
        result_upload=$(cat $HOME/out/result_upload.txt)
        for line in $result_upload; do
            dx-jobutil-add-output --array --class=array:string result_upload $line
        done
    fi
}
