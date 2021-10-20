#!/bin/bash -xe --pipefail

######################################################################
## constants

baseDir="$HOME/dxfuse_test"
mountpoint="${baseDir}/MNT"

# Directories created during the test
writeable_dirs=()
######################################################################

teardown_complete=0

function teardown {
    echo "teardown"
#    for d in ${writeable_dirs[@]}; do
#        dx rm -r $DX_PROJECT_CONTEXT_ID:/$out_dir >& /dev/null || true
#    done
}

# trap any errors and cleanup
trap teardown EXIT

######################################################################

function measure_and_compare {
    local top_dir=$1
    local data_dir=$2
    local output_file=$3
    set -x

    echo "Discover the benchmark files"
    files=$(ls $top_dir/$data_dir)
    echo $files

    echo "file-size,method1,time(seconds),method2,time(seconds)" > $output_file

    for fname in $files; do
        echo $fname
        rm -f /tmp/X_dxfuse
        rm -f /tmp/X_dx_cat

        echo "downloading with dx cat"
        start=`date +%s`
        dx cat $DX_PROJECT_CONTEXT_ID:/$data_dir/$fname > /tmp/X_dx_cat
        end=`date +%s`
        runtime1=$((end-start))

        start=`date +%s`
        echo "copying from dxfuse"
        cat $top_dir/$data_dir/$fname > /tmp/X_dxfuse
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
    local out_dir=$3
    local result_file=$4
    set -x

    echo "Discover the benchmark files"
    files=$(ls $top_dir/$data_dir)
    echo $files

    echo "file-size,method1,time(seconds),method2,time(seconds)" > $result_file

    for fname in $files; do
        fileSize=$(ls -l $top_dir/$data_dir/$fname | cut -d ' ' -f 5)

        sizeDesc=$(ls -lh $top_dir/$data_dir/$fname | cut -d ' ' -f 5)
        echo "size($fname) = $sizeDesc"

        echo "downloading $fname with dx cat"
        dx cat $DX_PROJECT_CONTEXT_ID:/$data_dir/$fname > /tmp/X

        # Measure upload times
        echo "upload with dx"
        start=`date +%s`
        dx upload /tmp/X --path $DX_PROJECT_CONTEXT_ID:/$out_dir/$fname.1 --wait
        end=`date +%s`
        runtime1=$((end-start))

        echo "copy into dxfuse filesystem"
        start=`date +%s`
        cp /tmp/X $top_dir/$out_dir/$fname.2
        end=`date +%s`
        runtime2=$((end-start))

        echo "sanity check, compare data"
        diff /tmp/X $top_dir/$out_dir/$fname.2

        echo "$sizeDesc,dx-upload,$runtime1,dxfuse,$runtime2"  >> $result_file
    done
}

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"
    source environment

    mkdir -p $mountpoint
    set -x

    # create an output directory for uploads
    # generate a random alphanumeric string
    out_dir=$(dd if=/dev/urandom bs=15 count=1 2>/dev/null| base64 | tr -dc 'a-zA-Z0-9'|fold -w 12|head -n1)
    out_dir="out_$out_dir"
    writeable_dirs=($out_dir)
    dx mkdir $DX_PROJECT_CONTEXT_ID:/$out_dir

    # download with dxfuse
    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags="-limitedWrite"
    if [[ $verbose != "" ]]; then
        flags="$flags -verbose 1"
    fi
    dxfuse $flags $mountpoint $DX_PROJECT_CONTEXT_ID
    sleep 1
    projName=$(ls $mountpoint)
    echo "projName = $projName"

    mkdir -p $HOME/out
    measure_and_compare $mountpoint/$projName benchmarks $HOME/out/result.txt
    measure_and_compare $mountpoint/$projName benchmarks_symlinks $HOME/out/result_symlinks.txt
    measure_and_compare_upload $mountpoint/$projName benchmarks $out_dir $HOME/out/result_upload.txt

    echo "unmounting dxfuse"
    fusermount -u $mountpoint

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

    teardown
}
