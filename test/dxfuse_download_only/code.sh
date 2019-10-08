#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -o pipefail

######################################################################
## constants

projName="dxfuse_test_data"
projId="project-FbZ25gj04J9B8FJ3Gb5fVP41"
dxDirOnProject="correctness"

baseDir="$HOME/dxfuse_test"
dxTrgDir="${baseDir}/dxCopy"
dxfuseTrgDir="${baseDir}/dxfuseCopy"
mountpoint="${baseDir}/MNT"


######################################################################

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # don't leak the token to stdout
    source environment >& /dev/null

    # clean and make fresh directories
    for d in $dxTrgDir $dxfuseTrgDir $mountpoint; do
        mkdir -p $d
    done

    # download with dxfuse
    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    sudo -E dxfuse $mountpoint $projId &

    sleep 1

    echo "copying from a dxfuse mount point"
    cp -r  "$mountpoint/$projName/$dxDirOnProject" $dxfuseTrgDir
    echo "unmounting dxfuse"
    sudo umount $mountpoint


    echo "download recursively with dx download"
    dx download --no-progress -o $dxTrgDir -r  "$projId:/$dxDirOnProject"

    # do not exit immediately if there are differences; we want to see the files
    # that aren't the same
    mkdir -p $HOME/out/result
    diff -r --brief $dxTrgDir $dxfuseTrgDir > $HOME/out/result/results.txt || true

    # If the diff is non empty, declare that the results
    # are not equivalent.
    equivalent="true"
    if [[ -s $HOME/out/result/results.txt ]]; then
        equivalent="false"
    fi

    dx-jobutil-add-output --class=boolean equality $equivalent

    # There was a difference, upload diff files.
    if [[ $equivalent == "false" ]]; then
        dx-upload-all-outputs
    fi
}
