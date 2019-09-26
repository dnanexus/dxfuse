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
dxfs2TrgDir="${baseDir}/dxfs2Copy"
mountpoint="${baseDir}/MNT"


######################################################################

main() {
    # Get all the DX environment variables, so that dxfs2 can use them
    echo "loading the dx environment"

    # don't leak the token to stdout
    source environment >& /dev/null

    # clean and make fresh directories
    for d in $dxTrgDir $dxfs2TrgDir $mountpoint; do
        mkdir -p $d
    done

    # download with dxfs2
    # Start the dxfs2 daemon in the background, and wait for it to initilize.
    echo "Mounting dxfs2"
    sudo -E dxfs2 $mountpoint $projId &

    sleep 1

    echo "copying from a dxfs2 mount point"
    cp -r  "$mountpoint/$projName/$dxDirOnProject" $dxfs2TrgDir
    echo "unmounting dxfs2"
    sudo umount $mountpoint


    echo "download recursively with dx download"
    dx download --no-progress -o $dxTrgDir -r  "$projId:/$dxDirOnProject"

    # do not exit immediately if there are differences; we want to see the files
    # that aren't the same
    mkdir -p $HOME/out/result
    diff -r --brief $dxTrgDir $dxfs2TrgDir > $HOME/out/result/results.txt || true

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
