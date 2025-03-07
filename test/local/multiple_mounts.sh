CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
projName="dxfuse_test_data"
dxfuse="$CRNT_DIR/../../dxfuse"
baseDir=$HOME/dxfuse_test
mountpoint0=${baseDir}/MNT
mountpoint1=${baseDir}/MNT1

# cleanup sequence
function teardown {
    if [[ $teardown_complete == 1 ]]; then
        return
    fi
    teardown_complete=1


    echo "unmounting dxfuse"
    cd $HOME
    fusermount -u $mountpoint
    fusermount -u $mountpoint1
}

# trap any errors and cleanup
trap teardown EXIT

function multiple_mounts {
    mkdir -p $mountpoint0
    mkdir -p $mountpoint1

    # mount dxfuse with default folder path
    $dxfuse $mountpoint0 $projName

    folder_path="$baseDir/.dxfusebase"
    # mount dxfuse with a different folder path
    $dxfuse -folderPath $folder_path $mountpoint1 $projName

    # check that two dxfuse processes are running
    dxfuse_process_count=$(pgrep -c dxfuse)
    if [[ $dxfuse_process_count -eq 2 ]]; then
        echo "Two dxfuse processes are running."
    else
        echo "Error: Expected 2 dxfuse processes, but found $dxfuse_process_count."
        exit 1
    fi

    # check that two dxfuse mounts are present
    mount_count=$(mount | grep -c dxfuse)
    if [[ $mount_count -eq 2 ]]; then
        echo "Two dxfuse mounts are present."
    else
        echo "Error: Expected 2 dxfuse mounts, but found $mount_count."
        exit 1
    fi

    # check that the mounts are the same
    if ! diff <(ls -l $mountpoint0) <(ls -l $mountpoint1); then
        echo "Error: The two mounts are different."
        exit 1
    fi

    teardown
}