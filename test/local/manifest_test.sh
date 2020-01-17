CRNT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

function manifest_test {
    local mountpoint=${HOME}/MNT
    mkdir -p $mountpoint

    sudo -E /go/bin/dxfuse -verbose 2 -uid $(id -u) -gid $(id -g) $mountpoint $CRNT_DIR/two_files.json

    tree $mountpoint
    full_path=/correctness/small/A.txt
    local content=$(cat $mountpoint/A.txt)
    local content_dx=$(dx cat $full_path)

    if [[ "$content" == "$content_dx" ]]; then
        echo "$full_path +"
    else
        echo "file $full_path has incorrect content"
        exit 1
    fi
    sudo umount $mountpoint
}
