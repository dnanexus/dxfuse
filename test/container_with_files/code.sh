#!/bin/bash -e

main() {
    mkdir -p out/foo
    mkdir -p out/bar
    echo "just a test file with junk data" >> out/foo/A.txt
    echo "just a test file with junk data" >> out/bar/B.txt
    dx-upload-all-outputs
}
