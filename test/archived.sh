#!/bin/bash -e

sudo -E /go/bin/dxfuse -verbose 2 -uid $(id -u) -gid $(id -g) ~/MNT dxfuse_test_data project-Ff0KJpj0g79fF5Yk7j7pjGfK

ls -l $HOME/MNT/dxfuse_test_data/New\ Archival

sudo umount ~/MNT
