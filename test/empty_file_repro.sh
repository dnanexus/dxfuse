#!/bin/bash -e

sudo rm -f /var/log/dxfuse.log
sudo -E /go/bin/dxfuse -verbose 2 -uid $(id -u) -gid $(id -g) ~/MNT dxfuse_test_data

#f=$HOME/MNT/dxfuse_test_data/e1.txt
#rm -f $f
#echo "nuts and berries" > $f

e=$HOME/MNT/dxfuse_test_data/empty.txt
rm -f $e
touch $e

sleep 2s
sudo umount ~/MNT

cat /var/log/dxfuse.log
