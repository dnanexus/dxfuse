#!/bin/bash -ex
# Test script for parallel read/write operations on a dxfuse mount to check resource starvation
pkill dxfuse || true
umount /home/kjensen/MNT || true
# mount dxfuse
/home/kjensen/dxfuse -verbose 2 -limitedWrite /home/kjensen/MNT testing
rm -rf /home/kjensen/MNT/testing/1GiB*
rm -rf /home/kjensen/MNT/testing/1kib*
# read 5 1GiB files
for i in {1..25}; do cat /home/kjensen/MNT/testing/1gb$i >/dev/null & done
# # write 15 1GiB files
for i in {1..25}; do dd if=/dev/zero of=/home/kjensen/MNT/testing/1GiB$i bs=1M count=1024 & done

# time and create 100 1kib files in parallel
#for i in {1..100}; do dd if=/dev/zero of=/home/kjensen/MNT/testing/1kib$i bs=1K count=1 & done

# Check that all background processes are done without errors
time wait
