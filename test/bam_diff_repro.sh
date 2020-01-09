#!/bin/bash -e

sudo -E /go/bin/dxfuse -uid $(id -u) -gid $(id -g) ~/MNT dxfuse_test_data
sample_name='SRR10270774'

cd ~/MNT/dxfuse_test_data/reference_data/bam

bam="${sample_name}_markdup.A.bam"
bam2="${sample_name}_markdup.B.bam"

ls -lh $bam $bam2
rm -f ~/${sample_name}_diff.txt.gz

time bam diff --in1 "$bam" --in2 "$bam2" --onlyDiffs --baseQual --tags MD:Z,NM:i,MQ:i,RG:Z,XA:Z,XS:i | gzip > ~/${sample_name}_diff.txt.gz
