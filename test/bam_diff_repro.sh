#!/bin/bash -e

proj="project-FYXkqf80bG98k75f58F4Bfbk"

sudo -E /go/bin/dxfuse -uid $(id -u) -gid $(id -g) ~/MNT $proj
sample_name='SRR10270774'

cd ~/MNT/BWA\ MEM2/0_scale_test/HiSeq_2500/HiSeq-2500

bam="wgs/bwamem/${sample_name}_markdup.bam"
bam2="wgs/bwamem2/${sample_name}_markdup.bam"

ls -lh $bam $bam2

time bam diff --in1 "$bam" --in2 "$bam2" --onlyDiffs --baseQual --tags MD:Z,NM:i,MQ:i,RG:Z,XA:Z,XS:i | gzip > ~/${sample_name}_diff.txt.gz
