#!/bin/bash -e

######################################################################
## constants

projName="dxfuse_test_data"
mountpoint=$HOME/MNT

######################################################################

main() {
    # Get all the DX environment variables, so that dxfuse can use them
    echo "loading the dx environment"

    # don't leak the token to stdout
    if [[ $DX_JOB_ID == "" ]]; then
        # local machine
        rm -f ENV
        dx env --bash > ENV
        source ENV >& /dev/null
        dxfuse="/go/bin/dxfuse"
    else
        # Running on a cloud worker
        source environment >& /dev/null
        dxfuse="dxfuse"
    fi

    # clean and make fresh directories
    mkdir -p $mountpoint

    # Start the dxfuse daemon in the background, and wait for it to initilize.
    echo "Mounting dxfuse"
    flags=""
    if [[ $verbose != "" ]]; then
        flags="-verbose 2"
    fi
    $dxfuse $flags $mountpoint $projName

    # we get bam from the resources
    sudo apt-get install -y g++

    # install samtools
    sudo apt-get install -y samtools

    # install sambamba
    # sudo apt-get install sambamba
    # we get sambamba from the resources directory
    cd $mountpoint/$projName/reference_data/bam

    start=`date +%s`
    bam diff \
         --in1 SRR10270774_markdup.A.bam \
         --in2 SRR10270774_markdup.B.bam \
         --onlyDiffs --baseQual --tags MD:Z,NM:i,MQ:i,RG:Z,XA:Z,XS:i | gzip > ~/SRR10270774_diff.txt.gz
    end=`date +%s`
    runtime=$((end-start))
    dx-jobutil-add-output --class=string runtime_bam_diff "$runtime seconds"

    # check that samtools works
    echo "samtools view"
    num_lines=$(samtools view SRR10270774_markdup.A.bam | wc -l)
    dx-jobutil-add-output --class=int num_lines $num_lines

    echo "sambamba"
    start=`date +%s`
    sambamba flagstat SRR10270774_markdup.A.bam -p > ~/sambamba_info.txt
    end=`date +%s`
    runtime=$((end-start))
    dx-jobutil-add-output --class=string runtime_sambamba "$runtime seconds"

#    echo "samtools split"
    #    samtools split --threads 1 -u SJAML030069_D1.RNA-Seq.unaccounted_reads.bam -f '%*_%!.%.' > ~/SJAML030069_D1.RNA-Seq.bam
#    start=`date +%s`
#    samtools split --threads 1 -u SRR10270774_markdup.A.bam -f '%*_%!.%.' > ~/filter_A.bam
#    end=`date +%s`
#    runtime=$((end-start))
#    dx-jobutil-add-output --class=string runtime_samtools_split "$runtime seconds"
}
