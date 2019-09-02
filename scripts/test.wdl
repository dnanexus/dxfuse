version 1.0

task dxfs2_test {
    command {
        ./dxfs2_workdir/correctness_test.py
        ./dxfs2_workdir/streaming_benchmark.py
    }
    runtime {
        dx_instance_type: "mem1_ssd1_x16"
        docker: "dxfs2_test_data:/images/dxfs2_testbox.tar"
    }
}
