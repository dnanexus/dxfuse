version 1.0

task dxfs2_test {
    input {
        String instance
        String suite
    }
    command {
        case ~{suite} in
            correct*)
                /dxfs2_workdir/correctness_test.py
                ;;
            bench*)
                /dxfs2_workdir/streaming_benchmark.py
                ;;
             *)
                /dxfs2_workdir/correctness_test.py
                /dxfs2_workdir/streaming_benchmark.py
                ;;
        esac
    }
    runtime {
        dx_instance_type: instance
        docker: "dx://dxfs2_test_data:/images/dxfs2_testbox.tar"
    }
}
