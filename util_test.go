package dxfuse

import "testing"

func TestCalcHttpClientPoolSize_SaneBounds(t *testing.T) {
	// Low CPU count should not under-allocate below the minimum.
	if got := calcHttpClientPoolSize(2); got != MinHttpClientPoolSize {
		t.Fatalf("expected pool size %d for 2 CPUs, got %d", MinHttpClientPoolSize, got)
	}

	// Very large CPU count should be capped.
	if got := calcHttpClientPoolSize(128); got != 128 {
		t.Fatalf("expected pool size capped at 128 for 128 CPUs, got %d", got)
	}
}

func TestCalcUploadWorkerCount_SaneBounds(t *testing.T) {
	// Low CPU count should not go below the min write buffers.
	if got := calcUploadWorkerCount(2); got != MinNumWriteBuffers {
		t.Fatalf("expected upload workers %d for 2 CPUs, got %d", MinNumWriteBuffers, got)
	}

	// Large CPU count should be capped to avoid runaway concurrency.
	if got := calcUploadWorkerCount(128); got != 64 {
		t.Fatalf("expected upload workers capped at 64 for 128 CPUs, got %d", got)
	}
}
