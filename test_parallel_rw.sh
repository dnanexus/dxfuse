#!/bin/bash -e
# Test script for parallel read/write operations on a dxfuse mount to check resource starvation

# Default settings - all tests disabled by default
RUN_READ_TEST=false
RUN_WRITE_TEST=false
RUN_SMALL_TEST=false

# Get number of CPU cores and calculate test counts
NUM_CPUS=$(nproc)
READ_TESTS=$((2 * NUM_CPUS))
WRITE_TESTS=$((2 * NUM_CPUS))
SMALL_TESTS=$((4 * NUM_CPUS))

# Parse command line arguments
usage() {
  echo "Usage: $0 [-r] [-w] [-s] [-a] [-h]"
  echo "  -r: Run read test (${READ_TESTS} 1GiB files - 2 × CPU count)"
  echo "  -w: Run write test (${WRITE_TESTS} 1GiB files - 2 × CPU count)"
  echo "  -s: Run small file test (${SMALL_TESTS} 1KiB files - 4 × CPU count)"
  echo "  -a: Run all tests"
  echo "  -h: Show this help message"
  exit 1
}

# If no arguments provided, show usage
if [ $# -eq 0 ]; then
  usage
fi

while getopts "rwsah" opt; do
  case $opt in
    r) RUN_READ_TEST=true ;;
    w) RUN_WRITE_TEST=true ;;
    s) RUN_SMALL_TEST=true ;;
    a) RUN_READ_TEST=true; RUN_WRITE_TEST=true; RUN_SMALL_TEST=true ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Create directory for timing logs
mkdir -p /tmp/dxfuse_timing
rm -f /tmp/dxfuse_timing/read_*.time /tmp/dxfuse_timing/write_*.time /tmp/dxfuse_timing/small_*.time

pkill dxfuse || true
umount /home/kjensen/MNT || true
# mount dxfuse
/home/kjensen/dxfuse --version
/home/kjensen/dxfuse -limitedWrite /home/kjensen/MNT testing
rm -rf /home/kjensen/MNT/testing/1GiB*
rm -rf /home/kjensen/MNT/testing/1kib*

# Function to calculate min, max, and average from a list of values
calculate_stats() {
  local file_pattern=$1
  local label=$2
  
  if [ -z "$(ls $file_pattern 2>/dev/null)" ]; then
    echo "No data for $label"
    return
  fi
  
  local min=$(sort -n $file_pattern | head -1)
  local max=$(sort -n $file_pattern | tail -1)
  local avg=$(awk '{ sum += $1; n++ } END { if (n > 0) print sum / n; else print 0; }' $file_pattern)
  
  echo "$label statistics:"
  echo "  Minimum: $min seconds"
  echo "  Maximum: $max seconds"
  echo "  Average: $avg seconds"
}

# read files with timing - 2 × CPU count
if $RUN_READ_TEST; then
  echo "Starting read operations (${READ_TESTS} files)..."
  for i in $(seq 1 $READ_TESTS); do
    (time_start=$(date +%s.%N); 
     cat /home/kjensen/MNT/testing/1gb$i >/dev/null; 
     time_end=$(date +%s.%N); 
     echo "$time_end - $time_start" | bc > /tmp/dxfuse_timing/read_$i.time) &
  done
fi

# write files with timing - 2 × CPU count
if $RUN_WRITE_TEST; then
  echo "Starting write operations (${WRITE_TESTS} files)..."
  for i in $(seq 1 $WRITE_TESTS); do
    (time_start=$(date +%s.%N); 
     dd if=/dev/zero of=/home/kjensen/MNT/testing/1GiB$i bs=1M count=1024 status=none; 
     time_end=$(date +%s.%N); 
     echo "$time_end - $time_start" | bc > /tmp/dxfuse_timing/write_$i.time) &
  done
fi

# time and create small files in parallel - 4 × CPU count
if $RUN_SMALL_TEST; then
  echo "Starting small file creation operations (${SMALL_TESTS} files)..."
  for i in $(seq 1 $SMALL_TESTS); do
    (time_start=$(date +%s.%N); 
     dd if=/dev/zero of=/home/kjensen/MNT/testing/1kib$i bs=1K count=1 status=none; 
     time_end=$(date +%s.%N); 
     echo "$time_end - $time_start" | bc > /tmp/dxfuse_timing/small_$i.time) &
  done
fi

# Check that all background processes are done without errors and measure total time
echo -e "Waiting for all operations to complete...\n"
TIMEFORMAT="Overall execution time: %3R seconds"
time wait

# Print statistics
echo -e "\n===== TIMING STATISTICS ====="
if $RUN_READ_TEST; then
  calculate_stats "/tmp/dxfuse_timing/read_*.time" "Read operations (${READ_TESTS} files)"
fi
if $RUN_WRITE_TEST; then
  calculate_stats "/tmp/dxfuse_timing/write_*.time" "Write operations (${WRITE_TESTS} files)"
fi
if $RUN_SMALL_TEST; then
  calculate_stats "/tmp/dxfuse_timing/small_*.time" "Small file operations (${SMALL_TESTS} files)"
fi
