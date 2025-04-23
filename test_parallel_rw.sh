#!/bin/bash -e
# Test script for parallel read/write operations on a dxfuse mount to check resource starvation

# Create directory for timing logs
mkdir -p /tmp/dxfuse_timing
rm -f /tmp/dxfuse_timing/read_*.time /tmp/dxfuse_timing/write_*.time

pkill dxfuse || true
umount /home/kjensen/MNT || true
# mount dxfuse
/home/kjensen/dxfuse -verbose 2 -limitedWrite /home/kjensen/MNT testing
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

# read 25 1GiB files with timing
echo "Starting read operations..."
for i in {1..25}; do
  (time_start=$(date +%s.%N); 
   cat /home/kjensen/MNT/testing/1gb$i >/dev/null; 
   time_end=$(date +%s.%N); 
   echo "$time_end - $time_start" | bc > /tmp/dxfuse_timing/read_$i.time) &
done

# write 25 1GiB files with timing
echo "Starting write operations..."
for i in {1..25}; do
  (time_start=$(date +%s.%N); 
   dd if=/dev/zero of=/home/kjensen/MNT/testing/1GiB$i bs=1M count=1024 status=none; 
   time_end=$(date +%s.%N); 
   echo "$time_end - $time_start" | bc > /tmp/dxfuse_timing/write_$i.time) &
done

# time and create 100 1kib files in parallel (commented out)
#for i in {1..100}; do dd if=/dev/zero of=/home/kjensen/MNT/testing/1kib$i bs=1K count=1 & done

# Check that all background processes are done without errors and measure total time
echo "Waiting for all operations to complete..."
TIMEFORMAT="Overall execution time: %3R seconds"
time wait

# Print statistics
echo -e "\n===== TIMING STATISTICS ====="
calculate_stats "/tmp/dxfuse_timing/read_*.time" "Read operations"
calculate_stats "/tmp/dxfuse_timing/write_*.time" "Write operations"
