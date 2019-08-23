# Requirements

Performing prefetch for sequential streams can incur overhead and cost
memory. Here are some guidelines on what we can reasonably expect.

The goal is: if a file is read from start to finish, we want to be
able to read it in large network requests. At least 1MB.

1. Limit memory overhead.
2.

# Detection

A file is determined to be streaming if:
1. An IO is issued that is 128KB
2. The 1MB immediately after that IO is read in its entirety. This is measured by a bitmap with 4KB granularity.



limiting tracking memory overhead:
1. There is a global limit of 1000 streaming files.
2. If no IO hits the bitmap area in five minutes, prefetch tracking information for the file is discarded.

When a file is in streaming mode, a single prefetch IO is issued for
the next 1MB, and a bitmap covers that area. If 75% of the area is
read, then the next 1MB is read. This goes on until the file is
closed, or,


# Synchronization with ongoing read requests

If a file is being prefetched, reads to prefetched areas will wait
until the prefetch-IO completes, and copy the data directly from memory.
