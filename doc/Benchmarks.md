# Benchmarks

updated: 11/17/2021
https://github.com/dnanexus/dxfuse/pull/73

## Upload benchmarks

Upload benchmarks are from an Ubuntu 20.04 DNAnexus worker mem2_ssd1_v2_x32 (AWS m5d.8xlarge) instance running kernel 5.4.0-1055-aws.
`dx` and `dxfuse` benchmark commands were run like so. These benchmarks are not exact because they include the wait time until the uploaded file is transitioned to the `closed` state.

`time dd if=/dev/zero bs=1M count=$SIZE | dx upload --wait -`

`time dd if=/dev/zero bs=1M count=$SIZE of=MNT/project/$SIZE`
| dx upload --wait (seconds) | dxfuse upload(seconds) | file size |
| ---                        |  ----                  | ----      |
|	4.4 |	3.5 | 100MiB |
|	4.8  |	4.2 | 200MiB |
|	5.9 |	4.8 | 400MiB |
|	5.9 |	6.8 | 800MiB |
|	7 |	10 | 1GiB |
|	22.5 |	19.2 | 2GiB |
|	37.8  |	87 | 10GiB |
|	254  |	495 | 100GiB |

## Download benchmarks

Streaming a file from DNAnexus using dxfuse performs similarly to dx-toolkit.
The following table shows performance across several
instance types. The benchmark was *how many seconds does it take to
download a file of size X?* The lower the number, the better. The two
download methods were (1) `dx cat`, and (2) `cat` from a dxfuse mount point.

| instance type   | dx cat (seconds) | dxfuse cat (seconds) | file size |
| ----            | ----             | ---                  |  ----     |
| mem1\_ssd1\_v2\_x4|	         207 |	219                 |       17G |
| mem1\_ssd1\_v2\_x4|	          66 |      	         77 |      5.9G |
| mem1\_ssd1\_v2\_x4|		6|	4 | 705M|
| mem1\_ssd1\_v2\_x4|		3|	3 | 285M|
| | | | |
| mem1\_ssd1\_v2\_x16|	57|	49 | 17G|
| mem1\_ssd1\_v2\_x16|		22|	24 | 5.9G|
| mem1\_ssd1\_v2\_x16|		3|	3 | 705M|
| mem1\_ssd1\_v2\_x16|		2|	1 | 285M|
| | | | |
| mem3\_ssd1\_v2\_x32|		52|	51 | 17G|
| mem3\_ssd1\_v2\_x32|		20	| 15 | 5.9G |
| mem3\_ssd1\_v2\_x32|		4|	2 | 705M|
| mem3\_ssd1\_v2\_x32|		2|	1 | 285M|

Load on the DNAnexus API servers and the cloud object system is carefully controlled. Bulk calls
are used to describe data objects, and the number of parallel IO requests is bounded.

dxfuse operations can sometimes be slow, for example, if the server is
slow to respond, or has been temporarily shut down (503 mode). This
may cause the filesystem to lose its interactive feel. Running it on a
cloud worker reduces network latency significantly, and is the way it
is used in the product. Running on a local, non cloud machine, runs
the risk of network choppiness.
