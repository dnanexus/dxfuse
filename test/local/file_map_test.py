import mmap
import os

home_dir = os.getenv("HOME")
extras_json = "{}/MNT/dxfuse_test_data/mini/extras.json".format(home_dir)
fd = open(extras_json, 'rb')
mmap.mmap(fd.fileno(), 0, mmap.PROT_READ)
fd.readline()
