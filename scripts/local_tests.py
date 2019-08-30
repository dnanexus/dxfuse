#!/usr/bin/env python
import argparse
import os
import shutil
import subprocess
import sys
import time

import dxpy

from typing import Callable, Iterator, Union, Optional, List



def get_project(project_name):
    '''Try to find the project with the given name or id.'''

    # First, see if the project is a project-id.
    try:
        project = dxpy.DXProject(project_name)
        return project
    except dxpy.DXError:
        pass

    project = dxpy.find_projects(name=project_name, name_mode='glob', return_handler=True, level="VIEW")
    project = [p for p in project]
    if len(project) == 0:
        print('Did not find project {0}'.format(project_name), file=sys.stderr)
        return None
    elif len(project) == 1:
        return project[0]
    else:
        raise Exception('Found more than 1 project matching {0}'.format(project_name))

######################################################################

def test_download_entire_project(dxProj):
    dxTrgDir = "/tmp/dxCopy"
    dxfs2TrgDir = "/tmp/dxfs2Copy"
    mountpoint = "/tmp/dxfs2_mountpoint"

    # clean and make fresh directories
    for d in [dxTrgDir, dxfs2TrgDir, mountpoint]:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)

    # download with dxfs2

    # Start the dxfs2 daemon in the background, and wait for it to initilize.
    cmdline = ["sudo", "/go/bin/dxfs2", mountpoint, dxProj.get_id()]
    print(" ".join(cmdline))
    subprocess.Popen(cmdline, close_fds=True)
    time.sleep(1)
    try:
        subprocess.check_output(["cp", "-r", mountpoint + "/correctness/small", dxfs2TrgDir])
    except:
        pass
    subprocess.check_output(["sudo", "umount", mountpoint])

    # download the entire project with dx
    subprocess.check_output(["dx", "download", "-o", dxTrgDir, "-r", ":/correctness/small"])

    # compare
    results = subprocess.check_output(["diff", "-r", "--brief", dxTrgDir, dxfs2TrgDir])
    print("comparison results:")
    print(results)

## Program entry point
def main():
    argparser = argparse.ArgumentParser(description="Tests for dxfs2 filesystem")
    argparser.add_argument("--verbose", help="Verbose outputs",
                           action="store_true", default=False)
    argparser.add_argument("--project", help="DNAx project to take data from",
                           default="dxfs2_test_data")
    args = argparser.parse_args()

    #print("top_dir={} test_dir={}".format(top_dir, test_dir))

    # some sanity checks
    dxProj = get_project(args.project)

    test_download_entire_project(dxProj)

if __name__ == '__main__':
    main()
