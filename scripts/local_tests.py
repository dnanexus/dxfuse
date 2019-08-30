#!/usr/bin/env python
import argparse
import os
from termcolor import colored, cprint
import shutil
import subprocess
import sys
import time

import dxpy

from typing import Callable, Iterator, Union, Optional, List

######################################################################
## constants

userHomeDir = os.environ["HOME"]
baseDir = os.path.join(userHomeDir, "dxfs2_test")
dxTrgDir = os.path.join(baseDir, "dxCopy")
dxfs2TrgDir = os.path.join(baseDir, "dxfs2Copy")
mountpoint = os.path.join(baseDir, "MNT")

dxDirOnProject = "correctness"

######################################################################


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
    cprint("Clearing out directory {} for testing".format(baseDir), "blue")

    # clean and make fresh directories
    # Be careful here, NOT to erase the user home directory
    for d in [dxTrgDir, dxfs2TrgDir, mountpoint]:
        if d == userHomeDir:
            printf("Error, must not erase user home directory")
            os.exit(1)
        if os.path.exists(d):
            subprocess.check_output(["sudo", "rm", "-rf", d])
        os.makedirs(d)

    # download with dxfs2

    # Start the dxfs2 daemon in the background, and wait for it to initilize.
    cmdline = ["sudo", "/go/bin/dxfs2", mountpoint, dxProj.get_id()]
    print(" ".join(cmdline))
    subprocess.Popen(cmdline, close_fds=True)
    time.sleep(1)

    cprint("copying from a dxfs2 mount point", "blue")
    try:
        subprocess.check_output(["cp", "-r", mountpoint + "/" + dxDirOnProject, dxfs2TrgDir])
    except:
        pass
    subprocess.check_output(["sudo", "umount", mountpoint])

    # download the platform directory with 'dx'
    cprint("download recursively with 'dx download -r", "blue")
    subprocess.check_output(["dx", "download", "--no-progress", "-o", dxTrgDir, "-r", ":/" + dxDirOnProject])

    # compare
    resultsBytes = subprocess.check_output(["diff", "-r", "--brief", dxTrgDir, dxfs2TrgDir])
    results = resultsBytes.decode("ascii")
    if results != "":
        cprint("Error, there is a difference between the download methods:", "red")
        print(results)
        os.exit(1)
    else:
        cprint("Success!", "grey", attrs=['bold'])

## Program entry point
def main():
    argparser = argparse.ArgumentParser(description="Tests for dxfs2 filesystem")
    argparser.add_argument("--verbose", help="Verbose outputs",
                           action="store_true", default=False)
    argparser.add_argument("--project", help="DNAx project to take data from",
                           default="dxfs2_test_data")
    args = argparser.parse_args()

    # some sanity checks
    dxProj = get_project(args.project)

    test_download_entire_project(dxProj)

if __name__ == '__main__':
    main()
