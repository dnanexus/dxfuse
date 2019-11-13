#!/usr/bin/env python
import argparse
from collections import namedtuple
import dxpy
import json
import pprint
import os


from typing import Callable, Iterator, Union, Optional, List
import time


def get_project(project_name):
    '''Try to find the project with the given name or id.'''

    # First, see if the project is a project-id.
    try:
        project = dxpy.DXProject(project_name)
        return project
    except dxpy.DXError:
        pass

    project = dxpy.find_projects(name=project_name, return_handler=True, level="VIEW")
    project = [p for p in project]
    if len(project) == 0:
        print('Did not find project {0}'.format(project_name), file=sys.stderr)
        return None
    elif len(project) == 1:
        return project[0]
    else:
        raise Exception('Found more than 1 project matching {0}'.format(project_name))

def main():
    argparser = argparse.ArgumentParser(description="Create symbolic link")
    argparser.add_argument("--project", help="DNAnexus project")
    argparser.add_argument("--name", help="name of symbolic link file")
    argparser.add_argument("--folder", help="folder in project")
    argparser.add_argument("--url", help="the url to reference")

    args = argparser.parse_args()
    if args.project is None:
        print("Must provide project")
        exit(1)
    if args.name is None:
        print("Must provide name")
        exit(1)
    if args.url is None:
        print("Must provide url")
        exit(1)

    dx_proj = get_project(args.project)

    folder = "/"
    if args.folder is not None:
        folder = args.folder

    # create a symlink on the platform, with the correct checksum
    input_params = {
        'name' : args.name,
        'project': dx_proj.get_id(),
        'drive': "drive-PUBLISHED",
        'md5sum': "00000000000000000000000000000000",
        'symlinkPath': {
            'object': args.url
        },
        'folder' : folder
    }

    result = dxpy.api.file_new(input_params=input_params)
    f = dxpy.DXFile(dxid = result["id"], project = dx_proj.get_id())

    desc = f.describe()
    print(desc)

if __name__ == '__main__':
    main()
