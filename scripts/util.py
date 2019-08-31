#!/usr/bin/env python
import dxpy

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
