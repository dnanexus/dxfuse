#!/usr/bin/env python
import argparse
from collections import namedtuple
import dxpy
import json
import pprint
import os
import sys
import subprocess
import time
from dxpy.exceptions import DXJobFailureError

# The list of instance types to test on. We don't want too many, because it will be expensive.
# We are trying to take a representative from small, medium, and large instances.
aws_ladder = {
    "small" : ["mem1_ssd1_v2_x4"],
    "large" : ["mem1_ssd1_v2_x4", "mem1_ssd1_v2_x16", "mem3_ssd1_v2_x32"]
}

azure_ladder = {
    "small" : ["azure:mem1_ssd1_x4"],
    "large" : ["azure:mem1_ssd1_x4", "azure:mem1_ssd1_x16", "azure:mem3_ssd1_x16"],
}

def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def lookup_applet(name, project, folder):
    wfgen = dxpy.bindings.search.find_data_objects(name= name,
                                                   folder= folder,
                                                   project= project.get_id(),
                                                   limit= 1)
    objs = [item for item in wfgen]
    if len(objs) == 0:
        raise RuntimeError("applet {} not found in folder {}".format(name, folder))
    if len(objs) == 1:
        oid = objs[0]['id']
        return dxpy.DXApplet(project=project.get_id(), dxid=oid)
    raise RuntimeError("sanity")

def wait_for_completion(jobs):
    print("awaiting completion ...")
    # wait for analysis to finish while working around Travis 10m console inactivity timeout
    noise = subprocess.Popen(["/bin/bash", "-c", "while true; do sleep 60; date; done"])
    try:
        for job in jobs:
            try:
                job.wait_on_done()
            except DXJobFailureError:
                raise RuntimeError("Executable {} failed".format(job.get_id()))
    finally:
        noise.kill()
    print("done")

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
        print('Did not find project {0}'.format(project_name))
        return None
    elif len(project) == 1:
        return project[0]
    else:
        raise Exception('Found more than 1 project matching {0}'.format(project_name))


def launch_jobs(project, applet, instance_types, verbose):
    # Run the workflows
    jobs=[]
    desc = applet.describe()
    applet_name = desc["name"]
    print("Launching applet {}".format(applet_name))
    for itype in instance_types:
        print("instance: {}".format(itype))
        app_args = {}
        if verbose:
            app_args = { "verbose" : True }
        job = applet.run(app_args,
                         project=project.get_id(),
                         instance_type=itype)
        jobs.append(job)
    print("executables: " + ", ".join([a.get_id() for a in jobs]))
    return jobs


def extract_results(jobs, field_name, description):
    header = "instance-type, file size, dx cat (seconds), dxfuse (seconds)"
    print(header)
    for j in jobs:
        desc = j.describe()
        i_type = desc["systemRequirements"]["*"]["instanceType"]
        results = desc['output'][field_name]

        # skip the header
        measurements = results[1:]
        print("{}".format(description))
        for line in measurements:
            parts = line.split(",")
            print("{},\t{},\t{},\t{}".format(i_type, parts[0], parts[2], parts[4]))
        print("")

def run_benchmarks(dx_proj, instance_types, verbose):
    applet = lookup_applet("benchmark", dx_proj, "/applets")
    jobs = launch_jobs(dx_proj, applet, instance_types, verbose)
    wait_for_completion(jobs)
    extract_results(jobs, 'result', 'file downloads')
    extract_results(jobs, 'result_symlinks', 'symbolic link downloads')
    extract_results(jobs, 'result_upload', 'file upload')

def run_local_test():
    try:
        print("running local tests")
        p = get_script_path()
        cmd = ["/bin/bash", p + "/local/local.sh"]
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        msg = ""
        if e and e.output:
            msg = e.output.strip()
        print("Failed to run local tests: {cmd}\n{msg}\n".format(cmd=str(cmd), msg=msg))
        sys.exit(1)

def run_correctness(dx_proj, itype, verbose):
    correctness = lookup_applet("correctness", dx_proj, "/applets")
    bio_tools = lookup_applet("bio_tools", dx_proj, "/applets")
    correctness_downloads = lookup_applet("correctness_downloads", dx_proj, "/applets")
    jobs1 = launch_jobs(dx_proj, correctness, [itype], verbose)
    jobs2 = launch_jobs(dx_proj, bio_tools, [itype], verbose)
    jobs3 = launch_jobs(dx_proj, correctness_downloads, [itype], verbose)
    wait_for_completion(jobs1 + jobs2 + jobs3)

def run_biotools(dx_proj, itype, verbose):
    bio_tools = lookup_applet("bio_tools", dx_proj, "/applets")
    jobs = launch_jobs(dx_proj, bio_tools, [itype], verbose)
    wait_for_completion(jobs)

def main():
    argparser = argparse.ArgumentParser(description="Run benchmarks on several instance types for dxfuse")
    argparser.add_argument("--project", help="DNAnexus project",
                           default="dxfuse_test_data")
    argparser.add_argument("--test", help="which testing suite to run [bench, bio, correct, local]")
    argparser.add_argument("--size", help="how large should the test be? [small, large]",
                           default="small")
    argparser.add_argument("--verbose", help="run the tests in verbose mode",
                           action='store_true', default=False)
    args = argparser.parse_args()
    dx_proj = get_project(args.project)

    # figure out which region we are operating in
    region = dx_proj.describe()["region"]
    scale = None
    if region.startswith("aws:"):
        scale = aws_ladder
    elif region.startswith("azure"):
        scale = azure_ladder
    else:
        raise Exception("unknown region {}".format(region))

    if args.size in scale.keys():
        instance_types = scale[args.size]
    else:
        print("Unknown size value {}".format(args.scale))
        exit(1)

    if args.test is None:
        print("Test not specified")
        exit(1)
    if args.test.startswith("bench"):
        run_benchmarks(dx_proj, instance_types, args.verbose)
    elif args.test.startswith("correct"):
        run_correctness(dx_proj, instance_types[0], args.verbose)
    elif args.test.startswith("bio"):
        run_biotools(dx_proj, instance_types[0], args.verbose)
    elif args.test.startswith("local"):
        run_local_test()
    else:
        print("Unknown test {}".format(args.test))
        exit(1)

if __name__ == '__main__':
    main()
