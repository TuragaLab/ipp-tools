""" This module contains slurm related utilities
"""

import subprocess
import socket
import time
import os

from ipp_tools.utils import find_free_profile, package_path

PROFILE_NAME = 'profile_slurm'
# need a package path util

# need to check if the profile is installed

# check if cluster is already running with the desired profile
# if not duplicate it
# so profile = profile_slurm_{id}

# start controller locally
# submit ipengine to cluster
# wait for engines
# once engines are ready submit jobs to load balancer
# once jobs are finished take down cluster

def slurm_map(fnc, iterables, resource_spec, env='root', job_name=None, output_path=None):
    """

    Args:
      fnc
      iterables
      resource_spec
      env: virtual env to launch engines in
      job_name: name of job to use. Derived from fnc name if not specified
      output_path: location to direct output to.
        If unspecified output is sent to a file (based on job name and timestamp) in ~/logs/slurm

    """
    resource_spec = process_resource_spec(resource_spec)

    # find an unused profile or create one if none exist
    free_profile = find_free_profile(PROFILE_NAME)
    print('Using profile: {}'.format(free_profile))

    submission_time = time.strftime("%Y%m%d-%H%M%S")

    controller_cmd_template = ('source activate {env};'
                               ' ipcontroller --profile={profile} --sqlitedb --location={hostname} --ip="*"')
    controller_cmd = controller_cmd_template.format(
        env=env, profile=free_profile, location=socket.gethostname()
    )

    print("Starting controller with: {} \n".format(controller_cmd))
    # runs in the background if executed this way
    subprocess.Popen(controller_cmd, shell=True)

    print("Sleeping for 10")
    time.sleep(10)

    engine_cmd_template_path = package_path() + '/templates/slurm_template.sh'
    with open(engine_cmd_template_path,  'r') as engine_cmd_template_file:
        engine_command_template = engine_cmd_template_file.read()


    # prepare engine commands
    if job_name is None:
        job_name = fnc.__name__ + '_slurm_map'
    else:
        assert isinstance(job_name, str)

    if output_path is None:
        output_dir = os.path.expanduser('~/logs/slurm')
        output_path = '{}/{}_{}'.format (output_dir, job_name, submission_time)

        if not os.path.exists(output_dir):
            os.makedirs(output_path)
    else:
        assert isinstance(output_path, str)
        assert os.path.exists(output_path)

    engine_command = engine_command_template.format(
        job_name=job_name,
        output_path=output_path,
        n_tasks=resource_spec['n_workers'],
        mem_mb=resource_spec['worker_mem_mb'],
        n_cpus=resource_spec['worker_n_cpus'],
        n_gpus=resource_spec['worker_n_gpus'],
        dev_env=env,
        profile=free_profile,
        controller_hostname=socket.gethostname()
    )

    print("Starting engines")
    # runs in the background if executed this way
    subprocess.Popen(engine_command, shell=True)

    print("Sleeping for 30")
    time.sleep(30)


    # TODO: finish
    # try to connect to cluster
    # check number of workers
    # if less than allocated, close client, sleep, and try again

    # run tasks

    # once finished, shutdown cluster


def process_resource_spec(resource_spec):
    """ Process resource spec, filling in missing fields with default values
    """
    default_spec = {
        'n_workers': 1,
        'worker_n_cpus': 4,
        'worker_n_gpus': 0,
        'worker_mem_mb': 32000
    }
    assert set(resource_spec.keys()) <= set(default_spec.keys())
    default_spec.update(resource_spec)
    return default_spec
