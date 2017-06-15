""" This module contains slurm related utilities
"""

import subprocess
import socket
import time
import os

from ipyparallel import Client
from warnings import warn

from ipp_tools.utils import profile_installed, install_profile, package_path


PROFILE_NAME = 'profile_slurm'

def slurm_map(fnc, iterables, resource_spec,
              env='root', job_name=None, output_path=None,
              n_retries=5, patience=30):
    """

    Args:
      fnc
      iterables
      resource_spec
      env: virtual env to launch engines in
      job_name: name of job to use. Derived from fnc name if not specified
      output_path: location to direct output to.
        If unspecified output is sent to a file (based on job name and timestamp) in ~/logs/slurm
      n_retries: number of times to retry connecting to client if less than the requested number
        of workers are available.
      patience: seconds to wait after failed attempt to connect to client

    """
    resource_spec = process_resource_spec(resource_spec)

    if not profile_installed(PROFILE_NAME):
        print("No profile found for {}, installing".format(PROFILE_NAME))
        install_profile(PROFILE_NAME)

    submission_time = time.strftime("%Y%m%d-%H%M%S")
    cluster_id = '{}_{}'.format(fnc.__name__, submission_time)
    print("Using cluster id: {}".format(cluster_id))

    # break down by line:
    # run in bash
    # activate the specified environment
    # launch controller with desired settings
    controller_cmd_template = ("exec bash -c '"
                               "source activate {env};"
                               " ipcontroller --profile={profile} --sqlitedb --location={hostname} --ip=\'*\' --cluster-id={cluster_id}'")
    controller_cmd = controller_cmd_template.format(
        env=env, profile=PROFILE_NAME, hostname=socket.gethostname(), cluster_id=cluster_id
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
        n_tasks=resource_spec['max_workers'],
        mem_mb=resource_spec['worker_mem_mb'],
        n_cpus=resource_spec['worker_n_cpus'],
        n_gpus=resource_spec['worker_n_gpus'],
        dev_env=env,
        profile=PROFILE_NAME,
        controller_hostname=socket.gethostname(),
        cluster_id=cluster_id
    )
    # wrap command to execute in bash
    engine_command = "exec bash -c '{}'".format(engine_command)

    print("Starting engines")
    # runs in the background if executed this way
    subprocess.Popen(engine_command, shell=True)
    print("Sleeping for {}".format(patience))
    time.sleep(patience)

    # TODO: shut down unused engines
    for attempt_idx in range(n_retries):
        print("Attempt {} to connect to cluster".format(attempt_idx))
        try:
            client = Client(profile=PROFILE_NAME, cluster_id=cluster_id)
            if resource_spec['min_workers'] <= len(client.ids) <= resource_spec['max_workers']:
                print('Succesfully connected to cluster with {} engines out of {} requested'.format(
                    len(client.ids), resource_spec['max_workers']))

                if len(client.ids) < resource_spec['max_workers']:
                    warn("{} slurm jobs submitted but only {} are being used.".format(
                        resource_spec['max_workers'], len(client.ids)))
                break
            else:
                print("{} available engines less than minimum requested of {}".format(
                    len(client.ids), resource_spec['min_workers']))
                print("Retrying after {}".format(patience))
                client.close()
                time.sleep(patience)
        except OSError as os_err:
            print("Caught OSError while attempting to connect to {}: {}.".format(PROFILE_NAME, os_err))
        except TimeoutError as timeout_err:
            print("Caught TimeoutError while attempting to connect to {}: {}".format(PROFILE_NAME, timeout_err))

    # run tasks
    print("Submitting tasks")
    start_time = time.time()
    lb_view = client.load_balanced_view()
    result = lb_view.map(fnc, iterables, block=True)
    print("Tasks finished after {} seconds".format(time.time() - start_time))

    print("Shutting down cluster")
    client.shutdown(hub=True)
    print("Relinquishing slurm nodes")
    shutdown_cmd =  'scancel --jobname={job_name}'.format(job_name=job_name)
    shutdown_cmd = "exec bash -c '{}'".format(shutdown_cmd)
    # runs in the background if executed this way
    subprocess.Popen(shutdown_cmd, shell=True)

    return result


def process_resource_spec(resource_spec):
    """ Process resource spec, filling in missing fields with default values
    """
    default_spec = {
        'max_workers': 1,
        'min_workers': 1,
        'worker_n_cpus': 4,
        'worker_n_gpus': 0,
        'worker_mem_mb': 32000
    }
    assert set(resource_spec.keys()) <= set(default_spec.keys())
    default_spec.update(resource_spec)
    return default_spec
