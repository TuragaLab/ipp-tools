""" This module contains methods providing a high-level map interface to an ipp cluster
"""

import six
import socket
import os
import time

import numpy as np

from ipp_tools.gpu import fetch_gpu_status
from ipp_tools.log_tools import setup_logging

WS_N_GPUS = {
    'turagas-ws1': 2,
    'turagas-ws2': 2,
    'turagas-ws3': 2,
    'turagas-ws4': 2,
    'c04u01': 8,
    'c04u07': 8,
    'c04u12': 8,
    'c04u17': 8,
}

def assigned_gpu_is_free(gpu_id):
    """ Checks to see if gpu with gpu_id is free, returns True if so
    """
    max_memory = 0.1
    max_usage = 0.2
    gpu_id = int(gpu_id)
    gpu_status = fetch_gpu_status()
    gpu_ids = [gpu['id'] for gpu in gpu_status]
    if gpu_id not in gpu_id:
        print("GPU {} not found".format(gpu_id))
        return False

    gpu_idx = np.where(gpu_ids == gpu_id)[0].squeeze()
    gpu_status = gpu_status[gpu_idx]

    if gpu_status['memory_frac'] > max_memory:
        print("Wired memory frac exceeds max")
        return False

    if gpu_status['usage_frac'] > max_usage:
        print("Volatile usage exceeds max")
        return False


def gpu_job_runner(job_fnc, job_args, ipp_profile='ssh_gpu_py2', log_name=None, log_dir='~/logs/default',
                   status_interval=600, allow_engine_overlap=True, devices_assigned=False):
    """ Distribute a set of jobs across an IPyParallel 'GPU cluster'
    Requires that cluster has already been started with `ipcluster start --profile={}`.forat(ipp_profile)
    Checks on the jobs every status_interval seconds, logging status.

    Args:
      job_fnc: the function to distribute
        must accept `device` as a kwarg, as this  function is wrapped so that
        device is bound within the engine namespace
        returned values are ignored
      job_args: list of args passed to job_fnc - list
      ipp_profile: profile of GPU IPyParallel profile - str
      log_name: (optional) name for log
      log_dir: (optional), default is ~/logs/default which is created if it doesn't exist
      status_interval: (optional) the amount of time, in seconds, to wait before querying the AsyncResult
       object for the status of the jobs
      devices_assigned: (optional) set this to True if devices have already been assigned to
        the engines on this cluster

    """
    from ipyparallel import Client, RemoteError, Reference
    import inspect

    # setup logging
    log_path = os.path.expanduser(log_dir)
    log_name = log_name or 'job_runner'
    logger = setup_logging(log_name, log_path)

    # TODO: this isn't strictly necessary
    try:
        # check that job_fnc accepts a device kwarg
        args = inspect.getargspec(job_fnc)[0]
        assert 'device' in args
    except AssertionError:
        logger.critical("job_fnc does not except device kwarg. Halting.")

    client = Client(profile=ipp_profile)

    logger.info("Succesfully initialized client on %s with %s engines", ipp_profile, len(client))


    if not devices_assigned:
        # assign each engine to a GPU
        engines_per_host = {}
        device_assignments = []
        engine_hosts = client[:].apply(socket.gethostname).get()

        for host in engine_hosts:
            if host in engines_per_host:
                device_assignments.append('/gpu:{}'.format(engines_per_host[host]))
                engines_per_host[host] += 1
            else:
                device_assignments.append('/gpu:0')
                engines_per_host[host] = 1

        logger.info("Engines per host: \n")

        if not allow_engine_overlap:
            try:
                # check that we haven't over-provisioned GPUs
                for host, n_engines in six.iteritems(engines_per_host):
                    logger.info("%s: %s", host, n_engines)
                    assert n_engines <= WS_N_GPUS[host]
            except AssertionError:
                logger.critical("Host has more engines than GPUs. Halting.")


        while True:
            try:
                # NOTE: could also be accomplished with process environment variables
                # broadcast device assignments and job_fnc
                for engine_id, engine_device in enumerate(device_assignments):
                    print("Pushing to engine {}: device: {}".format(engine_id, engine_device))
                    client[engine_id].push({'device': engine_device,
                                            'job_fnc': job_fnc})

                for engine_id, (host, assigned_device) in enumerate(zip(engine_hosts, device_assignments)):
                    remote_device = client[engine_id].pull('device').get()
                    logger.info("Engine %s: host = %s; device = %s, remote device = %s",
                                engine_id, host, assigned_device, remote_device)
                break
            except RemoteError as remote_err:
                logger.warn("Caught remote error: %s. Sleeping for 10s before retry", remote_err)
                time.sleep(10)
    else:
        try:
            device_assignments = client[:].pull('device').get()
        except RemoteError as remote_err:
            logger.warn('Caught remote error when checking device assignments: %s. You may want to initialize device assignments', remote_err)

    logger.info("Dispatching jobs: %s", job_args)
    # dispatch jobs
    async_result = client[:].map(job_fnc, job_args, [Reference('device')] * len(job_args))

    start_time = time.time()

    while not async_result.ready():
        time.sleep(status_interval)
        n_finished = async_result.progress
        n_jobs = len(job_args)
        wall_time = start_time - time.time()
        logger.info("%s seconds elapsed. %s of %s jobs finished",
                    wall_time, n_finished, n_jobs)
    logger.info("All jobs finished in %s seconds!", async_result.wall_time)
