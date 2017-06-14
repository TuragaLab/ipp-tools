""" This module contains misc. utils
"""

import os
import sys
import shutil
from ipyparallel import Client

from glob import glob

def package_path():
    """ Returns the absolute path to this package base directory

    :returns: absolute path to ipp-tools/
    :rtype: string

    """
    mjhmc_path = [path for path in sys.path if 'ipp-tools' in path][0]
    if mjhmc_path is None:
        raise Exception('You must include ipp-tools in your PYTHON_PATH')
    prefix = mjhmc_path.split('ipp-tools')[0]
    return "{}ipp-tools".format(prefix)


def find_free_profile(profile):
    """ Finds a free version of profile

    Args:
      profile: base name of profile (no version)

    Returns:
      full_profile: the full name of a free profile
    """
    if profile_installed(profile):
        print("Found existing profiles for {}. Checking for active clusters".format(profile))
        # check if any are free. if not make a new one
        n_versions = num_profile_versions(profile)
        for version in range(n_versions):
            version_name = '{}_{}'.format(profile, version)
            if not profile_running(version_name):
                print("Found free profile: {}".format(version_name))
                return version_name

        print("All {} existing versions of {} in use. Creating version {}".format(n_versions, profile, n_versions + 1))
        return install_profile(profile, n_versions + 1)
    else:
        print("Found no existing profiles for {}. Creating version 0".format(profile))
        return install_profile(profile, 0)
         # make version 0


def profile_running(profile):
    """ Checks if there is an ipyparallel cluster running with profile

    Args:
      profile: name of profile

    Returns:
      is_running: true if found
    """
    # TODO: more sophisticated checks
    # maybe we can just check to see if the connection files exist?
    try:
        client = Client(profile=profile)
        return True
    except OSError as os_err:
        print("Caught OSError while attempting to connect to {}: {}.".format(profile, os_err))
        return False
    except TimeoutError as timeout_err:
        print("Caught TimeoutError while attempting to connect to {}: {}".format(profile, timeout_err))
        return False


def profile_installed(profile):
    """ Checks if there is an entry for profile in ~/.ipython/

    Args:
      profile: base name of profile, no version numbers

    Returns:
      installed: True if a profile exists

    """
    return num_profile_versions(profile) >= 1

def num_profile_versions(profile):
    """ Returns the number of versions of a profile that exist

    Args:
      profile: base name of profile, no version numbers

    Returns:
      n_profiles: the number of versions of that profile that exist

    """
    profile_glob = os.path.expanduser('~/.ipython/{}_*'.format(profile))
    matching_profiles = glob(profile_glob)
    return len(matching_profiles)


def install_profile(template_profile, version_num=0):
    """ Copy a templated profile into ~/.ipython/

    Returns:
      full_profile: the full name of the installed profile
    """
    # copy files from ipp-tools/profile/{template} to ~/.ipython
    template_path = '{}/{}'.format(package_path(), template_profile)
    dst_path = os.path.expanduser('~/.ipython/{}_{}').format(template_profile, version_num)
    assert os.path.exists(template_path)

    shutil.copytree(template_path, dst_path)
