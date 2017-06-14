""" This module contains misc. utils
"""

def package_path():
    """ Returns the absolute path to this package base directory

    :returns: absolute path to ipp-tools/
    :rtype: string

    """
    import sys
    mjhmc_path = [path for path in sys.path if 'ipp-tools' in path][0]
    if mjhmc_path is None:
        raise Exception('You must include ipp-tools in your PYTHON_PATH')
    prefix = mjhmc_path.split('ipp-tools')[0]
    return "{}ipp-tools".format(prefix)
