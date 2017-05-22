""" This module contains logging related methods
"""

import logging
import os

def setup_logging(log_name, log_path):
    """ Sets up module level logging
    """

    # define module level logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)

    log_path = os.path.expanduser(log_path)

    if not os.exists(log_path):
        os.makedirs(log_path, exist_ok=True)

    # define file handler for module
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)

    # create formatter and add to handler
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    # add handler to logger
    logger.addHandler(fh)

    # TODO: implement email handler
    # define email handler for important logs in module
    #eh = logging.SMTPHandler()
    return logger
