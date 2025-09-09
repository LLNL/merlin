##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module handles setting up the extensive logging system in Merlin."""

import logging
import sys

import coloredlogs


FORMATS = {
    "DEFAULT": "[%(asctime)s: %(levelname)s] %(message)s",
    "DEBUG": "[%(asctime)s: %(levelname)s] [%(module)s: %(lineno)d] %(message)s",
    "WORKER": "[%(asctime)s: %(levelname)s] [%(task_name)s(%(task_id)s)] %(message)s",
}


def setup_logging(logger: logging.Logger, log_level: str = "INFO", colors: bool = True):
    """
    Setup and configure Python logging.

    Args:
        logger: A logging.Logger object.
        log_level: Logger level.
        colors: If True use colored logs.
    """
    formatter = logging.Formatter()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.setLevel(log_level)
    logger.propagate = False

    if colors is True:
        coloredlogs.install(level=log_level, logger=logger, fmt=FORMATS["DEFAULT"])
