#pylint: disable=invalid-name
#pylint: disable=logging-format-interpolation
# -*- coding: utf-8 -*-
"""
.. :module:: logger
   :platform: Linux
   :synopsis: Module for logging

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 15, 2022)
"""

import logging

logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig( \
    format="%(asctime)s %(levelname)s %(process)d: %(filename)s:%(lineno)d %(message)s" \
    )

def do_info(string):
    """Used for logs with log level INFO"""
    logger.log(logging.INFO, "{0}".format(string))

def do_debug(string):
    """Used for logs with log level DEBUG"""
    logger.log(logging.DEBUG, "{0} {1}".format(string, logger.findCaller()))

def do_warn(string):
    """Used for logs with log level WARN"""
    logger.log(logging.WARN, "{0} {1}".format(string, logger.findCaller()))

def do_error(string):
    """Used for logs with log level ERROR"""
    logger.log(logging.ERROR, "{0} {1}".format(string, logger.findCaller()))
