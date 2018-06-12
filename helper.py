# -*- coding: utf-8 -*-
"""
Helper methods for TeamCity build
"""

import logging
import os
from contextlib import contextmanager
try:
    from urllib.request import urlopen, Request
except ImportError:
    from urllib2 import Request, urlopen

ARTIFACT_PUBLISHING_PATH = "output"


# TeamCity
# Based on https://confluence.jetbrains.com/display/TCD9/Build+Script+Interaction+with+TeamCity

_quote = {"'": "|'", "|": "||", "\n": "|n", "\r": "|r", '[': '|[', ']': '|]'}


def escape_value(value):
    return "".join(_quote.get(x, x) for x in value)


def tc_log_sys_msg(msg, tc_logger=None):
    """Log system message to Teamcity.

    Args:
        msg: system message to log.
        tc_logger: Logger instance or logger name.
    """
    sys_msg = "##teamcity[{0}]".format(msg)
    if tc_logger is not None:
        _logger = tc_logger if isinstance(tc_logger, logging.Logger) else logging.getLogger(tc_logger)
        _logger.info(msg=sys_msg)
    else:
        print(sys_msg)


def tc_block_open(block_name, tc_logger=None):
    """Blocks are used to group several messages in the build log.

    Args:
        block_name: name of block.
        tc_logger: teamcity logger.
    """
    tc_log_sys_msg("blockOpened name='{}'".format(escape_value(block_name)), tc_logger)


def tc_block_close(block_name, tc_logger=None):
    """Blocks are used to group several messages in the build log.

    Args:
        block_name: name of block.
        tc_logger: teamcity logger.
    """
    tc_log_sys_msg("blockClosed name='{}'".format(escape_value(block_name)), tc_logger)


def current_dir():
    """Get the current directory, from which we started. It must be 'run'.
    Returns:
        Current dir.
    """
    return os.path.realpath(os.getcwd())


def create_output_dir(output_subdir):
    """Create output directory.

    Args:
        output_subdir: dir to create in outputs.
    Returns:
        Full path to output dir or passed subdir.
    """
    out_dir = get_output_dir(output_subdir)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    return out_dir


def get_output_dir(subdir=None, absolute=True):
    """Get output directory.

    Args:
        subdir: subdirectory of output dir.
        absolute: if True - absolute path, otherwise - relative.
    Returns:
        Full path to output dir or passed subdir.
    """
    dirs = []
    if absolute:
        dirs.append(current_dir())
    dirs.append(ARTIFACT_PUBLISHING_PATH)
    if subdir is not None:
        dirs.append(subdir)
    return os.path.join(*dirs)


@contextmanager
def tc_log(tc_block_name, tc_logger=None):
    """Context manager for logging messages to TeamCity

    Args:
        tc_block_name: TeamCity build log block name.
        tc_logger: teamcity logger.
    Yields: generator.
    """
    tc_block_open(tc_block_name, tc_logger=tc_logger)
    yield
    tc_block_close(tc_block_name, tc_logger=tc_logger)
