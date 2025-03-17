"""Helper functions for the `nts.service` module."""

from time import time_ns


def time_ms():
    """
    Return time in milliseconds since the epoch.
    :return: time in ms.
    """
    return int(time_ns() // 1e6)
