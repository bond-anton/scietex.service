"""This module provides classes for implementation of simple daemons working in a background."""

__version__ = "0.0.2"

from enum import Enum

from .basic_sync_worker import BasicSyncWorker
from .redis_sync_worker import RedisSyncWorker
