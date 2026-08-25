"""State management utilities."""

from enum import Enum


class ServiceState(Enum):
    STOPPED = "Stopped"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"


class ServiceEvent(Enum):
    START = "Start"
    STOP = "Stop"
    STARTUP_SUCCESS = "Startup SUCCESS"
    STARTUP_FAILURE = "Startup FAILURE"
    STOPPING_COMPLETE = "Stopping COMPLETE"
