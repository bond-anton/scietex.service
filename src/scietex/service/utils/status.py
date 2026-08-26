"""State management utilities."""

from enum import Enum


class ServiceStatus(Enum):
    STOPPED = "Stopped"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"


class ManagerStatus(Enum):
    STOPPED = "Stopped"
    RUNNING = "Running"
    STOPPING = "Stopping"
