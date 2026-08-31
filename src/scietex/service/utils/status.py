"""State management utilities."""

from enum import Enum


class ServiceStatus(Enum):
    """Service Status."""

    STOPPED = "Stopped"
    STARTING = "Starting"
    RUNNING = "Running"
    STOPPING = "Stopping"


class LoggerStatus(Enum):
    """Async Logger Status."""

    STOPPED = "Stopped"
    RUNNING = "Running"


class ManagerStatus(Enum):
    """Manager Status."""

    STOPPED = "Stopped"
    RUNNING = "Running"
    STOPPING = "Stopping"
