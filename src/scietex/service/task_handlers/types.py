"""Defines types used in task handlers."""

from typing import TypeVar
from enum import Enum


TaskType = TypeVar("TaskType", bound=Enum)
