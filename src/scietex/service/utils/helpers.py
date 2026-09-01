"""General helper functions for ``scietex.service``.

Provides ``get_anything_name()`` which extracts a human-readable name
from any Python object via ``__name__``, ``__class__.__name__``, or
``str()`` fallback.
"""

from typing import Any


def get_anything_name(anything: Any) -> str:
    """Get the name of any Python object."""
    if hasattr(anything, "__name__"):
        anything_name = str(anything.__name__)
    elif hasattr(anything, "__class__"):
        anything_name = anything.__class__.__name__
    else:
        anything_name = str(anything)
    return anything_name
