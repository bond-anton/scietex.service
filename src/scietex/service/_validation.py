"""Shared range validation for the configuration structs.

Kept in its own module so both the core :mod:`scietex.service.config` and the
optional :mod:`scietex.service.valkey.config` can import it without the valkey
package reaching into a private name of the core config module (AR-079).
"""

import msgspec


def validate_range(
    value: float | int | None,
    name: str,
    *,
    minimum: float | int,
    maximum: float | int | None = None,
) -> None:
    """Raise ``msgspec.ValidationError`` if ``value`` is outside the bounds.

    ``None`` is always allowed (it means "use the default"). ``maximum`` may be
    ``None`` to enforce only a lower bound.

    Args:
        value: The value to validate.
        name: Field name used in the error message.
        minimum: Inclusive lower bound.
        maximum: Inclusive upper bound, or ``None`` for no upper bound.

    Raises:
        msgspec.ValidationError: If ``value`` is below ``minimum`` or above
            ``maximum``.
    """
    if value is None:
        return
    if value < minimum:
        raise msgspec.ValidationError(f"{name} must be >= {minimum}, got {value!r}")
    if maximum is not None and value > maximum:
        raise msgspec.ValidationError(f"{name} must be <= {maximum}, got {value!r}")
