"""Generic RFC 7396 layered configuration merge over plain dicts."""

from collections.abc import Mapping, Sequence
from typing import Any, TypeVar

import msgspec

T = TypeVar("T", bound=msgspec.Struct)


def merge(base: Mapping[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    """RFC 7396 merge of ``patch`` onto ``base``. Returns a new dict.

    - key present with ``None`` -> pop the key (clear)
    - key present with a dict AND ``base[key]`` is a dict -> recurse
    - key present with any other value -> set
    - key absent -> leave the base value untouched
    """
    out = dict(base)
    for key, value in patch.items():
        if value is None:
            out.pop(key, None)
        elif isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = merge(out[key], value)
        else:
            out[key] = value
    return out


def merge_all(base: Mapping[str, Any], patches: Sequence[Mapping[str, Any] | None]) -> dict[str, Any]:
    """Fold ``patches`` low->high onto ``base``, skipping ``None``/empty patches.

    Returns a new dict.
    """
    merged = dict(base)
    for patch in patches:
        if patch:
            merged = merge(merged, patch)
    return merged


def resolve_section(
    struct_type: type[T],
    layers: Sequence[Mapping[str, Any] | None],
    *,
    defaults: msgspec.Struct | Mapping[str, Any] | None = None,
) -> T:
    """Resolve a section's effective struct from its layer patches.

    ``layers`` is ordered HIGH->LOW (e.g. ``[L3, L2, L1]``). The L0 base is
    ``msgspec.to_builtins(defaults)`` when ``defaults`` is given, else
    ``msgspec.to_builtins(struct_type())``. Layers are applied low->high, and
    the merged dict is converted back through ``struct_type``, which validates
    and coerces every level and rejects unknown fields.
    """
    base = _l0_base(defaults, struct_type)
    merged = merge_all(base, layers[::-1])
    return msgspec.convert(merged, struct_type)


def _l0_base(
    defaults: msgspec.Struct | Mapping[str, Any] | None,
    struct_type: type[T],
) -> dict[str, Any]:
    """Build the L0 base dict from ``defaults`` or the struct's own defaults."""
    if defaults is None:
        return msgspec.to_builtins(struct_type())
    return msgspec.to_builtins(defaults)


__all__ = ["merge", "merge_all", "resolve_section"]
