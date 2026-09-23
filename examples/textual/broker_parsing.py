"""Pure parsers for broker wire formats.

Kept free of optional-extra imports so the parsing logic is unit-testable
without ``valkey-glide`` or ``aiomqtt`` installed.
"""

#: Maps a mosquitto ``$SYS`` topic to the snapshot field it feeds and the
#: converter applied to its payload. Topics absent from this map are ignored.
SYS_TOPIC_FIELDS: dict[str, tuple[str, type]] = {
    "$SYS/broker/version": ("version", str),
    "$SYS/broker/uptime": ("uptime_s", int),
    "$SYS/broker/clients/connected": ("clients_connected", int),
    "$SYS/broker/clients/total": ("clients_total", int),
    "$SYS/broker/messages/received": ("messages_received", int),
    "$SYS/broker/messages/sent": ("messages_sent", int),
    "$SYS/broker/bytes/received": ("bytes_received", int),
    "$SYS/broker/bytes/sent": ("bytes_sent", int),
    "$SYS/broker/subscriptions/count": ("subscriptions", int),
    "$SYS/broker/retained messages/count": ("retained_messages", int),
    "$SYS/broker/store/messages/count": ("store_messages", int),
    "$SYS/broker/load/messages/received/1min": ("load_messages_received_1min", float),
    "$SYS/broker/heap/current": ("heap_current", int),
}


def parse_info(raw: bytes) -> tuple[dict[str, str], dict[str, dict[str, int]]]:
    """Split a Valkey ``INFO`` reply into scalar fields and keyspace counts.

    Returns ``(fields, keyspace)``. ``fields`` holds every ``key:value`` line
    outside the Keyspace section verbatim; ``keyspace`` maps a database name to
    its parsed counters. Section tracking is required because a keyspace line
    (``db0:keys=5,...``) is shaped like a ``cmdstat_*`` line, so the section
    header is the only thing that distinguishes them.
    """
    fields: dict[str, str] = {}
    keyspace: dict[str, dict[str, int]] = {}
    section = ""

    for line in raw.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            section = line[1:].strip().lower()
            continue
        key, separator, value = line.partition(":")
        if not separator:
            continue
        if section == "keyspace":
            counters: dict[str, int] = {}
            for pair in value.split(","):
                name, _, count = pair.partition("=")
                try:
                    counters[name.strip()] = int(count)
                except ValueError:
                    continue
            keyspace[key.strip()] = counters
        else:
            fields[key.strip()] = value.strip()

    return fields, keyspace


def parse_sys_message(topic: str, payload: bytes) -> tuple[str, str | int | float] | None:
    """Convert one ``$SYS`` message into ``(field_name, value)``.

    Returns ``None`` for an unknown topic or an unparsable payload, so the
    caller keeps the previous good value instead of overwriting it with junk.
    """
    mapping = SYS_TOPIC_FIELDS.get(topic)
    if mapping is None:
        return None
    field, converter = mapping
    text = payload.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    # Uptime arrives as "12345 seconds"; the leading token is the value.
    token = text.split()[0] if converter is int else text
    try:
        return field, converter(token)
    except ValueError:
        return None
