"""Unit tests for the pure broker wire-format parsers."""

import pytest

from examples.textual.broker_parsing import parse_info, parse_sys_message

INFO_SAMPLE = (
    "# Server\r\n"
    "redis_version:7.2.0\r\n"
    "valkey_version:8.0.1\r\n"
    "uptime_in_seconds:3600\r\n"
    "\r\n"
    "# Clients\r\n"
    "connected_clients:3\r\n"
    "# Memory\r\n"
    "used_memory:1048576\r\n"
    "used_memory_peak:2097152\r\n"
    "# Stats\r\n"
    "instantaneous_ops_per_sec:42\r\n"
    "# CPU\r\n"
    "used_cpu_sys:1.5\r\n"
    "used_cpu_user:2.25\r\n"
    "# Commandstats\r\n"
    "cmdstat_get:calls=1,usec=2\r\n"
    "# Keyspace\r\n"
    "db0:keys=5,expires=1,avg_ttl=0\r\n"
    "db1:keys=2,expires=0,avg_ttl=0\r\n"
)


def test_parse_info_extracts_scalar_fields():
    fields, _ = parse_info(INFO_SAMPLE.encode())
    assert fields["redis_version"] == "7.2.0"
    assert fields["valkey_version"] == "8.0.1"
    assert fields["uptime_in_seconds"] == "3600"
    assert fields["connected_clients"] == "3"
    assert fields["used_memory"] == "1048576"
    assert fields["instantaneous_ops_per_sec"] == "42"
    assert fields["used_cpu_sys"] == "1.5"


def test_parse_info_extracts_keyspace_counters():
    _, keyspace = parse_info(INFO_SAMPLE.encode())
    assert keyspace == {
        "db0": {"keys": 5, "expires": 1, "avg_ttl": 0},
        "db1": {"keys": 2, "expires": 0, "avg_ttl": 0},
    }


def test_parse_info_keeps_commandstats_out_of_keyspace():
    """A cmdstat line is shaped like a keyspace line; only the section separates them."""
    fields, keyspace = parse_info(INFO_SAMPLE.encode())
    assert fields["cmdstat_get"] == "calls=1,usec=2"
    assert "cmdstat_get" not in keyspace


def test_parse_info_tolerates_empty_and_malformed_input():
    assert parse_info(b"") == ({}, {})
    assert parse_info(b"# Server\r\nno_colon_here\r\n") == ({}, {})
    assert parse_info(b"# Server\r\nredis_version:7.2.0") == ({"redis_version": "7.2.0"}, {})


def test_parse_info_skips_unparsable_keyspace_counters():
    _, keyspace = parse_info(b"# Keyspace\r\ndb0:keys=5,bogus=abc\r\n")
    assert keyspace == {"db0": {"keys": 5}}


@pytest.mark.parametrize(
    ("topic", "payload", "expected"),
    [
        ("$SYS/broker/clients/connected", b"3", ("clients_connected", 3)),
        ("$SYS/broker/uptime", b"12345 seconds", ("uptime_s", 12345)),
        ("$SYS/broker/version", b"mosquitto version 2.0.18", ("version", "mosquitto version 2.0.18")),
        ("$SYS/broker/load/messages/received/1min", b"12.5", ("load_messages_received_1min", 12.5)),
        ("$SYS/broker/heap/current", b"4096", ("heap_current", 4096)),
        ("$SYS/broker/retained messages/count", b"7", ("retained_messages", 7)),
    ],
)
def test_parse_sys_message_maps_known_topics(topic, payload, expected):
    assert parse_sys_message(topic, payload) == expected


def test_parse_sys_message_ignores_unknown_topic():
    assert parse_sys_message("$SYS/broker/unknown/thing", b"1") is None


@pytest.mark.parametrize("payload", [b"", b"   ", b"not-a-number"])
def test_parse_sys_message_rejects_unparsable_payload(payload):
    assert parse_sys_message("$SYS/broker/clients/connected", payload) is None
