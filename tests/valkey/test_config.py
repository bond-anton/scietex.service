"""Configuration tests for the Valkey worker (``ValkeyWorkerConfig``)."""

from pathlib import Path

import msgspec
import pytest

from scietex.service import ValkeyWorker
from scietex.service.valkey.config import (
    DEFAULT_CONTROL_STREAM_MAXLEN,
    DEFAULT_LOG_STREAM_MAXLEN,
    MAX_CONTROL_STREAM_MAXLEN,
    MAX_LOG_STREAM_MAXLEN,
    MAX_TASK_TRACKING_TTL,
    MIN_CONTROL_STREAM_MAXLEN,
    MIN_LOG_STREAM_MAXLEN,
    MIN_TASK_TRACKING_TTL,
    ValkeyBaseConfig,
    ValkeyConfig,
    ValkeyNode,
    ValkeyPubSubConfig,
    ValkeyUserCredentials,
    ValkeyWorkerConfig,
    _encode_valkey_config_value,
    generate_glide_config,
    read_valkey_config,
)


def test_read_valkey_config_creates_file(tmp_path: Path):
    conf_dir = tmp_path
    valkey_yml = conf_dir / "valkey.yml"
    # Ensure file does not exist initially
    if valkey_yml.exists():
        valkey_yml.unlink()

    cfg = read_valkey_config(conf_dir)
    assert isinstance(cfg, ValkeyConfig)
    assert valkey_yml.exists()


def test_read_valkey_config_invalid_file_raises_and_preserves(tmp_path: Path):
    conf_dir = tmp_path
    valkey_yml = conf_dir / "valkey.yml"
    malformed = b"not: [valid: yaml\n  base_config: broken"
    valkey_yml.write_bytes(malformed)

    with pytest.raises(RuntimeError):
        read_valkey_config(conf_dir)

    assert valkey_yml.read_bytes() == malformed


def test_read_valkey_config_missing_file_creates_defaults(tmp_path: Path):
    conf_dir = tmp_path
    valkey_yml = conf_dir / "valkey.yml"
    assert not valkey_yml.exists()

    cfg = read_valkey_config(conf_dir)

    assert isinstance(cfg, ValkeyConfig)
    assert valkey_yml.exists()


def test_read_valkey_config_no_create_default_does_not_write(tmp_path: Path):
    conf_dir = tmp_path
    valkey_yml = conf_dir / "valkey.yml"
    assert not valkey_yml.exists()

    with pytest.raises(RuntimeError):
        read_valkey_config(conf_dir, create_default=False)

    assert not valkey_yml.exists()
    assert not any(conf_dir.iterdir())


def test_read_valkey_config_no_create_default_reads_existing(tmp_path: Path):
    conf_dir = tmp_path
    valkey_yml = conf_dir / "valkey.yml"
    expected = ValkeyConfig()
    valkey_yml.write_bytes(msgspec.yaml.encode(expected))

    cfg = read_valkey_config(conf_dir, create_default=False)

    assert isinstance(cfg, ValkeyConfig)
    assert valkey_yml.read_bytes() == msgspec.yaml.encode(expected)


def test_read_valkey_config_no_create_default_missing_dir_raises(tmp_path: Path):
    conf_dir = tmp_path / "missing"
    assert not conf_dir.exists()

    with pytest.raises(RuntimeError):
        read_valkey_config(conf_dir, create_default=False)

    assert not conf_dir.exists()


def test_read_valkey_config_no_create_default_invalid_file_preserves(tmp_path: Path):
    conf_dir = tmp_path
    valkey_yml = conf_dir / "valkey.yml"
    malformed = b"not: [valid: yaml\n  base_config: broken"
    valkey_yml.write_bytes(malformed)

    with pytest.raises(RuntimeError):
        read_valkey_config(conf_dir, create_default=False)

    assert valkey_yml.read_bytes() == malformed


def test_generate_glide_config_defaults():
    cfg = ValkeyConfig()
    client_cfg = generate_glide_config(cfg, service_name="svc")
    # basic shape checks
    assert hasattr(client_cfg, "addresses")
    assert isinstance(client_cfg.addresses, list)
    assert len(client_cfg.addresses) == len(cfg.base_config.nodes)


def test_valkey_node_addresses_roundtrip():
    node = ValkeyNode(host="127.0.0.1", port=6380)
    cfg = ValkeyBaseConfig(nodes=[node])
    client_cfg = generate_glide_config(ValkeyConfig(base_config=cfg), service_name="svc")
    assert len(client_cfg.addresses) == 1


def test_credentials_property():
    credentials = ValkeyUserCredentials(username="u", password="p")
    cfg = ValkeyBaseConfig(user_credentials=credentials)
    server_credentials = cfg.credentials
    # server_credentials may be a glide ServerCredentials object; ensure not None
    assert server_credentials is not None


def test_generate_glide_config_pubsub_listening_true():
    """A listening ValkeyPubSubConfig wires the PubSub subscriptions into the client config."""
    received = []

    def parse_control_message(msg, context):
        received.append((msg, context))

    cfg = ValkeyConfig(pubsub_config=ValkeyPubSubConfig(listening=True, parse_control_message=parse_control_message))
    client_cfg = generate_glide_config(cfg, service_name="svc")

    ps = client_cfg.pubsub_subscriptions
    assert ps is not None
    # The exact-mode channel set holds the service channel and the broadcast channel.
    channels = ps.channels_and_patterns
    assert len(channels) == 1
    exact = channels[list(channels)[0]]
    assert exact == {"scietex:svc", "scietex:broadcast"}
    assert ps.callback is parse_control_message
    assert ps.context is None


def test_generate_glide_config_pubsub_listening_false_default():
    """The default (listening=False) leaves pubsub_subscriptions unset."""
    cfg = ValkeyConfig()
    client_cfg = generate_glide_config(cfg, service_name="svc")
    assert client_cfg.pubsub_subscriptions is None


def test_pubsub_callback_encodes_as_null_and_roundtrips(tmp_path: Path):
    """A populated parse_control_message encodes as null and round-trips via
    read_valkey_config, so a listening config never breaks YAML writes."""

    def parse_control_message(msg, context):
        pass

    cfg = ValkeyConfig(pubsub_config=ValkeyPubSubConfig(listening=True, parse_control_message=parse_control_message))

    encoded = msgspec.yaml.encode(cfg, enc_hook=_encode_valkey_config_value)
    assert b"parse_control_message: null" in encoded
    assert b"listening: true" in encoded

    # Default-write/decode round-trip: read_valkey_config writes defaults with
    # the hook, and a listening config serialized without a callback decodes back.
    conf_dir = tmp_path
    cfg = read_valkey_config(conf_dir)
    assert isinstance(cfg, ValkeyConfig)
    assert cfg.pubsub_config.listening is False
    assert cfg.pubsub_config.parse_control_message is None

    # A YAML file describing listening without a callback decodes cleanly.
    valkey_yml = conf_dir / "valkey.yml"
    valkey_yml.write_bytes(b"pubsub_config:\n  listening: true\n")
    loaded = read_valkey_config(conf_dir, create_default=False)
    assert loaded.pubsub_config.listening is True
    assert loaded.pubsub_config.parse_control_message is None


def test_invalid_read_from_raises():
    cfg = ValkeyBaseConfig()
    # inject invalid value
    cfg = ValkeyBaseConfig(read_from="INVALID")
    with pytest.raises(ValueError):
        generate_glide_config(ValkeyConfig(base_config=cfg), service_name="svc")


def test_invalid_protocol_raises():
    cfg = ValkeyBaseConfig(protocol="NOPE")
    with pytest.raises(ValueError):
        generate_glide_config(ValkeyConfig(base_config=cfg), service_name="svc")


def test_worker_config_log_stream_name_default_is_service_templated():
    """log_stream_name defaults to a {service}/{instance_id}-templated stream name."""
    assert ValkeyWorkerConfig().log_stream_name == "scietex:{service}:{instance_id}:log"


def test_worker_config_log_stream_maxlen_default():
    assert ValkeyWorkerConfig().log_stream_maxlen == DEFAULT_LOG_STREAM_MAXLEN


def test_worker_config_log_stream_maxlen_accepts_none():
    """None is the explicit unbounded-growth opt-out."""
    assert ValkeyWorkerConfig(log_stream_maxlen=None).log_stream_maxlen is None


def test_worker_config_log_stream_maxlen_rejects_zero():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(log_stream_maxlen=0)


def test_worker_config_log_stream_maxlen_rejects_above_max():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(log_stream_maxlen=MAX_LOG_STREAM_MAXLEN + 1)


def test_worker_config_log_stream_maxlen_accepts_boundaries():
    assert ValkeyWorkerConfig(log_stream_maxlen=MIN_LOG_STREAM_MAXLEN).log_stream_maxlen == MIN_LOG_STREAM_MAXLEN
    assert ValkeyWorkerConfig(log_stream_maxlen=MAX_LOG_STREAM_MAXLEN).log_stream_maxlen == MAX_LOG_STREAM_MAXLEN


def test_worker_config_task_tracking_ttl_default_is_none():
    """task_tracking_ttl is opt-in: the default config disables tracking TTL."""
    assert ValkeyWorkerConfig().task_tracking_ttl is None


def test_worker_config_task_tracking_ttl_accepts_explicit_value():
    cfg = ValkeyWorkerConfig(task_tracking_ttl=3600)
    assert cfg.task_tracking_ttl == 3600


def test_worker_config_task_tracking_ttl_rejects_zero():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(task_tracking_ttl=0)


def test_worker_config_task_tracking_ttl_rejects_above_max():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(task_tracking_ttl=MAX_TASK_TRACKING_TTL + 1)


def test_worker_config_task_tracking_ttl_accepts_boundaries():
    assert ValkeyWorkerConfig(task_tracking_ttl=MIN_TASK_TRACKING_TTL).task_tracking_ttl == MIN_TASK_TRACKING_TTL
    assert ValkeyWorkerConfig(task_tracking_ttl=MAX_TASK_TRACKING_TTL).task_tracking_ttl == MAX_TASK_TRACKING_TTL


def test_worker_config_control_stream_name_default_is_templated():
    """control_stream_name defaults to a {service}/{instance_id}-templated name."""
    assert ValkeyWorkerConfig().control_stream_name == "scietex:{service}:control:{instance_id}"


def test_worker_config_control_broadcast_stream_name_default_is_templated():
    """control_broadcast_stream_name defaults to a {service}-templated name."""
    assert ValkeyWorkerConfig().control_broadcast_stream_name == "scietex:{service}:control"


def test_worker_config_control_stream_maxlen_default():
    assert ValkeyWorkerConfig().control_stream_maxlen == DEFAULT_CONTROL_STREAM_MAXLEN


def test_worker_config_control_stream_maxlen_rejects_zero():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(control_stream_maxlen=0)


def test_worker_config_control_stream_maxlen_rejects_above_max():
    with pytest.raises(msgspec.ValidationError):
        ValkeyWorkerConfig(control_stream_maxlen=MAX_CONTROL_STREAM_MAXLEN + 1)


def test_worker_config_control_stream_maxlen_accepts_boundaries():
    assert (
        ValkeyWorkerConfig(control_stream_maxlen=MIN_CONTROL_STREAM_MAXLEN).control_stream_maxlen
        == MIN_CONTROL_STREAM_MAXLEN
    )
    assert (
        ValkeyWorkerConfig(control_stream_maxlen=MAX_CONTROL_STREAM_MAXLEN).control_stream_maxlen
        == MAX_CONTROL_STREAM_MAXLEN
    )


def test_control_stream_names_resolved_at_construction():
    """Both template placeholders resolve: the directed stream embeds the
    instance id, the broadcast stream is service-scoped (AR-123)."""
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    assert worker._control_stream_name == f"scietex:svc:control:{worker.instance_id}"
    assert worker._control_broadcast_stream_name == "scietex:svc:control"


def test_log_stream_name_resolved_at_construction():
    """log_stream_name embeds the instance id so each worker logs to its own stream."""
    worker = ValkeyWorker(ValkeyWorkerConfig(service_name="svc", valkey_config=ValkeyConfig()))
    assert worker._log_stream_name == f"scietex:svc:{worker.instance_id}:log"


def test_worker_config_heartbeat_key_default_is_templated():
    """heartbeat_key defaults to a {service}/{instance_id}-templated key."""
    assert ValkeyWorkerConfig().heartbeat_key == "scietex:{service}:{instance_id}:status"


def test_worker_config_task_stream_name_default_is_templated():
    """task_stream_name defaults to a {service}-templated stream name."""
    assert ValkeyWorkerConfig().task_stream_name == "scietex:{service}:tasks"


def test_worker_config_task_group_name_default_is_templated():
    """task_group_name defaults to a {service}-templated group name."""
    assert ValkeyWorkerConfig().task_group_name == "scietex:{service}:task_group"


def test_worker_config_consumer_name_default_is_templated():
    """consumer_name defaults to a {service}/{instance_id}-templated name."""
    assert ValkeyWorkerConfig().consumer_name == "scietex:{service}:{instance_id}"
