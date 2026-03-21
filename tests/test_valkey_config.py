from pathlib import Path

import pytest

from scietex.service.valkey.valkey_config import (
    ValkeyBaseConfig,
    ValkeyNode,
    ValkeyUserCredentials,
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
    assert isinstance(cfg, ValkeyBaseConfig)
    assert valkey_yml.exists()


def test_generate_glide_config_defaults():
    cfg = ValkeyBaseConfig()
    client_cfg = generate_glide_config(cfg, service_name="svc", worker_id=1)
    # basic shape checks
    assert hasattr(client_cfg, "addresses")
    assert isinstance(client_cfg.addresses, list)
    assert len(client_cfg.addresses) == len(cfg.nodes)


def test_valkeynode_addresses_roundtrip():
    node = ValkeyNode(host="127.0.0.1", port=6380)
    cfg = ValkeyBaseConfig(nodes=[node])
    client_cfg = generate_glide_config(cfg, service_name="svc", worker_id=1)
    assert len(client_cfg.addresses) == 1


def test_credentials_property():
    creds = ValkeyUserCredentials(username="u", password="p")
    cfg = ValkeyBaseConfig(user_credentials=creds)
    server_creds = cfg.credentials
    # server_creds may be a glide ServerCredentials object; ensure not None
    assert server_creds is not None


def test_invalid_read_from_raises():
    cfg = ValkeyBaseConfig()
    # inject invalid value
    cfg = ValkeyBaseConfig(read_from="INVALID")
    with pytest.raises(ValueError):
        generate_glide_config(cfg, service_name="svc", worker_id=1)


def test_invalid_protocol_raises():
    cfg = ValkeyBaseConfig(protocol="NOPE")
    with pytest.raises(ValueError):
        generate_glide_config(cfg, service_name="svc", worker_id=1)
