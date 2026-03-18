from __future__ import annotations

from sniff import create_shm_bridge, parse_runtime_config


def test_sniff_runtime_defaults_to_statistics_only():
    config = parse_runtime_config([])
    assert config.write_shm is False
    assert create_shm_bridge(config) is None


def test_sniff_runtime_can_enable_shared_memory_from_parameter():
    config = parse_runtime_config([
        '--write-shm',
        '--shm-base-name', 'case_sniff',
        '--shm-max-channels', '4',
        '--shm-blocks-per-channel', '15',
        '--shm-frame-width', '64',
        '--shm-frame-height', '64',
    ])
    bridge = create_shm_bridge(config)
    assert config.write_shm is True
    assert bridge is not None
    assert bridge.config.base_name == 'case_sniff'
    try:
        assert bridge.config.max_channels == 4
    finally:
        bridge.close()


def test_sniff_runtime_stats_only_overrides_write_shm_parameter():
    config = parse_runtime_config(['--write-shm', '--stats-only'])
    assert config.write_shm is False
