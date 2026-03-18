from __future__ import annotations

from video_shm_core.ffmpeg_decoder import decode_video_file_to_shm
from video_shm_core.shared_ring import STATUS_DECODING


def test_default_config_supports_1080p_requirement():
    from video_shm_core.config import SharedVideoConfig

    config = SharedVideoConfig()
    assert config.max_channels >= 4
    assert config.blocks_per_channel >= 15
    assert config.frame_size >= 1920 * 1080 * 3 // 2


def test_decode_video_and_shared_memory_readback(shm_ring, tmp_video_path):
    channel_id = shm_ring.ensure_channel('127.0.0.1')
    stats = decode_video_file_to_shm(tmp_video_path, shm_ring, channel_id, '127.0.0.1')
    assert stats.frames_written > 0
    frame = shm_ring.read_latest_frame(channel_id)
    assert frame is not None
    assert frame.channel_id == channel_id
    assert frame.data_len == len(frame.data)
    assert frame.pix_fmt == 'nv12'


def test_latest_reader_skips_current_decoding_block(shm_ring):
    channel_id = shm_ring.ensure_channel('10.0.0.1')
    full_frame = bytes([12]) * shm_ring.config.frame_size
    older = shm_ring.write_frame(channel_id, 100, full_frame, source_id='10.0.0.1')
    handle = shm_ring.begin_frame_write(channel_id, 200, source_id='10.0.0.1')
    try:
        handle.write(bytes([99]) * shm_ring.config.frame_size)
        latest = shm_ring.read_latest_frame(channel_id)
        assert latest is not None
        assert latest.pts == older.pts
        assert latest.write_seq == older.write_seq
        assert STATUS_DECODING == 1
    finally:
        handle.cancel()


def test_latest_reader_returns_top_completed_frame(shm_ring):
    channel_id = shm_ring.ensure_channel('10.0.0.2')
    shm_ring.write_frame(channel_id, 100, bytes([1]) * shm_ring.config.frame_size, source_id='10.0.0.2')
    shm_ring.write_frame(channel_id, 200, bytes([2]) * shm_ring.config.frame_size, source_id='10.0.0.2')
    latest = shm_ring.write_frame(channel_id, 300, bytes([3]) * shm_ring.config.frame_size, source_id='10.0.0.2')
    frame = shm_ring.read_latest_frame(channel_id)
    assert frame is not None
    assert frame.pts == 300
    assert frame.write_seq == latest.write_seq
    assert frame.data[0] == 3
