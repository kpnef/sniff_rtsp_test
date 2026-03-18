from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import cv2
import numpy as np
import pytest

from video_shm_core.config import SharedVideoConfig
from video_shm_core.shared_ring import SharedVideoRingBuffer


@pytest.fixture()
def tmp_video_path(tmp_path: Path) -> Path:
    out = tmp_path / 'sample_1s_h264.mp4'
    cmd = [
        'ffmpeg', '-y',
        '-f', 'lavfi', '-i', 'testsrc=size=160x120:rate=10:duration=1',
        '-pix_fmt', 'yuv420p',
        '-c:v', 'libx264',
        '-t', '1',
        str(out),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return out


@pytest.fixture()
def shm_config(tmp_path: Path) -> SharedVideoConfig:
    return SharedVideoConfig(
        base_name=f'test_sniff_{tmp_path.name}',
        max_channels=4,
        blocks_per_channel=15,
        frame_width=320,
        frame_height=240,
        pix_fmt='nv12',
    )


@pytest.fixture()
def shm_ring(shm_config: SharedVideoConfig):
    ring = SharedVideoRingBuffer.create(shm_config, reset_existing=True)
    try:
        yield ring
    finally:
        ring.close(unlink=True)


def bgr_to_nv12_bytes(image_bgr: np.ndarray) -> bytes:
    height, width = image_bgr.shape[:2]
    yuv_i420 = cv2.cvtColor(image_bgr, cv2.COLOR_BGR2YUV_I420)
    flat = yuv_i420.reshape(-1)
    y_size = width * height
    uv_size = y_size // 4
    y = flat[:y_size]
    u = flat[y_size:y_size + uv_size]
    v = flat[y_size + uv_size:y_size + 2 * uv_size]
    uv = np.empty((uv_size * 2,), dtype=np.uint8)
    uv[0::2] = u
    uv[1::2] = v
    return bytes(np.concatenate([y, uv]))


@pytest.fixture(name='bgr_to_nv12_bytes')
def _bgr_to_nv12_bytes_fixture():
    return bgr_to_nv12_bytes
