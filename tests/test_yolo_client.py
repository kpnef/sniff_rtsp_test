from __future__ import annotations

import json

import cv2
import numpy as np

from video_shm_core.ffmpeg_decoder import decode_video_file_to_shm
from video_yolo_client.detector import DetectorBase
from video_yolo_client.service import RoundRobinYoloService


class DummyDetector(DetectorBase):
    def __init__(self, input_size: int = 320) -> None:
        self.input_size = input_size
        self.calls = []

    def detect(self, image_bgr):
        self.calls.append(image_bgr.shape)
        mean_value = float(image_bgr.mean())
        return [{'cls': 0, 'conf': 0.9, 'mean': mean_value}]


def test_single_channel_video_pipeline_with_detection(shm_ring, tmp_video_path, tmp_path):
    channel_id = shm_ring.ensure_channel('192.168.0.10')
    decode_video_file_to_shm(tmp_video_path, shm_ring, channel_id, '192.168.0.10')
    detector = DummyDetector(input_size=320)
    service = RoundRobinYoloService(shm_ring, detector, log_dir=tmp_path)

    result = service.run_once()
    assert result is not None
    assert result['channel_id'] == channel_id
    assert result['pts'] >= 0
    assert detector.calls
    logged = json.loads((tmp_path / f"{result['pts']}ms.log").read_text(encoding='utf-8'))
    assert logged['channel_id'] == channel_id
    assert logged['pts'] == result['pts']


def test_multi_channel_round_robin_detection(shm_ring, tmp_path, bgr_to_nv12_bytes):
    detector = DummyDetector(input_size=224)
    service = RoundRobinYoloService(shm_ring, detector, log_dir=tmp_path)

    first = np.zeros((240, 320, 3), dtype=np.uint8)
    cv2.rectangle(first, (40, 40), (120, 120), (255, 255, 255), -1)
    second = np.zeros((240, 320, 3), dtype=np.uint8)
    cv2.circle(second, (160, 120), 50, (255, 255, 255), -1)

    ch0 = shm_ring.ensure_channel('192.168.1.10')
    ch1 = shm_ring.ensure_channel('192.168.1.11')
    shm_ring.write_frame(ch0, 1000, bgr_to_nv12_bytes(first), width=320, height=240, pix_fmt='nv12', source_id='192.168.1.10')
    shm_ring.write_frame(ch1, 2000, bgr_to_nv12_bytes(second), width=320, height=240, pix_fmt='nv12', source_id='192.168.1.11')

    result1 = service.run_once()
    result2 = service.run_once()
    assert service.get_video_channel_count() == 2
    assert {result1['channel_id'], result2['channel_id']} == {ch0, ch1}
    assert result1['channel_id'] != result2['channel_id']
