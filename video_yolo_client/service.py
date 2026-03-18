from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import cv2
import numpy as np

from video_shm_core.shared_ring import FrameRecord, SharedVideoRingBuffer
from .detector import DetectorBase


def yuv420_to_bgr(frame: FrameRecord) -> np.ndarray:
    height, width = frame.height, frame.width
    payload = np.frombuffer(frame.data, dtype=np.uint8)
    if frame.pix_fmt == 'nv12':
        yuv = payload.reshape((height * 3 // 2, width))
        return cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_NV12)
    if frame.pix_fmt == 'yuv420p':
        yuv = payload.reshape((height * 3 // 2, width))
        return cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_I420)
    raise ValueError(f'unsupported pix_fmt: {frame.pix_fmt}')


class RoundRobinYoloService:
    def __init__(
        self,
        ring: SharedVideoRingBuffer,
        detector: DetectorBase,
        *,
        log_dir: str | Path = '.',
    ) -> None:
        self.ring = ring
        self.detector = detector
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._cursor = 0

    def get_video_channel_count(self) -> int:
        return self.ring.get_active_channel_count()

    def _next_channel_id(self) -> Optional[int]:
        channels = self.ring.list_channels()
        if not channels:
            return None
        channel = channels[self._cursor % len(channels)]
        self._cursor = (self._cursor + 1) % max(len(channels), 1)
        return channel.channel_id

    def _prepare_image(self, frame: FrameRecord):
        image = yuv420_to_bgr(frame)
        input_size = getattr(self.detector, 'input_size', 640)
        return cv2.resize(image, (input_size, input_size), interpolation=cv2.INTER_LINEAR)

    def _write_log(self, *, pts: int, channel_id: int, detections: list[dict]) -> Path:
        path = self.log_dir / f'{pts}ms.log'
        if path.exists():
            path = self.log_dir / f'{pts}ms_ch{channel_id}.log'
        payload = {
            'pts': pts,
            'channel_id': channel_id,
            'detections': detections,
            'created_ms': int(time.time() * 1000),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path

    def run_once(self) -> Optional[dict]:
        channel_id = self._next_channel_id()
        if channel_id is None:
            return None
        frame = self.ring.read_latest_frame(channel_id)
        if frame is None:
            return None
        image = self._prepare_image(frame)
        detections = self.detector.detect(image)
        log_path = self._write_log(pts=frame.pts, channel_id=channel_id, detections=detections)
        return {
            'pts': frame.pts,
            'channel_id': channel_id,
            'detections': detections,
            'log_path': str(log_path),
        }

    def run_loop(self, iterations: int = 0, sleep_s: float = 0.2) -> None:
        executed = 0
        while True:
            self.run_once()
            executed += 1
            if iterations and executed >= iterations:
                return
            time.sleep(sleep_s)
