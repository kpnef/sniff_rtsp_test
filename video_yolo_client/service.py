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
        stats_interval_s: float = 10.0,
    ) -> None:
        self.ring = ring
        self.detector = detector
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._cursor = 0
        self.stats_interval_s = stats_interval_s
        self._last_pts_by_channel: dict[int, int] = {}
        self._stats = {
            'total_frames_seen': 0,
            'total_frames_computed': 0,
            'total_frames_skipped_same_pts': 0,
            'window_frames_seen': 0,
            'window_frames_computed': 0,
            'window_frames_skipped_same_pts': 0,
            'stats_started_at': time.time(),
            'last_stats_report_at': time.time(),
        }

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

    def _mark_frame_seen(self) -> None:
        self._stats['total_frames_seen'] += 1
        self._stats['window_frames_seen'] += 1

    def _mark_frame_computed(self) -> None:
        self._stats['total_frames_computed'] += 1
        self._stats['window_frames_computed'] += 1

    def _mark_frame_skipped_same_pts(self) -> None:
        self._stats['total_frames_skipped_same_pts'] += 1
        self._stats['window_frames_skipped_same_pts'] += 1

    def _emit_stats(self, *, final: bool = False) -> None:
        now = time.time()
        elapsed = now - self._stats['last_stats_report_at']
        total_elapsed = max(now - self._stats['stats_started_at'], 1e-9)
        window_elapsed = max(elapsed, 1e-9)
        tag = 'final' if final else 'stats'
        print(
            f'[video_yolo_client][{tag}] ' 
            f'window_s={window_elapsed:.1f} ' 
            f'window_seen={self._stats["window_frames_seen"]} ' 
            f'window_computed={self._stats["window_frames_computed"]} ' 
            f'window_skipped_same_pts={self._stats["window_frames_skipped_same_pts"]} ' 
            f'total_seen={self._stats["total_frames_seen"]} ' 
            f'total_computed={self._stats["total_frames_computed"]} ' 
            f'total_skipped_same_pts={self._stats["total_frames_skipped_same_pts"]} ' 
            f'avg_compute_fps={self._stats["total_frames_computed"] / total_elapsed:.2f} ' 
            f'window_compute_fps={self._stats["window_frames_computed"] / window_elapsed:.2f}'
        )
        self._stats['window_frames_seen'] = 0
        self._stats['window_frames_computed'] = 0
        self._stats['window_frames_skipped_same_pts'] = 0
        self._stats['last_stats_report_at'] = now

    def _maybe_report_stats(self) -> None:
        now = time.time()
        elapsed = now - self._stats['last_stats_report_at']
        if elapsed < self.stats_interval_s:
            return
        self._emit_stats(final=False)

    def snapshot_stats(self) -> dict:
        return {
            'total_frames_seen': self._stats['total_frames_seen'],
            'total_frames_computed': self._stats['total_frames_computed'],
            'total_frames_skipped_same_pts': self._stats['total_frames_skipped_same_pts'],
            'window_frames_seen': self._stats['window_frames_seen'],
            'window_frames_computed': self._stats['window_frames_computed'],
            'window_frames_skipped_same_pts': self._stats['window_frames_skipped_same_pts'],
            'stats_started_at': self._stats['stats_started_at'],
            'last_stats_report_at': self._stats['last_stats_report_at'],
        }

    def run_once(self) -> Optional[dict]:
        channel_id = self._next_channel_id()
        if channel_id is None:
            self._maybe_report_stats()
            return None
        frame = self.ring.read_latest_frame(channel_id)
        if frame is None:
            self._maybe_report_stats()
            return None
        self._mark_frame_seen()
        last_pts = self._last_pts_by_channel.get(channel_id)
        if last_pts == frame.pts:
            self._mark_frame_skipped_same_pts()
            self._maybe_report_stats()
            return {
                'pts': frame.pts,
                'channel_id': channel_id,
                'detections': None,
                'log_path': None,
                'skipped_same_pts': True,
            }
        self._last_pts_by_channel[channel_id] = frame.pts
        image = self._prepare_image(frame)
        detections = self.detector.detect(image)
        self._mark_frame_computed()
        log_path = self._write_log(pts=frame.pts, channel_id=channel_id, detections=detections)
        self._maybe_report_stats()
        return {
            'pts': frame.pts,
            'channel_id': channel_id,
            'detections': detections,
            'log_path': str(log_path),
            'skipped_same_pts': False,
        }

    def run_loop(self, iterations: int = 0, sleep_s: float = 0.2) -> None:
        executed = 0
        while True:
            self.run_once()
            executed += 1
            if iterations and executed >= iterations:
                self._emit_stats(final=True)
                return
            time.sleep(sleep_s)
