from __future__ import annotations

import queue
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import cv2
import ffmpeg
import numpy as np

from .shared_ring import SharedVideoRingBuffer

SHOWINFO_RE = re.compile(r'pts_time:\s*([0-9\.\-]+)')


@dataclass
class DecodeStats:
    frames_written: int = 0
    last_pts: int = -1


class SharedMemoryFrameSink:
    def __init__(self, ring: SharedVideoRingBuffer, channel_id: int, source_id: str, *, target_pix_fmt: str | None = None) -> None:
        self.ring = ring
        self.channel_id = channel_id
        self.source_id = source_id
        self.target_width = ring.config.frame_width
        self.target_height = ring.config.frame_height
        self.target_pix_fmt = target_pix_fmt or ring.config.pix_fmt

    def write_frame(self, pts: int, frame_bytes: bytes, *, width: int, height: int, pix_fmt: str) -> None:
        out_width, out_height, out_pix_fmt, out_bytes = normalize_frame_for_shared_memory(
            frame_bytes,
            width=width,
            height=height,
            pix_fmt=pix_fmt,
            max_width=self.target_width,
            max_height=self.target_height,
            target_pix_fmt=self.target_pix_fmt,
        )
        self.ring.write_frame(
            self.channel_id,
            pts,
            out_bytes,
            width=out_width,
            height=out_height,
            pix_fmt=out_pix_fmt,
            source_id=self.source_id,
        )


class _FfmpegDecodeWorker:
    def __init__(self, process, sink: SharedMemoryFrameSink, *, frame_width: int, frame_height: int, pix_fmt: str, synthetic_pts: bool = False) -> None:
        self.process = process
        self.sink = sink
        self.frame_width = frame_width
        self.frame_height = frame_height
        self.pix_fmt = pix_fmt
        self.frame_size = frame_width * frame_height * 3 // 2
        self.synthetic_pts = synthetic_pts
        self.stats = DecodeStats()
        self._stderr_pts_queue: 'queue.Queue[int]' = queue.Queue()
        self._stdout_exc: Optional[BaseException] = None
        self._stderr_thread = threading.Thread(target=self._stderr_loop, daemon=True)
        self._stdout_thread = threading.Thread(target=self._stdout_loop, daemon=True)

    def _stderr_loop(self) -> None:
        if self.process.stderr is None:
            return
        while True:
            line = self.process.stderr.readline()
            if not line:
                break
            text = line.decode('utf-8', errors='ignore')
            match = SHOWINFO_RE.search(text)
            if match is None:
                continue
            try:
                self._stderr_pts_queue.put(int(float(match.group(1)) * 1000))
            except ValueError:
                continue

    def _resolve_pts(self, frame_index: int) -> int:
        if self.synthetic_pts:
            return frame_index
        try:
            return self._stderr_pts_queue.get(timeout=0.5)
        except queue.Empty:
            return frame_index

    def _stdout_loop(self) -> None:
        assert self.process.stdout is not None
        frame_index = 0
        try:
            while True:
                chunk = self.process.stdout.read(self.frame_size)
                if not chunk or len(chunk) != self.frame_size:
                    break
                pts = self._resolve_pts(frame_index)
                self.sink.write_frame(pts, chunk, width=self.frame_width, height=self.frame_height, pix_fmt=self.pix_fmt)
                frame_index += 1
                self.stats.frames_written = frame_index
                self.stats.last_pts = pts
        except BaseException as exc:
            self._stdout_exc = exc

    def run(self, timeout: Optional[float] = None) -> DecodeStats:
        self._stderr_thread.start()
        self._stdout_thread.start()
        self._stdout_thread.join(timeout=timeout)
        if self._stdout_thread.is_alive():
            raise TimeoutError('ffmpeg stdout thread did not finish in time')
        self.process.wait(timeout=timeout)
        self._stderr_thread.join(timeout=1)
        if self._stdout_exc is not None:
            raise self._stdout_exc
        if self.process.returncode not in (0, None):
            raise RuntimeError(f'ffmpeg exited with returncode={self.process.returncode}')
        return self.stats


class AnnexBSharedMemoryWriter:
    def __init__(self, ring: SharedVideoRingBuffer, channel_id: int, source_id: str, *, codec: str, frame_width: int, frame_height: int, pix_fmt: str = 'nv12') -> None:
        self.ring = ring
        self.channel_id = channel_id
        self.source_id = source_id
        self.codec = codec
        self.frame_width = frame_width
        self.frame_height = frame_height
        self.pix_fmt = pix_fmt
        self.frame_size = frame_width * frame_height * 3 // 2
        self.sink = SharedMemoryFrameSink(ring, channel_id, source_id, target_pix_fmt=pix_fmt)
        input_kwargs = {'format': 'h264' if codec == 'H264' else 'hevc'}
        stream = ffmpeg.input('pipe:', **input_kwargs).filter('showinfo')
        stream = ffmpeg.output(stream, 'pipe:', format='rawvideo', pix_fmt=pix_fmt, s=f'{frame_width}x{frame_height}', loglevel='info')
        self.process = ffmpeg.run_async(stream, pipe_stdin=True, pipe_stdout=True, pipe_stderr=True, quiet=True)
        self._pts_queue: 'queue.Queue[int]' = queue.Queue()
        self._stderr_thread = threading.Thread(target=self._stderr_loop, daemon=True)
        self._stdout_thread = threading.Thread(target=self._stdout_loop, daemon=True)
        self._stderr_thread.start(); self._stdout_thread.start()
        self._frames_written = 0
        self._last_pts = -1
        self._closed = False

    def _stderr_loop(self) -> None:
        if self.process.stderr is None:
            return
        while self.process.stderr.readline():
            pass

    def _stdout_loop(self) -> None:
        assert self.process.stdout is not None
        while True:
            chunk = self.process.stdout.read(self.frame_size)
            if not chunk or len(chunk) != self.frame_size:
                break
            try:
                pts = self._pts_queue.get(timeout=1.0)
            except queue.Empty:
                pts = int(time.time() * 1000)
            self.sink.write_frame(pts, chunk, width=self.frame_width, height=self.frame_height, pix_fmt=self.pix_fmt)
            self._frames_written += 1
            self._last_pts = pts

    def push_access_unit(self, annexb_bytes: bytes, pts: int) -> None:
        if self._closed:
            raise RuntimeError('annexb writer already closed')
        assert self.process.stdin is not None
        self._pts_queue.put(pts)
        self.process.stdin.write(annexb_bytes)
        self.process.stdin.flush()

    def close(self) -> DecodeStats:
        if self._closed:
            return DecodeStats(frames_written=self._frames_written, last_pts=self._last_pts)
        self._closed = True
        if self.process.stdin is not None:
            self.process.stdin.close()
        self._stdout_thread.join(timeout=3)
        self.process.wait(timeout=3)
        self._stderr_thread.join(timeout=1)
        return DecodeStats(frames_written=self._frames_written, last_pts=self._last_pts)


def _raw_to_bgr(frame_bytes: bytes, *, width: int, height: int, pix_fmt: str) -> np.ndarray:
    y_size = width * height
    uv_size = y_size // 4
    if pix_fmt == 'nv12':
        y = np.frombuffer(frame_bytes[:y_size], dtype=np.uint8).reshape((height, width))
        uv = np.frombuffer(frame_bytes[y_size:y_size + 2 * uv_size], dtype=np.uint8).reshape((height // 2, width // 2, 2))
        return cv2.cvtColorTwoPlane(y, uv, cv2.COLOR_YUV2BGR_NV12)
    if pix_fmt == 'yuv420p':
        yuv = np.frombuffer(frame_bytes[: y_size + 2 * uv_size], dtype=np.uint8).reshape((height * 3 // 2, width))
        return cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_I420)
    raise ValueError(f'unsupported pix_fmt: {pix_fmt}')


def _bgr_to_raw_nv12(frame_bgr: np.ndarray) -> bytes:
    height, width = frame_bgr.shape[:2]
    i420 = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2YUV_I420).reshape(-1)
    y_size = width * height
    uv_plane = y_size // 4
    y = i420[:y_size]
    u = i420[y_size:y_size + uv_plane]
    v = i420[y_size + uv_plane:y_size + 2 * uv_plane]
    uv = np.empty(uv_plane * 2, dtype=np.uint8)
    uv[0::2] = u
    uv[1::2] = v
    return np.concatenate([y, uv]).tobytes()


def _bgr_to_raw_yuv420p(frame_bgr: np.ndarray) -> bytes:
    return cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2YUV_I420).reshape(-1).tobytes()


def _fit_within(width: int, height: int, max_width: int, max_height: int) -> tuple[int, int]:
    if width <= max_width and height <= max_height:
        return width, height
    scale = min(max_width / width, max_height / height)
    return max(2, int(width * scale) // 2 * 2), max(2, int(height * scale) // 2 * 2)


def normalize_frame_for_shared_memory(frame_bytes: bytes, *, width: int, height: int, pix_fmt: str, max_width: int, max_height: int, target_pix_fmt: str) -> tuple[int, int, str, bytes]:
    out_width, out_height = _fit_within(width, height, max_width, max_height)
    if (out_width, out_height) == (width, height) and pix_fmt == target_pix_fmt:
        return width, height, pix_fmt, frame_bytes
    frame_bgr = _raw_to_bgr(frame_bytes, width=width, height=height, pix_fmt=pix_fmt)
    if (out_width, out_height) != (width, height):
        frame_bgr = cv2.resize(frame_bgr, (out_width, out_height), interpolation=cv2.INTER_AREA)
    if target_pix_fmt == 'nv12':
        out_bytes = _bgr_to_raw_nv12(frame_bgr)
    elif target_pix_fmt == 'yuv420p':
        out_bytes = _bgr_to_raw_yuv420p(frame_bgr)
    else:
        raise ValueError(f'unsupported target_pix_fmt: {target_pix_fmt}')
    return out_width, out_height, target_pix_fmt, out_bytes


def _build_decode_stream(input_path: str, *, pix_fmt: str):
    return ffmpeg.output(ffmpeg.input(input_path).filter('showinfo'), 'pipe:', format='rawvideo', pix_fmt=pix_fmt, loglevel='info')


def _probe_video_shape(input_path: str) -> tuple[int, int]:
    probe = ffmpeg.probe(input_path)
    streams = [stream for stream in probe['streams'] if stream.get('codec_type') == 'video']
    if not streams:
        raise RuntimeError(f'no video stream found in {input_path!r}')
    stream = streams[0]
    return int(stream['width']), int(stream['height'])


def decode_video_file_to_shm(input_path: str | Path, ring: SharedVideoRingBuffer, channel_id: int, source_id: str, *, pix_fmt: str = 'nv12', timeout: float | None = 20) -> DecodeStats:
    input_path = str(input_path)
    width, height = _probe_video_shape(input_path)
    process = ffmpeg.run_async(_build_decode_stream(input_path, pix_fmt=pix_fmt), pipe_stdout=True, pipe_stderr=True, quiet=True)
    worker = _FfmpegDecodeWorker(process=process, sink=SharedMemoryFrameSink(ring, channel_id, source_id, target_pix_fmt=ring.config.pix_fmt), frame_width=width, frame_height=height, pix_fmt=pix_fmt)
    return worker.run(timeout=timeout)
