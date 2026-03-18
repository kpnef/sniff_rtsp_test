from __future__ import annotations

import queue
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import ffmpeg

from .shared_ring import SharedVideoRingBuffer

SHOWINFO_RE = re.compile(r'pts_time:\s*([0-9\.\-]+)')


@dataclass
class DecodeStats:
    frames_written: int = 0
    last_pts: int = -1


class _FfmpegDecodeWorker:
    def __init__(
        self,
        process,
        ring: SharedVideoRingBuffer,
        channel_id: int,
        source_id: str,
        *,
        frame_width: int,
        frame_height: int,
        pix_fmt: str,
        synthetic_pts: bool = False,
    ) -> None:
        self.process = process
        self.ring = ring
        self.channel_id = channel_id
        self.source_id = source_id
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
            pts_value = match.group(1)
            try:
                pts_ms = int(float(pts_value) * 1000)
            except ValueError:
                continue
            self._stderr_pts_queue.put(pts_ms)

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
                if not chunk:
                    break
                if len(chunk) != self.frame_size:
                    break
                pts = self._resolve_pts(frame_index)
                self.ring.write_frame(
                    self.channel_id,
                    pts,
                    chunk,
                    width=self.frame_width,
                    height=self.frame_height,
                    pix_fmt=self.pix_fmt,
                    source_id=self.source_id,
                )
                frame_index += 1
                self.stats.frames_written = frame_index
                self.stats.last_pts = pts
        except BaseException as exc:  # pragma: no cover - only for crash visibility
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
    def __init__(
        self,
        ring: SharedVideoRingBuffer,
        channel_id: int,
        source_id: str,
        *,
        codec: str,
        frame_width: int,
        frame_height: int,
        pix_fmt: str = 'nv12',
    ) -> None:
        self.ring = ring
        self.channel_id = channel_id
        self.source_id = source_id
        self.codec = codec
        self.frame_width = frame_width
        self.frame_height = frame_height
        self.pix_fmt = pix_fmt
        self.frame_size = frame_width * frame_height * 3 // 2
        input_kwargs = {'format': 'h264' if codec == 'H264' else 'hevc'}
        stream = ffmpeg.input('pipe:', **input_kwargs)
        stream = stream.filter('showinfo')
        stream = ffmpeg.output(
            stream,
            'pipe:',
            format='rawvideo',
            pix_fmt=pix_fmt,
            s=f'{frame_width}x{frame_height}',
            loglevel='info',
        )
        self.process = ffmpeg.run_async(stream, pipe_stdin=True, pipe_stdout=True, pipe_stderr=True, quiet=True)
        self._pts_queue: 'queue.Queue[int]' = queue.Queue()
        self._stderr_thread = threading.Thread(target=self._stderr_loop, daemon=True)
        self._stdout_thread = threading.Thread(target=self._stdout_loop, daemon=True)
        self._stderr_thread.start()
        self._stdout_thread.start()
        self._frames_written = 0
        self._last_pts = -1
        self._closed = False

    def _stderr_loop(self) -> None:
        if self.process.stderr is None:
            return
        while True:
            line = self.process.stderr.readline()
            if not line:
                break

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
            self.ring.write_frame(
                self.channel_id,
                pts,
                chunk,
                width=self.frame_width,
                height=self.frame_height,
                pix_fmt=self.pix_fmt,
                source_id=self.source_id,
            )
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


def _build_decode_stream(input_url: str, *, pix_fmt: str, rtsp: bool = False):
    input_kwargs = {'rtsp_transport': 'tcp'} if rtsp else {}
    stream = ffmpeg.input(input_url, **input_kwargs)
    stream = stream.filter('showinfo')
    return ffmpeg.output(stream, 'pipe:', format='rawvideo', pix_fmt=pix_fmt, loglevel='info')


def _probe_video_shape(input_url: str) -> tuple[int, int]:
    probe = ffmpeg.probe(input_url)
    streams = [stream for stream in probe['streams'] if stream.get('codec_type') == 'video']
    if not streams:
        raise RuntimeError(f'no video stream found in {input_url!r}')
    stream = streams[0]
    return int(stream['width']), int(stream['height'])


def decode_video_file_to_shm(
    input_path: str | Path,
    ring: SharedVideoRingBuffer,
    channel_id: int,
    source_id: str,
    *,
    pix_fmt: str = 'nv12',
    timeout: float | None = 20,
) -> DecodeStats:
    input_path = str(input_path)
    width, height = _probe_video_shape(input_path)
    process = ffmpeg.run_async(
        _build_decode_stream(input_path, pix_fmt=pix_fmt),
        pipe_stdout=True,
        pipe_stderr=True,
        quiet=True,
    )
    worker = _FfmpegDecodeWorker(
        process=process,
        ring=ring,
        channel_id=channel_id,
        source_id=source_id,
        frame_width=width,
        frame_height=height,
        pix_fmt=pix_fmt,
    )
    return worker.run(timeout=timeout)


def decode_rtsp_to_shm(
    rtsp_url: str,
    ring: SharedVideoRingBuffer,
    channel_id: int,
    source_id: str,
    *,
    pix_fmt: str = 'nv12',
    timeout: float | None = None,
) -> DecodeStats:
    width, height = _probe_video_shape(rtsp_url)
    process = ffmpeg.run_async(
        _build_decode_stream(rtsp_url, pix_fmt=pix_fmt, rtsp=True),
        pipe_stdout=True,
        pipe_stderr=True,
        quiet=True,
    )
    worker = _FfmpegDecodeWorker(
        process=process,
        ring=ring,
        channel_id=channel_id,
        source_id=source_id,
        frame_width=width,
        frame_height=height,
        pix_fmt=pix_fmt,
        synthetic_pts=True,
    )
    return worker.run(timeout=timeout)
