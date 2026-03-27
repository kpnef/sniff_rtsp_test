#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import os
import queue
import re
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urljoin, urlparse

import ffmpeg

from video_shm_core.config import SharedVideoConfig
from video_shm_core.ffmpeg_decoder import SharedMemoryFrameSink
from video_shm_core.shared_ring import SharedVideoRingBuffer

SHOWINFO_PTS_RE = re.compile(r'pts_time:\s*([0-9\.\-]+)')
SHOWINFO_ISKEY_RE = re.compile(r'iskey:(\d)')
RTP_HEADER_STRUCT = __import__('struct').Struct('!BBHII')

_PCM_AUDIO_CODECS = {
    'pcm_alaw',
    'pcm_mulaw',
    'pcm_s16be',
    'pcm_s16le',
    'pcm_s24be',
    'pcm_s24le',
    'pcm_s32be',
    'pcm_s32le',
    'pcm_u8',
}


@dataclass
class ActivePullStats:
    source_id: str
    channel_id: int
    pulled_chunks: int = 0
    pulled_bytes: int = 0
    video_access_units: int = 0
    video_rtp_packets: int = 0
    video_nalus: int = 0
    audio_pulled_chunks: int = 0
    audio_pulled_bytes: int = 0
    audio_packets: int = 0
    decoded_frames: int = 0
    key_frames: int = 0
    nonkey_frames: int = 0
    last_pts: int = -1
    queue_high_watermark: int = 0
    audio_queue_high_watermark: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def frames_written(self) -> int:
        """SNIFF-aligned frame count: treat each emitted video NALU as one frame unit."""
        return self.video_nalus

    @property
    def audio_frames(self) -> int:
        """Backward-compatible alias for older logs/tests."""
        return self.audio_packets


class StatsTracker:
    def __init__(self, source_id: str, channel_id: int) -> None:
        self._lock = threading.Lock()
        self.stats = ActivePullStats(source_id=source_id, channel_id=channel_id)

    def note_pull(self, chunk_len: int, queue_size: int) -> None:
        with self._lock:
            self.stats.pulled_chunks += 1
            self.stats.video_access_units += 1
            self.stats.pulled_bytes += chunk_len
            self.stats.queue_high_watermark = max(self.stats.queue_high_watermark, queue_size)

    def note_audio_pull(self, chunk_len: int, packet_count: int = 0) -> None:
        with self._lock:
            self.stats.audio_pulled_chunks += 1
            self.stats.audio_pulled_bytes += chunk_len
            self.stats.audio_packets += packet_count

    def note_audio_rtp_packet(self, payload_len: int) -> None:
        with self._lock:
            self.stats.audio_pulled_chunks += 1
            self.stats.audio_pulled_bytes += payload_len
            self.stats.audio_packets += 1

    def note_video_rtp_packet(self) -> None:
        with self._lock:
            self.stats.video_rtp_packets += 1

    def note_video_nalus(self, count: int) -> None:
        if count <= 0:
            return
        with self._lock:
            self.stats.video_nalus += count

    def note_decoded_frame(self, pts: int, is_key: bool) -> None:
        with self._lock:
            self.stats.decoded_frames += 1
            self.stats.last_pts = pts
            if is_key:
                self.stats.key_frames += 1
            else:
                self.stats.nonkey_frames += 1

    def snapshot(self) -> ActivePullStats:
        with self._lock:
            return ActivePullStats(**self.stats.__dict__)


def format_stats_line(
    snap: ActivePullStats,
    *,
    queue_size: int = 0,
    prefix: str = '[video_rtsp_ingest]',
) -> str:
    return (
        f'{prefix} '
        f'source={snap.source_id} channel={snap.channel_id} '
        f'video_queue={queue_size} video_high={snap.queue_high_watermark} '
        f'audio_queue=0 audio_high={snap.audio_queue_high_watermark} '
        f'key_frames={snap.key_frames} nonkey_frames={snap.nonkey_frames} '
        f'audio_packets={snap.audio_packets} frames={snap.video_nalus}  '
        f'pulled_chunks={snap.pulled_chunks} pulled_bytes={snap.pulled_bytes} '
        f'video_rtp_packets={snap.video_rtp_packets} video_nalus={snap.video_nalus} '
        f'audio_pulled_chunks={snap.audio_pulled_chunks} audio_pulled_bytes={snap.audio_pulled_bytes} '
        f'last_pts={snap.last_pts}'
    )


class MediaPtsTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._base_ts_by_media: dict[str, int] = {}
        self._latest_pts_ms_by_media: dict[str, int] = {}

    def note_rtp_packet(self, media: str, rtp_ts: int, clock_rate: int | None) -> int:
        media_key = media or 'unknown'
        if not clock_rate or clock_rate <= 0:
            pts_ms = -1
        else:
            with self._lock:
                base_ts = self._base_ts_by_media.setdefault(media_key, rtp_ts)
                delta = (rtp_ts - base_ts) & 0xFFFFFFFF
                pts_ms = int((delta * 1000) / clock_rate)
                self._latest_pts_ms_by_media[media_key] = pts_ms
                return pts_ms
        with self._lock:
            self._latest_pts_ms_by_media[media_key] = pts_ms
        return pts_ms

    def latest_pts_ms(self, media: str) -> int:
        with self._lock:
            return self._latest_pts_ms_by_media.get(media or 'unknown', -1)


class LineLogWriter:
    def __init__(self, path: str | os.PathLike[str] | None) -> None:
        self.path = None if path is None else Path(path)
        self._fp = None
        self._lock = threading.Lock()
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fp = self.path.open('a', encoding='utf-8', buffering=1)

    def write(self, line: str) -> None:
        if self._fp is None:
            return
        with self._lock:
            self._fp.write(line.rstrip('\n') + '\n')

    def close(self) -> None:
        if self._fp is None:
            return
        with self._lock:
            self._fp.close()
            self._fp = None


class AnnexBNaluLogger:
    def __init__(self, codec_name: str, writer: LineLogWriter, pts_supplier: Callable[[], int] | None = None) -> None:
        self.codec_name = codec_name.lower()
        self.writer = writer
        self.pts_supplier = pts_supplier
        self._buffer = bytearray()
        self._index = 0

    def feed(self, chunk: bytes) -> int:
        if not chunk:
            return 0
        self._buffer.extend(chunk)
        emitted = 0
        units, remain = _extract_annexb_units(bytes(self._buffer))
        self._buffer = bytearray(remain)
        for unit in units:
            nalu_type = _nalu_type_from_unit(unit, self.codec_name)
            pts_ms = self.pts_supplier() if self.pts_supplier is not None else -1
            self.writer.write(
                f'index={self._index} codec={self.codec_name} nalu_type={nalu_type} size={len(unit)} pts_ms={pts_ms}'
            )
            self._index += 1
            emitted += 1
        return emitted

    def flush(self) -> int:
        if not self._buffer:
            return 0
        data = bytes(self._buffer)
        self._buffer.clear()
        unit = _strip_start_code(data)
        if not unit:
            return 0
        nalu_type = _nalu_type_from_unit(unit, self.codec_name)
        pts_ms = self.pts_supplier() if self.pts_supplier is not None else -1
        self.writer.write(f'index={self._index} codec={self.codec_name} nalu_type={nalu_type} size={len(unit)} pts_ms={pts_ms}')
        self._index += 1
        return 1


class RtspInterleavedRtpLogger:
    def __init__(
        self,
        rtsp_url: str,
        writer: LineLogWriter,
        stop_event: threading.Event,
        timeout: float = 10.0,
        packet_observer: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self.rtsp_url = rtsp_url
        self.writer = writer
        self.stop_event = stop_event
        self.timeout = timeout
        self.packet_observer = packet_observer
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._exc: BaseException | None = None

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)
        if self._exc is not None:
            raise self._exc

    def _run(self) -> None:
        sock = None
        try:
            parsed = urlparse(self.rtsp_url)
            host = parsed.hostname or ''
            port = parsed.port or 554
            sock = socket.create_connection((host, port), timeout=self.timeout)
            sock.settimeout(1.0)
            cseq = 1
            self._request(sock, 'OPTIONS', self.rtsp_url, cseq); cseq += 1
            _status, _headers, body = self._request(sock, 'DESCRIBE', self.rtsp_url, cseq, extra={'Accept': 'application/sdp'})
            cseq += 1
            tracks = _parse_sdp_tracks(body.decode('utf-8', errors='ignore'), self.rtsp_url)
            session = None
            channel_map: dict[int, str] = {}
            for idx, track in enumerate(tracks):
                interleaved = f'{idx*2}-{idx*2+1}'
                _status, headers, _ = self._request(
                    sock,
                    'SETUP',
                    track['control'],
                    cseq,
                    extra={'Transport': f'RTP/AVP/TCP;unicast;interleaved={interleaved}'} | ({'Session': session} if session else {}),
                )
                cseq += 1
                session = _session_id_from_headers(headers) or session
                channel_map[idx * 2] = track
            play_extra = {'Session': session} if session else {}
            self._request(sock, 'PLAY', self.rtsp_url, cseq, extra=play_extra); cseq += 1
            buffer = bytearray()
            while not self.stop_event.is_set():
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    return
                buffer.extend(chunk)
                while True:
                    consumed = _consume_interleaved_frame(buffer, channel_map, self.writer, self.packet_observer)
                    if consumed <= 0:
                        break
                    del buffer[:consumed]
            with contextlib.suppress(Exception):
                self._request(sock, 'TEARDOWN', self.rtsp_url, cseq, extra=play_extra)
        except BaseException as exc:
            self._exc = RuntimeError(f'RTP logging probe failed: {exc}')
        finally:
            if sock is not None:
                with contextlib.suppress(Exception):
                    sock.close()

    def _request(self, sock: socket.socket, method: str, url: str, cseq: int, extra: dict[str, str] | None = None) -> tuple[int, dict[str, str], bytes]:
        lines = [f'{method} {url} RTSP/1.0', f'CSeq: {cseq}', 'User-Agent: video_rtsp_ingest-rtp-logger']
        if extra:
            lines.extend(f'{k}: {v}' for k, v in extra.items())
        payload = ('\r\n'.join(lines) + '\r\n\r\n').encode('utf-8')
        sock.sendall(payload)
        return _read_rtsp_response(sock)





def _strip_start_code(data: bytes) -> bytes:
    if data.startswith(b'\x00\x00\x00\x01'):
        return data[4:]
    if data.startswith(b'\x00\x00\x01'):
        return data[3:]
    return data


def _find_start_codes(data: bytes) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    i = 0
    limit = len(data) - 3
    while i <= limit:
        if data[i:i+4] == b'\x00\x00\x00\x01':
            result.append((i, 4))
            i += 4
            continue
        if data[i:i+3] == b'\x00\x00\x01':
            result.append((i, 3))
            i += 3
            continue
        i += 1
    return result


def _extract_annexb_units(data: bytes) -> tuple[list[bytes], bytes]:
    starts = _find_start_codes(data)
    if not starts:
        return [], data
    units: list[bytes] = []
    for idx in range(len(starts) - 1):
        start, prefix_len = starts[idx]
        next_start, _ = starts[idx + 1]
        unit = data[start + prefix_len:next_start]
        if unit:
            units.append(unit)
    last_start, _ = starts[-1]
    return units, data[last_start:]


def _nalu_type_from_unit(unit: bytes, codec_name: str) -> int | None:
    if not unit:
        return None
    codec = codec_name.lower()
    if codec == 'h264':
        return unit[0] & 0x1F
    if codec in ('hevc', 'h265'):
        return (unit[0] >> 1) & 0x3F
    return None


def _parse_sdp_tracks(sdp_text: str, base_url: str) -> list[dict[str, str | int | None]]:
    tracks: list[dict[str, str | int | None]] = []
    current_media: str | None = None
    current_payload_types: set[int] = set()
    current_clock_rates: dict[int, int] = {}
    session_control: str | None = None
    for raw_line in sdp_text.splitlines():
        line = raw_line.strip()
        if line.startswith('m='):
            parts = line[2:].split()
            current_media = parts[0] if parts else None
            current_payload_types = set()
            current_clock_rates = {}
            for part in parts[3:]:
                try:
                    current_payload_types.add(int(part))
                except ValueError:
                    continue
        elif line.startswith('a=rtpmap:') and current_media is not None:
            payload_desc = line[len('a=rtpmap:'):]
            payload_id, _, codec_desc = payload_desc.partition(' ')
            try:
                pt = int(payload_id)
            except ValueError:
                continue
            codec_parts = codec_desc.split('/')
            if len(codec_parts) >= 2:
                try:
                    current_clock_rates[pt] = int(codec_parts[1])
                except ValueError:
                    pass
        elif line.startswith('a=control:'):
            control = line.split(':', 1)[1]
            if current_media is None:
                session_control = control
                continue
            full_control = control
            if not control.startswith('rtsp://'):
                base = base_url if session_control in (None, '*') else urljoin(base_url.rstrip('/') + '/', session_control)
                full_control = urljoin(base.rstrip('/') + '/', control)
            clock_rate = None
            for pt in sorted(current_payload_types):
                if pt in current_clock_rates:
                    clock_rate = current_clock_rates[pt]
                    break
            if clock_rate is None and current_media == 'video':
                clock_rate = 90000
            tracks.append({'media': current_media, 'control': full_control, 'clock_rate': clock_rate})
    return [t for t in tracks if t['media'] in ('video', 'audio')]


def _session_id_from_headers(headers: dict[str, str]) -> str | None:
    value = headers.get('session')
    if not value:
        return None
    return value.split(';', 1)[0].strip()


def _read_rtsp_response(sock: socket.socket) -> tuple[int, dict[str, str], bytes]:
    data = bytearray()
    while b'\r\n\r\n' not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError('RTSP response closed early')
        data.extend(chunk)
    head, body = bytes(data).split(b'\r\n\r\n', 1)
    lines = head.decode('utf-8', errors='ignore').split('\r\n')
    status = int(lines[0].split(' ', 2)[1])
    headers: dict[str, str] = {}
    content_length = 0
    for line in lines[1:]:
        if ':' not in line:
            continue
        key, value = line.split(':', 1)
        headers[key.strip().lower()] = value.strip()
    content_length = int(headers.get('content-length', '0') or '0')
    while len(body) < content_length:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError('RTSP body closed early')
        body += chunk
    return status, headers, body[:content_length]


def _consume_interleaved_frame(
    buffer: bytearray,
    channel_map: dict[int, dict[str, str | int | None] | str],
    writer: LineLogWriter,
    packet_observer: Callable[[dict[str, object]], None] | None = None,
) -> int:
    if len(buffer) < 4:
        return 0
    if buffer[0] != 0x24:
        # skip non-interleaved text until next marker
        next_marker = buffer.find(b'$')
        return len(buffer) if next_marker < 0 else next_marker
    payload_len = int.from_bytes(buffer[2:4], 'big')
    total_len = 4 + payload_len
    if len(buffer) < total_len:
        return 0
    channel = buffer[1]
    payload = bytes(buffer[4:total_len])
    channel_info = channel_map.get(channel, 'unknown')
    if isinstance(channel_info, dict):
        media = str(channel_info.get('media', 'unknown'))
        clock_rate_value = channel_info.get('clock_rate')
        clock_rate = int(clock_rate_value) if clock_rate_value not in (None, '') else None
    else:
        media = channel_info
        clock_rate = 90000 if media == 'video' else None
    if payload_len >= 12:
        b1, b2, seq, ts, ssrc = RTP_HEADER_STRUCT.unpack_from(payload, 0)
        version = b1 >> 6
        pt = b2 & 0x7F
        marker = (b2 >> 7) & 0x1
        cc = b1 & 0x0F
        has_extension = (b1 & 0x10) != 0
        header_len = 12 + cc * 4
        if version == 2 and payload_len >= header_len:
            if has_extension and payload_len >= header_len + 4:
                ext_words = int.from_bytes(payload[header_len + 2:header_len + 4], 'big')
                header_len += 4 + ext_words * 4
            if payload_len < header_len:
                writer.write(f'channel={channel} media={media} malformed=1 bytes={payload_len}')
                return total_len
            payload_only = payload[header_len:]
            payload_only_len = len(payload_only)
            if packet_observer is not None:
                packet_observer({
                    'channel': channel,
                    'media': media,
                    'seq': seq,
                    'ts': ts,
                    'ssrc': ssrc,
                    'pt': pt,
                    'marker': marker,
                    'payload_len': payload_only_len,
                    'payload_bytes': payload_only,
                    'clock_rate': clock_rate,
                })
            else:
                writer.write(
                    f'channel={channel} media={media} seq={seq} ts={ts} ssrc={ssrc} pt={pt} marker={marker} payload={payload_only_len} pts_ms=-1'
                )
        else:
            writer.write(f'channel={channel} media={media} malformed=1 bytes={payload_len}')
    else:
        writer.write(f'channel={channel} media={media} short=1 bytes={payload_len}')
    return total_len


class RtpVideoNaluLogger:
    def __init__(self, codec_name: str, writer: LineLogWriter) -> None:
        self.codec_name = codec_name.lower()
        self.writer = writer
        self._index = 0
        self._h264_fu_buffer = bytearray()
        self._h264_fu_pts_ms = -1

    def note_rtp_packet(self, payload: bytes, pts_ms: int) -> int:
        if not payload:
            return 0
        if self.codec_name == 'h264':
            return self._note_h264(payload, pts_ms)
        return 0

    def flush(self) -> int:
        emitted = 0
        if self._h264_fu_buffer:
            emitted += self._emit(bytes(self._h264_fu_buffer), self._h264_fu_pts_ms)
            self._h264_fu_buffer.clear()
            self._h264_fu_pts_ms = -1
        return emitted

    def _note_h264(self, payload: bytes, pts_ms: int) -> int:
        nal_type = payload[0] & 0x1F
        if 1 <= nal_type <= 23:
            return self._emit(payload, pts_ms)
        if nal_type == 24:
            emitted = 0
            offset = 1
            while offset + 2 <= len(payload):
                size = int.from_bytes(payload[offset:offset+2], 'big')
                offset += 2
                if size <= 0 or offset + size > len(payload):
                    break
                emitted += self._emit(payload[offset:offset+size], pts_ms)
                offset += size
            return emitted
        if nal_type == 28 and len(payload) >= 2:
            fu_indicator = payload[0]
            fu_header = payload[1]
            start = (fu_header & 0x80) != 0
            end = (fu_header & 0x40) != 0
            original_type = fu_header & 0x1F
            nri = fu_indicator & 0x60
            forbidden = fu_indicator & 0x80
            nal_header = bytes([forbidden | nri | original_type])
            fragment = payload[2:]
            if start:
                self._h264_fu_buffer = bytearray(nal_header)
                self._h264_fu_buffer.extend(fragment)
                self._h264_fu_pts_ms = pts_ms
                return 0
            if self._h264_fu_buffer:
                self._h264_fu_buffer.extend(fragment)
                if end:
                    unit = bytes(self._h264_fu_buffer)
                    emit_pts = self._h264_fu_pts_ms
                    self._h264_fu_buffer.clear()
                    self._h264_fu_pts_ms = -1
                    return self._emit(unit, emit_pts)
            return 0
        return 0

    def _emit(self, unit: bytes, pts_ms: int) -> int:
        nalu_type = _nalu_type_from_unit(unit, self.codec_name)
        self.writer.write(
            f'index={self._index} codec={self.codec_name} nalu_type={nalu_type} size={len(unit)} pts_ms={pts_ms}'
        )
        self._index += 1
        return 1


@dataclass(frozen=True)
class RtspVideoSpec:
    width: int
    height: int
    codec_name: str
    has_audio: bool
    audio_codec_name: str | None = None

    @property
    def elementary_stream_format(self) -> str:
        if self.codec_name == 'h264':
            return 'h264'
        if self.codec_name in ('hevc', 'h265'):
            return 'hevc'
        raise ValueError(f'unsupported RTSP video codec: {self.codec_name}')


class ActivePullPipeline:
    def __init__(
        self,
        rtsp_url: str,
        ring: SharedVideoRingBuffer,
        channel_id: int,
        source_id: str,
        *,
        pix_fmt: str = 'nv12',
        timeout: float | None = None,
        queue_max_chunks: int = 400,
        audio_queue_max_chunks: int = 400,
        meta_queue_max_items: int = 400,
        stats_interval: float = 1.0,
        rtp_log_path: str | None = 'video_rtsp_ingest_rtp.log',
        nalu_log_path: str | None = 'video_rtsp_ingest_nalu.log',
    ) -> None:
        self.rtsp_url = rtsp_url
        self.ring = ring
        self.channel_id = channel_id
        self.source_id = source_id
        self.pix_fmt = pix_fmt
        self.timeout = timeout
        self.queue_max_chunks = queue_max_chunks
        self.audio_queue_max_chunks = audio_queue_max_chunks
        self.meta_queue_max_items = meta_queue_max_items
        self.stats_interval = stats_interval
        self.rtp_log_path = rtp_log_path
        self.nalu_log_path = nalu_log_path
        self.video_spec = _probe_rtsp_spec(rtsp_url)
        self.sink = SharedMemoryFrameSink(ring, channel_id, source_id, target_pix_fmt=ring.config.pix_fmt)
        self.stats_tracker = StatsTracker(source_id, channel_id)
        self._queue: queue.Queue[bytes | None] = queue.Queue(maxsize=queue_max_chunks)
        self._stop_event = threading.Event()
        self._pull_exc: Optional[BaseException] = None
        self._decode_exc: Optional[BaseException] = None
        self._audio_exc: Optional[BaseException] = None
        self._reporter_thread = threading.Thread(target=self._stats_reporter_loop, daemon=True)
        self._rtp_log_writer = LineLogWriter(rtp_log_path)
        self._nalu_log_writer = LineLogWriter(nalu_log_path)
        self._pts_tracker = MediaPtsTracker()
        self._nalu_logger = RtpVideoNaluLogger(self.video_spec.codec_name, self._nalu_log_writer)
        self._rtp_logger = RtspInterleavedRtpLogger(
            rtsp_url,
            self._rtp_log_writer,
            self._stop_event,
            packet_observer=self._handle_rtp_packet,
        )
        self._pull_thread = threading.Thread(target=self._pull_loop, daemon=True)
        self._decode_thread = threading.Thread(target=self._decode_loop, daemon=True)

    def run(self) -> ActivePullStats:
        try:
            self._reporter_thread.start()
            self._rtp_logger.start()
            self._pull_thread.start()
            self._decode_thread.start()
            self._pull_thread.join(timeout=self.timeout)
            if self._pull_thread.is_alive():
                self._stop_event.set()
                raise TimeoutError('RTSP pull thread did not finish in time')
            self._decode_thread.join(timeout=self.timeout)
            if self._decode_thread.is_alive():
                self._stop_event.set()
                raise TimeoutError('decode thread did not finish in time')
            if self._pull_exc is not None:
                raise self._pull_exc
            if self._decode_exc is not None:
                raise self._decode_exc
            if self._audio_exc is not None:
                raise self._audio_exc
            return self.stats_tracker.snapshot()
        finally:
            self._stop_event.set()
            with contextlib.suppress(Exception):
                self._reporter_thread.join(timeout=1)
            with contextlib.suppress(Exception):
                self._rtp_logger.join(timeout=1)
            with contextlib.suppress(Exception):
                self._pull_thread.join(timeout=1)
            with contextlib.suppress(Exception):
                self._decode_thread.join(timeout=1)
            with contextlib.suppress(Exception):
                self.stats_tracker.note_video_nalus(self._nalu_logger.flush())
            snap = self.stats_tracker.snapshot()
            print(format_stats_line(snap, queue_size=self._queue.qsize(), prefix='[video_rtsp_ingest][final]'))
            self._rtp_log_writer.close()
            self._nalu_log_writer.close()

    def _stats_reporter_loop(self) -> None:
        while not self._stop_event.is_set():
            snap = self.stats_tracker.snapshot()
            print(format_stats_line(snap, queue_size=self._queue.qsize()))
            if self._pull_thread.is_alive() or self._decode_thread.is_alive() or self._rtp_logger._thread.is_alive():
                self._stop_event.wait(self.stats_interval)
            else:
                return

    def _pull_loop(self) -> None:
        process = None
        try:
            stream = ffmpeg.output(
                ffmpeg.input(self.rtsp_url, rtsp_transport='tcp')['v'],
                'pipe:',
                format=self.video_spec.elementary_stream_format,
                vcodec='copy',
                loglevel='error',
            )
            process = ffmpeg.run_async(stream, pipe_stdout=True, pipe_stderr=True, quiet=True)
            assert process.stdout is not None
            while not self._stop_event.is_set():
                chunk = process.stdout.read(64 * 1024)
                if not chunk:
                    break
                self._queue.put(chunk)
                self.stats_tracker.note_pull(len(chunk), self._queue.qsize())
        except BaseException as exc:
            self._pull_exc = exc
            self._stop_event.set()
        finally:
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                self._queue.get_nowait()
                self._queue.put_nowait(None)
            if process is not None:
                try:
                    process.wait(timeout=3)
                except Exception:
                    process.kill()

    def _handle_rtp_packet(self, packet: dict[str, int | str | None]) -> None:
        media = str(packet.get('media', 'unknown'))
        rtp_ts = int(packet.get('ts', 0) or 0)
        clock_rate_value = packet.get('clock_rate')
        clock_rate = int(clock_rate_value) if clock_rate_value not in (None, '') else None
        pts_ms = self._pts_tracker.note_rtp_packet(media, rtp_ts, clock_rate)
        payload_len = int(packet.get('payload_len', 0) or 0)
        payload_bytes = packet.get('payload_bytes', b'') or b''
        self._rtp_log_writer.write(
            f"channel={packet.get('channel')} media={media} seq={packet.get('seq')} ts={rtp_ts} "
            f"ssrc={packet.get('ssrc')} pt={packet.get('pt')} marker={packet.get('marker')} "
            f"payload={payload_len} pts_ms={pts_ms}"
        )
        if media == 'audio':
            self.stats_tracker.note_audio_rtp_packet(payload_len)
        elif media == 'video' and isinstance(payload_bytes, (bytes, bytearray)):
            self.stats_tracker.note_video_rtp_packet()
            emitted = self._nalu_logger.note_rtp_packet(bytes(payload_bytes), pts_ms)
            self.stats_tracker.note_video_nalus(emitted)

    def _decode_loop(self) -> None:
        process = None
        try:
            frame_width = self.video_spec.width
            frame_height = self.video_spec.height
            frame_size = frame_width * frame_height * 3 // 2
            stream = ffmpeg.output(
                ffmpeg.input('pipe:', format=self.video_spec.elementary_stream_format).filter('showinfo'),
                'pipe:',
                format='rawvideo',
                pix_fmt=self.pix_fmt,
                loglevel='info',
            )
            process = ffmpeg.run_async(stream, pipe_stdin=True, pipe_stdout=True, pipe_stderr=True, quiet=True)
            pts_queue: queue.Queue[int] = queue.Queue(maxsize=self.meta_queue_max_items)
            key_queue: queue.Queue[bool] = queue.Queue(maxsize=self.meta_queue_max_items)

            def feed_stdin() -> None:
                assert process.stdin is not None
                while True:
                    chunk = self._queue.get()
                    if chunk is None:
                        break
                    process.stdin.write(chunk)
                    process.stdin.flush()
                process.stdin.close()

            def read_stderr() -> None:
                assert process.stderr is not None
                while True:
                    line = process.stderr.readline()
                    if not line:
                        break
                    text = line.decode('utf-8', errors='ignore')
                    pts_match = SHOWINFO_PTS_RE.search(text)
                    if pts_match is not None:
                        try:
                            try:
                                pts_queue.put_nowait(int(float(pts_match.group(1)) * 1000))
                            except queue.Full:
                                pass
                        except ValueError:
                            try:
                                pts_queue.put_nowait(-1)
                            except queue.Full:
                                pass
                    key_match = SHOWINFO_ISKEY_RE.search(text)
                    if key_match is not None:
                        try:
                            key_queue.put_nowait(key_match.group(1) == '1')
                        except queue.Full:
                            pass

            feeder = threading.Thread(target=feed_stdin, daemon=True)
            stderr_reader = threading.Thread(target=read_stderr, daemon=True)
            feeder.start()
            stderr_reader.start()
            assert process.stdout is not None
            frame_index = 0
            while True:
                chunk = process.stdout.read(frame_size)
                if not chunk or len(chunk) != frame_size:
                    break
                try:
                    pts = pts_queue.get(timeout=1.0)
                except queue.Empty:
                    pts = frame_index
                try:
                    is_key = key_queue.get(timeout=0.2)
                except queue.Empty:
                    is_key = frame_index == 0
                self.sink.write_frame(pts, chunk, width=frame_width, height=frame_height, pix_fmt=self.pix_fmt)
                self.stats_tracker.note_decoded_frame(pts, is_key)
                frame_index += 1
            feeder.join(timeout=3)
            process.wait(timeout=5)
            stderr_reader.join(timeout=1)
            if process.returncode not in (0, None):
                raise RuntimeError(f'ffmpeg decode exited with returncode={process.returncode}')
        except BaseException as exc:
            self._decode_exc = exc
            self._stop_event.set()
        finally:
            if process is not None and process.poll() is None:
                process.kill()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='pull RTSP and write decoded YUV frames to shared memory')
    parser.add_argument('rtsp_url')
    parser.add_argument('--base-name', default='sniff_video_shm')
    parser.add_argument('--max-channels', type=int, default=4)
    parser.add_argument('--blocks-per-channel', type=int, default=15)
    parser.add_argument('--width', type=int, default=1920)
    parser.add_argument('--height', type=int, default=1080)
    parser.add_argument('--pix-fmt', default='nv12', choices=['nv12', 'yuv420p'])
    parser.add_argument('--reset-existing', action='store_true')
    parser.add_argument('--queue-max-chunks', type=int, default=400)
    parser.add_argument('--audio-queue-max-chunks', type=int, default=400)
    parser.add_argument('--meta-queue-max-items', type=int, default=400)
    parser.add_argument('--stats-interval', type=float, default=1.0)
    parser.add_argument('--rtp-log-path', default='video_rtsp_ingest_rtp.log')
    parser.add_argument('--nalu-log-path', default='video_rtsp_ingest_nalu.log')
    return parser


def _source_id_from_rtsp(rtsp_url: str) -> str:
    parsed = urlparse(rtsp_url)
    return parsed.hostname or rtsp_url


def _parse_adts_frame_length(data: bytes | bytearray) -> int | None:
    if len(data) < 7:
        return None
    if data[0] != 0xFF or (data[1] & 0xF0) != 0xF0:
        return None
    frame_length = ((data[3] & 0x03) << 11) | (data[4] << 3) | ((data[5] & 0xE0) >> 5)
    if frame_length < 7:
        return None
    return frame_length


def _probe_rtsp_spec(rtsp_url: str) -> RtspVideoSpec:
    probe = ffmpeg.probe(rtsp_url, rtsp_transport='tcp')
    video_streams = [stream for stream in probe['streams'] if stream.get('codec_type') == 'video']
    if not video_streams:
        raise RuntimeError(f'no video stream found in {rtsp_url!r}')
    stream = video_streams[0]
    audio_streams = [item for item in probe['streams'] if item.get('codec_type') == 'audio']
    audio_stream = audio_streams[0] if audio_streams else None
    return RtspVideoSpec(
        width=int(stream['width']),
        height=int(stream['height']),
        codec_name=str(stream.get('codec_name', '')).lower(),
        has_audio=audio_stream is not None,
        audio_codec_name=(str(audio_stream.get('codec_name', '')).lower() if audio_stream is not None else None),
    )


def decode_rtsp_stream_to_shm(
    rtsp_url: str,
    ring: SharedVideoRingBuffer,
    channel_id: int,
    source_id: str,
    *,
    pix_fmt: str = 'nv12',
    timeout: float | None = None,
    queue_max_chunks: int = 400,
    audio_queue_max_chunks: int = 400,
    meta_queue_max_items: int = 400,
    stats_interval: float = 1.0,
    rtp_log_path: str | None = 'video_rtsp_ingest_rtp.log',
    nalu_log_path: str | None = 'video_rtsp_ingest_nalu.log',
) -> ActivePullStats:
    pipeline = ActivePullPipeline(
        rtsp_url,
        ring,
        channel_id,
        source_id,
        pix_fmt=pix_fmt,
        timeout=timeout,
        queue_max_chunks=queue_max_chunks,
        audio_queue_max_chunks=audio_queue_max_chunks,
        meta_queue_max_items=meta_queue_max_items,
        stats_interval=stats_interval,
        rtp_log_path=rtp_log_path,
        nalu_log_path=nalu_log_path,
    )
    return pipeline.run()


def main() -> int:
    args = build_parser().parse_args()
    config = SharedVideoConfig(
        base_name=args.base_name,
        max_channels=args.max_channels,
        blocks_per_channel=args.blocks_per_channel,
        frame_width=args.width,
        frame_height=args.height,
        pix_fmt=args.pix_fmt,
    )
    # Always recreate/clear producer shared memory on startup to avoid
    # stale metadata/frames from previous ingest runs contaminating the new run.
    ring = SharedVideoRingBuffer.create(config, reset_existing=True)
    try:
        source_id = _source_id_from_rtsp(args.rtsp_url)
        channel_id = ring.ensure_channel(source_id)
        stats = decode_rtsp_stream_to_shm(
            args.rtsp_url,
            ring,
            channel_id,
            source_id,
            pix_fmt=args.pix_fmt,
            queue_max_chunks=args.queue_max_chunks,
            audio_queue_max_chunks=args.audio_queue_max_chunks,
            meta_queue_max_items=args.meta_queue_max_items,
            stats_interval=args.stats_interval,
            rtp_log_path=args.rtp_log_path,
            nalu_log_path=args.nalu_log_path,
        )
        return 0
    finally:
        ring.close(unlink=False)


if __name__ == '__main__':
    raise SystemExit(main())
