from __future__ import annotations

import contextlib
import fcntl
import os
import struct
from dataclasses import dataclass
from multiprocessing import shared_memory
from pathlib import Path
from typing import Iterable, Optional

from .config import (
    PIX_FMT_CODE_TO_NAME,
    PIX_FMT_NAME_TO_CODE,
    SharedVideoConfig,
)

MAGIC = b'SNIFFSHM'
VERSION = 1
STATUS_EMPTY = 0
STATUS_DECODING = 1
STATUS_COMPLETE = 2

_HEADER_STRUCT = struct.Struct('<8sIIIIIII')
_CHANNEL_STRUCT = struct.Struct('<IIQ64s48s')
_BLOCK_STRUCT = struct.Struct('<IIIIIIIQq32s')

HEADER_SIZE = _HEADER_STRUCT.size
CHANNEL_SIZE = _CHANNEL_STRUCT.size
BLOCK_SIZE_META = _BLOCK_STRUCT.size
LOCK_DIR = Path('/tmp/sniff_video_shm_locks')
LOCK_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ChannelRecord:
    channel_id: int
    active: bool
    next_block: int
    write_seq: int
    source_id: str


@dataclass
class FrameRecord:
    channel_id: int
    block_index: int
    status: int
    width: int
    height: int
    data_len: int
    pix_fmt: str
    write_seq: int
    pts: int
    source_id: str
    data: bytes


class FrameWriteHandle:
    def __init__(
        self,
        ring: 'SharedVideoRingBuffer',
        channel_id: int,
        block_index: int,
        write_seq: int,
        pts: int,
        width: int,
        height: int,
        pix_fmt: str,
        source_id: str,
        lock_fd: int,
    ) -> None:
        self._ring = ring
        self.channel_id = channel_id
        self.block_index = block_index
        self.write_seq = write_seq
        self.pts = pts
        self.width = width
        self.height = height
        self.pix_fmt = pix_fmt
        self.source_id = source_id
        self._lock_fd = lock_fd
        self._closed = False

    def write(self, frame_bytes: bytes) -> None:
        if self._closed:
            raise RuntimeError('frame handle already closed')
        if len(frame_bytes) > self._ring.config.frame_size:
            raise ValueError(
                f'frame bytes too large for shared buffer: {len(frame_bytes)} > {self._ring.config.frame_size}'
            )
        self._ring._write_block_payload(self.channel_id, self.block_index, frame_bytes)
        self._ring._update_block_meta(
            self.channel_id,
            self.block_index,
            status=STATUS_DECODING,
            width=self.width,
            height=self.height,
            data_len=len(frame_bytes),
            pix_fmt=self.pix_fmt,
            write_seq=self.write_seq,
            pts=self.pts,
        )

    def commit(self) -> None:
        if self._closed:
            raise RuntimeError('frame handle already closed')
        lock_fd = self._ring._acquire_lock(f'channel_{self.channel_id}')
        try:
            self._ring._update_block_meta(
                self.channel_id,
                self.block_index,
                status=STATUS_COMPLETE,
                width=self.width,
                height=self.height,
                data_len=None,
                pix_fmt=self.pix_fmt,
                write_seq=self.write_seq,
                pts=self.pts,
            )
            self._ring._update_channel(
                self.channel_id,
                active=True,
                next_block=(self.block_index + 1) % self._ring.config.blocks_per_channel,
                write_seq=self.write_seq,
                source_id=self.source_id,
            )
        finally:
            self._ring._release_lock(lock_fd)
        self.close()

    def cancel(self) -> None:
        if self._closed:
            return
        lock_fd = self._ring._acquire_lock(f'channel_{self.channel_id}')
        try:
            self._ring._update_block_meta(
                self.channel_id,
                self.block_index,
                status=STATUS_EMPTY,
                width=self.width,
                height=self.height,
                data_len=0,
                pix_fmt=self.pix_fmt,
                write_seq=self.write_seq,
                pts=self.pts,
            )
        finally:
            self._ring._release_lock(lock_fd)
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        if self._lock_fd >= 0:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)
        self._closed = True

    def __enter__(self) -> 'FrameWriteHandle':
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is None:
            self.close()
        else:
            self.cancel()


class SharedVideoRingBuffer:
    def __init__(
        self,
        config: SharedVideoConfig,
        meta_shm: shared_memory.SharedMemory,
        data_shm: shared_memory.SharedMemory,
        owner: bool = False,
        validate_header: bool = True,
    ) -> None:
        self.config = config
        self._meta_shm = meta_shm
        self._data_shm = data_shm
        self._owner = owner
        self._meta = meta_shm.buf
        self._data = data_shm.buf
        if validate_header:
            self._validate_header()

    @classmethod
    def create(cls, config: SharedVideoConfig, reset_existing: bool = False) -> 'SharedVideoRingBuffer':
        meta_name = f'{config.base_name}_meta'
        data_name = f'{config.base_name}_data'
        meta_size = cls._meta_size(config)
        data_size = cls._data_size(config)
        if reset_existing:
            for name in (meta_name, data_name):
                with contextlib.suppress(FileNotFoundError):
                    shm = shared_memory.SharedMemory(name=name)
                    shm.close()
                    shm.unlink()
        meta_shm = shared_memory.SharedMemory(name=meta_name, create=True, size=meta_size)
        data_shm = shared_memory.SharedMemory(name=data_name, create=True, size=data_size)
        ring = cls(config=config, meta_shm=meta_shm, data_shm=data_shm, owner=True, validate_header=False)
        ring._init_memory()
        return ring


    @classmethod
    def create_or_attach(cls, config: SharedVideoConfig, reset_existing: bool = False) -> 'SharedVideoRingBuffer':
        if reset_existing:
            return cls.create(config, reset_existing=True)
        try:
            return cls.attach(config.base_name)
        except FileNotFoundError:
            return cls.create(config, reset_existing=False)

    @classmethod
    def attach(cls, base_name: str) -> 'SharedVideoRingBuffer':
        meta_shm = shared_memory.SharedMemory(name=f'{base_name}_meta', create=False)
        data_shm = shared_memory.SharedMemory(name=f'{base_name}_data', create=False)
        config = cls._read_config(meta_shm.buf, base_name=base_name)
        return cls(config=config, meta_shm=meta_shm, data_shm=data_shm, owner=False)

    @staticmethod
    def _meta_size(config: SharedVideoConfig) -> int:
        total_blocks = config.max_channels * config.blocks_per_channel
        return HEADER_SIZE + config.max_channels * CHANNEL_SIZE + total_blocks * BLOCK_SIZE_META

    @staticmethod
    def _data_size(config: SharedVideoConfig) -> int:
        total_blocks = config.max_channels * config.blocks_per_channel
        return total_blocks * config.frame_size

    @classmethod
    def _read_config(cls, meta: memoryview, base_name: str) -> SharedVideoConfig:
        magic, version, max_channels, blocks_per_channel, frame_width, frame_height, pix_fmt_code, label_bytes = _HEADER_STRUCT.unpack_from(meta, 0)
        if magic != MAGIC:
            raise ValueError('invalid shared memory magic')
        if version != VERSION:
            raise ValueError(f'unsupported shared memory version: {version}')
        return SharedVideoConfig(
            max_channels=max_channels,
            blocks_per_channel=blocks_per_channel,
            frame_width=frame_width,
            frame_height=frame_height,
            pix_fmt=PIX_FMT_CODE_TO_NAME[pix_fmt_code],
            channel_label_bytes=label_bytes,
            base_name=base_name,
        )

    def _validate_header(self) -> None:
        if self._meta[:8].tobytes() != MAGIC:
            raise ValueError('shared memory is not initialized by master')

    def _init_memory(self) -> None:
        self._meta[:] = b'\x00' * len(self._meta)
        self._data[:] = b'\x00' * len(self._data)
        _HEADER_STRUCT.pack_into(
            self._meta,
            0,
            MAGIC,
            VERSION,
            self.config.max_channels,
            self.config.blocks_per_channel,
            self.config.frame_width,
            self.config.frame_height,
            self.config.pix_fmt_code,
            self.config.channel_label_bytes,
        )
        for channel_id in range(self.config.max_channels):
            self._update_channel(channel_id, active=False, next_block=0, write_seq=0, source_id='')
        for channel_id in range(self.config.max_channels):
            for block_index in range(self.config.blocks_per_channel):
                self._update_block_meta(
                    channel_id,
                    block_index,
                    status=STATUS_EMPTY,
                    width=self.config.frame_width,
                    height=self.config.frame_height,
                    data_len=0,
                    pix_fmt=self.config.pix_fmt,
                    write_seq=0,
                    pts=-1,
                )

    def close(self, unlink: bool = False) -> None:
        self._meta.release()
        self._data.release()
        self._meta_shm.close()
        self._data_shm.close()
        if unlink and self._owner:
            with contextlib.suppress(FileNotFoundError):
                self._meta_shm.unlink()
            with contextlib.suppress(FileNotFoundError):
                self._data_shm.unlink()

    def _channel_offset(self, channel_id: int) -> int:
        self._ensure_channel_id(channel_id)
        return HEADER_SIZE + channel_id * CHANNEL_SIZE

    def _block_meta_offset(self, channel_id: int, block_index: int) -> int:
        self._ensure_channel_id(channel_id)
        if not 0 <= block_index < self.config.blocks_per_channel:
            raise IndexError(f'block_index out of range: {block_index}')
        total_before = channel_id * self.config.blocks_per_channel + block_index
        return HEADER_SIZE + self.config.max_channels * CHANNEL_SIZE + total_before * BLOCK_SIZE_META

    def _data_offset(self, channel_id: int, block_index: int) -> int:
        total_before = channel_id * self.config.blocks_per_channel + block_index
        return total_before * self.config.frame_size

    def _ensure_channel_id(self, channel_id: int) -> None:
        if not 0 <= channel_id < self.config.max_channels:
            raise IndexError(f'channel_id out of range: {channel_id}')

    def _lock_path(self, scope: str) -> Path:
        safe_base_name = (self.config.base_name or 'sniff_video_shm').replace('/', '_')
        return LOCK_DIR / f'{safe_base_name}_{scope}.lock'

    def _acquire_lock(self, scope: str) -> int:
        path = self._lock_path(scope)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o666)
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd

    def _release_lock(self, fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def _read_channel(self, channel_id: int) -> ChannelRecord:
        active, next_block, write_seq, source_id_raw, _ = _CHANNEL_STRUCT.unpack_from(self._meta, self._channel_offset(channel_id))
        source_id = source_id_raw.split(b'\x00', 1)[0].decode('utf-8', errors='ignore')
        return ChannelRecord(
            channel_id=channel_id,
            active=bool(active),
            next_block=next_block,
            write_seq=write_seq,
            source_id=source_id,
        )

    def _update_channel(self, channel_id: int, active: bool, next_block: int, write_seq: int, source_id: str) -> None:
        source_id_raw = source_id.encode('utf-8')[:64].ljust(64, b'\x00')
        _CHANNEL_STRUCT.pack_into(
            self._meta,
            self._channel_offset(channel_id),
            int(active),
            next_block,
            write_seq,
            source_id_raw,
            b'\x00' * 48,
        )

    def _read_block_meta(self, channel_id: int, block_index: int) -> tuple[int, int, int, int, int, int, int, int, int]:
        status, ch, blk, width, height, data_len, pix_fmt_code, write_seq, pts, _ = _BLOCK_STRUCT.unpack_from(
            self._meta,
            self._block_meta_offset(channel_id, block_index),
        )
        return status, ch, blk, width, height, data_len, pix_fmt_code, write_seq, pts

    def _update_block_meta(
        self,
        channel_id: int,
        block_index: int,
        *,
        status: int,
        width: int,
        height: int,
        data_len: Optional[int],
        pix_fmt: str,
        write_seq: int,
        pts: int,
    ) -> None:
        current = self._read_block_meta(channel_id, block_index)
        final_data_len = current[5] if data_len is None else data_len
        _BLOCK_STRUCT.pack_into(
            self._meta,
            self._block_meta_offset(channel_id, block_index),
            status,
            channel_id,
            block_index,
            width,
            height,
            final_data_len,
            PIX_FMT_NAME_TO_CODE[pix_fmt],
            write_seq,
            pts,
            b'\x00' * 32,
        )

    def _write_block_payload(self, channel_id: int, block_index: int, payload: bytes) -> None:
        start = self._data_offset(channel_id, block_index)
        end = start + len(payload)
        self._data[start:end] = payload
        block_end = start + self.config.frame_size
        if end < block_end:
            self._data[end:block_end] = b'\x00' * (block_end - end)

    def ensure_channel(self, source_id: str) -> int:
        lock_fd = self._acquire_lock('global')
        try:
            for channel_id in range(self.config.max_channels):
                channel = self._read_channel(channel_id)
                if channel.active and channel.source_id == source_id:
                    return channel_id
            for channel_id in range(self.config.max_channels):
                channel = self._read_channel(channel_id)
                if not channel.active:
                    self._update_channel(channel_id, active=True, next_block=0, write_seq=0, source_id=source_id)
                    return channel_id
        finally:
            self._release_lock(lock_fd)
        raise RuntimeError(f'no free channel available for source_id={source_id!r}')

    def list_channels(self) -> list[ChannelRecord]:
        result = []
        for channel_id in range(self.config.max_channels):
            record = self._read_channel(channel_id)
            if record.active:
                result.append(record)
        return result

    def get_active_channel_count(self) -> int:
        return len(self.list_channels())

    def begin_frame_write(
        self,
        channel_id: int,
        pts: int,
        *,
        width: Optional[int] = None,
        height: Optional[int] = None,
        pix_fmt: Optional[str] = None,
        source_id: str = '',
    ) -> FrameWriteHandle:
        width = width or self.config.frame_width
        height = height or self.config.frame_height
        pix_fmt = pix_fmt or self.config.pix_fmt
        lock_fd = self._acquire_lock(f'channel_{channel_id}')
        channel = self._read_channel(channel_id)
        block_index = channel.next_block % self.config.blocks_per_channel
        write_seq = channel.write_seq + 1
        effective_source = source_id or channel.source_id or f'channel-{channel_id}'
        self._update_channel(channel_id, active=True, next_block=block_index, write_seq=channel.write_seq, source_id=effective_source)
        self._update_block_meta(
            channel_id,
            block_index,
            status=STATUS_DECODING,
            width=width,
            height=height,
            data_len=0,
            pix_fmt=pix_fmt,
            write_seq=write_seq,
            pts=pts,
        )
        self._release_lock(lock_fd)
        return FrameWriteHandle(
            ring=self,
            channel_id=channel_id,
            block_index=block_index,
            write_seq=write_seq,
            pts=pts,
            width=width,
            height=height,
            pix_fmt=pix_fmt,
            source_id=effective_source,
            lock_fd=-1,
        )

    def write_frame(
        self,
        channel_id: int,
        pts: int,
        frame_bytes: bytes,
        *,
        width: Optional[int] = None,
        height: Optional[int] = None,
        pix_fmt: Optional[str] = None,
        source_id: str = '',
    ) -> FrameRecord:
        with self.begin_frame_write(
            channel_id,
            pts,
            width=width,
            height=height,
            pix_fmt=pix_fmt,
            source_id=source_id,
        ) as handle:
            handle.write(frame_bytes)
            handle.commit()
        frame = self.read_latest_frame(channel_id)
        if frame is None:
            raise RuntimeError('frame write completed but no readable frame found')
        return frame

    def read_latest_frame(self, channel_id: int) -> Optional[FrameRecord]:
        lock_fd = self._acquire_lock(f'channel_{channel_id}')
        try:
            channel = self._read_channel(channel_id)
            if not channel.active:
                return None
            winner: Optional[tuple[int, int, int, int, int, int, int]] = None
            for block_index in range(self.config.blocks_per_channel):
                status, _ch, _blk, width, height, data_len, pix_fmt_code, write_seq, pts = self._read_block_meta(channel_id, block_index)
                if status != STATUS_COMPLETE:
                    continue
                if winner is None or write_seq > winner[0]:
                    winner = (write_seq, block_index, width, height, data_len, pix_fmt_code, pts)
            if winner is None:
                return None
            write_seq, block_index, width, height, data_len, pix_fmt_code, pts = winner
            start = self._data_offset(channel_id, block_index)
            end = start + data_len
            payload = bytes(self._data[start:end])
            return FrameRecord(
                channel_id=channel_id,
                block_index=block_index,
                status=STATUS_COMPLETE,
                width=width,
                height=height,
                data_len=data_len,
                pix_fmt=PIX_FMT_CODE_TO_NAME[pix_fmt_code],
                write_seq=write_seq,
                pts=pts,
                source_id=channel.source_id,
                data=payload,
            )
        finally:
            self._release_lock(lock_fd)

    def read_latest_frames(self, channel_ids: Iterable[int]) -> list[FrameRecord]:
        frames = []
        for channel_id in channel_ids:
            frame = self.read_latest_frame(channel_id)
            if frame is not None:
                frames.append(frame)
        return frames
