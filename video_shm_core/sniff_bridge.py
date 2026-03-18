from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Dict

from .config import SharedVideoConfig
from .ffmpeg_decoder import AnnexBSharedMemoryWriter
from .shared_ring import SharedVideoRingBuffer


@dataclass
class _BridgeItem:
    codec: str
    writer: AnnexBSharedMemoryWriter


class SniffToSharedMemoryBridge:
    def __init__(
        self,
        *,
        base_name: str = 'sniff_video_shm',
        max_channels: int = 4,
        blocks_per_channel: int = 15,
        frame_width: int = 1920,
        frame_height: int = 1080,
        pix_fmt: str = 'nv12',
        reset_existing: bool = False,
    ) -> None:
        self.config = SharedVideoConfig(
            base_name=base_name,
            max_channels=max_channels,
            blocks_per_channel=blocks_per_channel,
            frame_width=frame_width,
            frame_height=frame_height,
            pix_fmt=pix_fmt,
        )
        self.ring = SharedVideoRingBuffer.create_or_attach(self.config, reset_existing=reset_existing)
        self._lock = threading.RLock()
        self._writers: Dict[str, _BridgeItem] = {}

    @staticmethod
    def _source_ip(frame: dict) -> str:
        flow = frame['flow']
        if getattr(flow, 'tag', '') == 'UDP-RTP':
            return flow.key[2]
        return flow.key[0]

    def _get_or_create_writer(self, source_id: str, codec: str) -> AnnexBSharedMemoryWriter:
        with self._lock:
            item = self._writers.get(source_id)
            if item is not None and item.codec == codec:
                return item.writer
            if item is not None:
                item.writer.close()
            channel_id = self.ring.ensure_channel(source_id)
            writer = AnnexBSharedMemoryWriter(
                ring=self.ring,
                channel_id=channel_id,
                source_id=source_id,
                codec=codec,
                frame_width=self.config.frame_width,
                frame_height=self.config.frame_height,
                pix_fmt=self.config.pix_fmt,
            )
            self._writers[source_id] = _BridgeItem(codec=codec, writer=writer)
            return writer

    def handle_frame(self, frame: dict) -> None:
        codec = frame.get('codec')
        if codec not in ('H264', 'H265'):
            return
        source_id = self._source_ip(frame)
        writer = self._get_or_create_writer(source_id, codec)
        writer.push_access_unit(frame['data'], int(frame['pts']))

    def close(self) -> None:
        with self._lock:
            for item in self._writers.values():
                item.writer.close()
            self._writers.clear()
            self.ring.close(unlink=False)
