from __future__ import annotations
from dataclasses import dataclass

PIX_FMT_NV12 = 1
PIX_FMT_YUV420P = 2

PIX_FMT_NAME_TO_CODE = {
    'nv12': PIX_FMT_NV12,
    'yuv420p': PIX_FMT_YUV420P,
}
PIX_FMT_CODE_TO_NAME = {v: k for k, v in PIX_FMT_NAME_TO_CODE.items()}


@dataclass(frozen=True)
class SharedVideoConfig:
    base_name: str = 'sniff_video_shm'
    max_channels: int = 4
    blocks_per_channel: int = 15
    frame_width: int = 1920
    frame_height: int = 1080
    pix_fmt: str = 'nv12'
    channel_label_bytes: int = 64

    @property
    def frame_size(self) -> int:
        return int(self.frame_width * self.frame_height * 3 // 2)

    @property
    def pix_fmt_code(self) -> int:
        try:
            return PIX_FMT_NAME_TO_CODE[self.pix_fmt]
        except KeyError as exc:
            raise ValueError(f'unsupported pix_fmt: {self.pix_fmt}') from exc
