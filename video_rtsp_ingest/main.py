#!/usr/bin/env python3
from __future__ import annotations

import argparse
from urllib.parse import urlparse

from video_shm_core.config import SharedVideoConfig
from video_shm_core.ffmpeg_decoder import decode_rtsp_to_shm
from video_shm_core.shared_ring import SharedVideoRingBuffer


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
    return parser


def _source_id_from_rtsp(rtsp_url: str) -> str:
    parsed = urlparse(rtsp_url)
    return parsed.hostname or rtsp_url


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
    ring = SharedVideoRingBuffer.create_or_attach(config, reset_existing=args.reset_existing)
    try:
        source_id = _source_id_from_rtsp(args.rtsp_url)
        channel_id = ring.ensure_channel(source_id)
        stats = decode_rtsp_to_shm(args.rtsp_url, ring, channel_id, source_id, pix_fmt=args.pix_fmt)
        print(
            f'[video_rtsp_ingest] source={source_id} channel={channel_id} '
            f'frames={stats.frames_written} last_pts={stats.last_pts}'
        )
        return 0
    finally:
        ring.close(unlink=False)


if __name__ == '__main__':
    raise SystemExit(main())
