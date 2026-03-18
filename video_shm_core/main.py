#!/usr/bin/env python3
from __future__ import annotations

import argparse

from .config import SharedVideoConfig
from .ffmpeg_decoder import decode_video_file_to_shm
from .shared_ring import SharedVideoRingBuffer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='shared memory video core')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_init = sub.add_parser('init', help='initialize shared memory as MASTER')
    p_init.add_argument('--base-name', default='sniff_video_shm')
    p_init.add_argument('--max-channels', type=int, default=4)
    p_init.add_argument('--blocks-per-channel', type=int, default=15)
    p_init.add_argument('--width', type=int, default=1920)
    p_init.add_argument('--height', type=int, default=1080)
    p_init.add_argument('--pix-fmt', default='nv12', choices=['nv12', 'yuv420p'])
    p_init.add_argument('--reset-existing', action='store_true')

    p_decode = sub.add_parser('decode-file', help='decode local file into shared memory')
    p_decode.add_argument('input_path')
    p_decode.add_argument('--base-name', default='sniff_video_shm')
    p_decode.add_argument('--source-id', default='local-file')
    p_decode.add_argument('--channel-id', type=int)
    p_decode.add_argument('--reset-existing', action='store_true')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.cmd == 'init':
        config = SharedVideoConfig(
            base_name=args.base_name,
            max_channels=args.max_channels,
            blocks_per_channel=args.blocks_per_channel,
            frame_width=args.width,
            frame_height=args.height,
            pix_fmt=args.pix_fmt,
        )
        ring = SharedVideoRingBuffer.create(config, reset_existing=args.reset_existing)
        print(f'[video_shm_core] initialized {config.base_name} with {config.max_channels} channels')
        ring.close(unlink=False)
        return 0

    ring = SharedVideoRingBuffer.attach(args.base_name)
    try:
        channel_id = args.channel_id
        if channel_id is None:
            channel_id = ring.ensure_channel(args.source_id)
        stats = decode_video_file_to_shm(args.input_path, ring, channel_id, args.source_id)
        print(
            f'[video_shm_core] channel={channel_id} frames={stats.frames_written} last_pts={stats.last_pts}'
        )
        return 0
    finally:
        ring.close(unlink=False)


if __name__ == '__main__':
    raise SystemExit(main())
