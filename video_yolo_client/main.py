#!/usr/bin/env python3
from __future__ import annotations

import argparse

from video_shm_core.shared_ring import SharedVideoRingBuffer
from .detector import UltralyticsYoloV8Detector
from .service import RoundRobinYoloService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='YOLOv8 CPU client on top of shared memory video')
    parser.add_argument('--base-name', default='sniff_video_shm')
    parser.add_argument('--model-path', default='yolov8n.pt')
    parser.add_argument('--iterations', type=int, default=0)
    parser.add_argument('--sleep-s', type=float, default=0.2)
    parser.add_argument('--log-dir', default='.')
    parser.add_argument('--input-size', type=int, default=640)
    parser.add_argument('--stats-interval-s', type=float, default=10.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        ring = SharedVideoRingBuffer.attach(args.base_name)
    except FileNotFoundError:
        print(
            '[video_yolo_client][error] shared memory not found for '
            f'base_name={args.base_name}. '
            'Start sniff/video_rtsp_ingest first, or confirm --base-name matches the producer.'
        )
        return 2
    try:
        detector = UltralyticsYoloV8Detector(model_path=args.model_path, input_size=args.input_size)
        service = RoundRobinYoloService(
            ring,
            detector,
            log_dir=args.log_dir,
            stats_interval_s=args.stats_interval_s,
        )
        print(f'[video_yolo_client] active channels={service.get_video_channel_count()}')
        service.run_loop(iterations=args.iterations, sleep_s=args.sleep_s)
        return 0
    finally:
        ring.close(unlink=False)


if __name__ == '__main__':
    raise SystemExit(main())
