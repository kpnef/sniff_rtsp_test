#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

from video_shm_core import SniffToSharedMemoryBridge


@dataclass(frozen=True)
class SniffRuntimeConfig:
    iface: str
    write_shm: bool
    shm_base_name: str
    shm_max_channels: int
    shm_blocks_per_channel: int
    shm_frame_width: int
    shm_frame_height: int
    shm_pix_fmt: str
    shm_reset_on_boot: bool


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='RTSP/RTP sniff statistics with optional shared-memory decode output.')
    parser.add_argument('--iface', default=os.getenv('SNIFF_IFACE', 'br0'), help='抓包网卡名')

    default_write_shm = _env_bool('SNIFF_WRITE_SHM', False)
    parser.add_argument(
        '--write-shm',
        dest='write_shm',
        action='store_true',
        default=default_write_shm,
        help='开启共享内存写入：嗅探到的视频会解码后写入共享内存，供后续 YOLO 等子工程读取。',
    )
    parser.add_argument(
        '--stats-only',
        dest='write_shm',
        action='store_false',
        help='仅做嗅探统计，不初始化共享内存，也不写入共享内存。',
    )

    parser.add_argument('--shm-base-name', default=os.getenv('SNIFF_SHM_BASE_NAME', 'sniff_video_shm'))
    parser.add_argument('--shm-max-channels', type=int, default=int(os.getenv('SNIFF_SHM_MAX_CHANNELS', '4')))
    parser.add_argument('--shm-blocks-per-channel', type=int, default=int(os.getenv('SNIFF_SHM_BLOCKS_PER_CHANNEL', '15')))
    parser.add_argument('--shm-frame-width', type=int, default=int(os.getenv('SNIFF_SHM_FRAME_WIDTH', '1920')))
    parser.add_argument('--shm-frame-height', type=int, default=int(os.getenv('SNIFF_SHM_FRAME_HEIGHT', '1080')))
    parser.add_argument('--shm-pix-fmt', default=os.getenv('SNIFF_SHM_PIX_FMT', 'nv12'))
    parser.add_argument(
        '--shm-reset-on-boot',
        action='store_true',
        default=_env_bool('SNIFF_SHM_RESET_ON_BOOT', False),
        help='启动时重建共享内存。',
    )
    return parser


def parse_runtime_config(argv: list[str] | None = None) -> SniffRuntimeConfig:
    args = build_arg_parser().parse_args(argv)
    return SniffRuntimeConfig(
        iface=args.iface,
        write_shm=bool(args.write_shm),
        shm_base_name=args.shm_base_name,
        shm_max_channels=args.shm_max_channels,
        shm_blocks_per_channel=args.shm_blocks_per_channel,
        shm_frame_width=args.shm_frame_width,
        shm_frame_height=args.shm_frame_height,
        shm_pix_fmt=args.shm_pix_fmt,
        shm_reset_on_boot=bool(args.shm_reset_on_boot),
    )


def create_shm_bridge(config: SniffRuntimeConfig) -> Optional[SniffToSharedMemoryBridge]:
    if not config.write_shm:
        return None
    return SniffToSharedMemoryBridge(
        base_name=config.shm_base_name,
        max_channels=config.shm_max_channels,
        blocks_per_channel=config.shm_blocks_per_channel,
        frame_width=config.shm_frame_width,
        frame_height=config.shm_frame_height,
        pix_fmt=config.shm_pix_fmt,
        reset_existing=config.shm_reset_on_boot,
    )


def run(argv: list[str] | None = None) -> int:
    from scapy.all import AsyncSniffer

    import callbacks
    import rtp_parser

    config = parse_runtime_config(argv)
    shm_bridge = create_shm_bridge(config)

    def on_rtp(i):
        print(f"[RTP ] pts={i['pts']:10} {i['codec']:6} "
              f"frag={i['frag']:<4} size={i['size']:>5} "
              f"{i['flow'].key}")

    def on_frame(f):
        if f['nalu_type'] is not None:
            print(f"[NAL ] pts={f['pts']:10} {f['codec']:6} "
                  f"nalu={f['nalu_type']:^2} size={len(f['data']):>6} "
                  f"{f['flow'].key}")
        else:
            print(f"[NAL ] pts={f['pts']:10} {f['codec']:6} "
                  f"size={len(f['data']):>6} "
                  f"{f['flow'].key}")

        if shm_bridge is None:
            return
        try:
            shm_bridge.handle_frame(f)
        except Exception as exc:
            print(f'[WARN] shared-memory bridge handle_frame failed: {exc}')

    rtp_parser.set_rtp_callback(on_rtp)
    rtp_parser.set_frame_callback(on_frame)
    callbacks.rtp_callback = rtp_parser.process

    sniffer = AsyncSniffer(iface=config.iface, prn=callbacks.dispatch,
                           filter='tcp or udp', store=False)
    sniffer.start()
    print(f'[+] sniffing on {config.iface}')
    if shm_bridge is None:
        print('[+] shared memory disabled: statistics-only mode')
    else:
        print(
            f'[+] shared memory enabled base={config.shm_base_name} '
            f'channels={config.shm_max_channels} blocks={config.shm_blocks_per_channel} '
            f'size={config.shm_frame_width}x{config.shm_frame_height} fmt={config.shm_pix_fmt}'
        )

    def gc_loop():
        while True:
            callbacks.gc()
            time.sleep(1)

    threading.Thread(target=gc_loop, daemon=True).start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sniffer.stop()
    finally:
        if shm_bridge is not None:
            try:
                shm_bridge.close()
            except Exception as exc:
                print(f'[WARN] shared-memory bridge close failed: {exc}')
    return 0


if __name__ == '__main__':
    raise SystemExit(run())
