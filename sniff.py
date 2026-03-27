#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, TextIO

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
    rtp_log_path: str
    frame_log_path: str
    stats_interval_s: float
    verbose: bool
    no_clear: bool


@dataclass
class SniffStreamStats:
    video_nalus: int = 0
    video_keyframes: int = 0
    audio_packets: int = 0
    video_normal: int = 0
    total: int = 0


class SniffStatsTracker:
    def __init__(self) -> None:
        self._stats: Dict[str, SniffStreamStats] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _stats_key(frame: dict) -> str:
        src_ip = frame.get('src_ip')
        if src_ip:
            return str(src_ip)
        flow = frame.get('flow')
        if flow is not None and getattr(flow, 'key', None):
            try:
                return str(flow.key[2])
            except Exception:
                return str(flow.key)
        return 'unknown'

    def note_frame(self, frame: dict) -> SniffStreamStats:
        stats_key = self._stats_key(frame)
        with self._lock:
            entry = self._stats.setdefault(stats_key, SniffStreamStats())
            entry.total += 1
            if frame.get('is_video', True):
                entry.video_nalus += 1
                if _is_keyframe(frame):
                    entry.video_keyframes += 1
                else:
                    entry.video_normal += 1
            else:
                entry.audio_packets += 1
            return SniffStreamStats(
                video_nalus=entry.video_nalus,
                video_keyframes=entry.video_keyframes,
                audio_packets=entry.audio_packets,
                video_normal=entry.video_normal,
                total=entry.total,
            )

    def snapshot_all(self) -> Dict[str, SniffStreamStats]:
        with self._lock:
            return {
                key: SniffStreamStats(
                    video_nalus=value.video_nalus,
                    video_keyframes=value.video_keyframes,
                    audio_packets=value.audio_packets,
                    video_normal=value.video_normal,
                    total=value.total,
                )
                for key, value in self._stats.items()
            }


class LineLogWriter:
    def __init__(self, path: str) -> None:
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._fp: TextIO = open(path, 'a', encoding='utf-8')
        self._lock = threading.Lock()

    def write_line(self, line: str) -> None:
        with self._lock:
            self._fp.write(line + '\n')
            self._fp.flush()

    def close(self) -> None:
        with self._lock:
            self._fp.close()


def _is_keyframe(frame: dict) -> bool:
    if not frame.get('is_video', True):
        return False
    codec = str(frame.get('codec', '')).upper()
    nalu_type = frame.get('nalu_type')
    if codec == 'H264':
        return nalu_type == 5
    if codec == 'H265':
        return nalu_type is not None and 16 <= nalu_type <= 21
    return False


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}



def resolve_local_ipv4s(iface: str) -> list[str]:
    ips: set[str] = set()
    try:
        from scapy.all import get_if_addr

        ip = get_if_addr(iface)
        if ip and ip != '0.0.0.0':
            ips.add(ip)
    except Exception:
        pass

    try:
        out = subprocess.check_output(
            ['ip', '-4', '-o', 'addr', 'show', 'dev', iface],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        for line in out.splitlines():
            parts = line.split()
            if 'inet' in parts:
                ip = parts[parts.index('inet') + 1].split('/')[0]
                if ip and ip != '0.0.0.0':
                    ips.add(ip)
    except Exception:
        pass
    return sorted(ips)


def build_bpf(local_ipv4s: Iterable[str]) -> str:
    parts = ['ip', '(tcp or udp)']
    for ip in local_ipv4s:
        parts.append(f'not dst host {ip}')
    return ' and '.join(parts)


def render_stats_table(
    stats_snapshot: Dict[str, SniffStreamStats],
    *,
    iface: str,
    local_ipv4s: list[str],
    no_clear: bool = False,
    printer=print,
) -> None:
    if not no_clear:
        printer('\033[2J\033[H', end='')

    now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    printer(f'[+] sniffing on {iface}')
    if local_ipv4s:
        printer(f'[+] local IPv4: {", ".join(local_ipv4s)} (已忽略目的地址为本机的报文)')
    else:
        printer('[!] 未获取到该网卡 IPv4，将不会按目的地址过滤本机报文')
    printer(f'[+] stats mode: cumulative since program start @ {now}')
    printer('-' * 88)
    printer(f"{'Source IP':<18} {'Total':>8} {'Video-Key':>12} {'Video-Normal':>14} {'Audio':>8}")
    printer('-' * 88)

    if not stats_snapshot:
        printer('(no NALU received since program start)')
        printer('-' * 88)
        return

    for source_ip in sorted(stats_snapshot):
        stats = stats_snapshot[source_ip]
        printer(
            f"{source_ip:<18} {stats.total:>8} {stats.video_keyframes:>12} "
            f"{stats.video_normal:>14} {stats.audio_packets:>8}"
        )
    printer('-' * 88)

def format_stats_line(flow_key: str, stats: SniffStreamStats, *, final: bool = False) -> str:
    tag = 'final' if final else 'stats'
    return (
        f'[sniff][{tag}] flow={flow_key} '
        f'video_keyframes={stats.video_keyframes} '
        f'video_nalus={stats.video_nalus} '
        f'audio_packets={stats.audio_packets}'
    )


def emit_all_stats(stats_tracker: SniffStatsTracker, *, final: bool = False, printer=print) -> None:
    for flow_key, stats in stats_tracker.snapshot_all().items():
        printer(format_stats_line(flow_key, stats, final=final))


def format_frame_line(frame: dict, stats: SniffStreamStats) -> str:
    flow_key = frame['flow'].key
    if not frame.get('is_video', True):
        return (
            f"[AUD ] pts={frame['pts']:10} {frame['codec']:6} "
            f"size={len(frame['data']):>6} packets={stats.audio_packets:>6} "
            f"{flow_key}"
        )
    if frame.get('nalu_type') is not None:
        return (
            f"[NAL ] pts={frame['pts']:10} {frame['codec']:6} "
            f"nalu={frame['nalu_type']:^2} size={len(frame['data']):>6} total={stats.video_nalus:>6} "
            f"keyframes={stats.video_keyframes:>6} {flow_key}"
        )
    return (
        f"[NAL ] pts={frame['pts']:10} {frame['codec']:6} "
        f"size={len(frame['data']):>6} total={stats.video_nalus:>6} "
        f"keyframes={stats.video_keyframes:>6} {flow_key}"
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='RTSP/RTP sniff statistics with optional shared-memory decode output.')
    parser.add_argument('-i', '--iface', default=os.getenv('SNIFF_IFACE', 'br0'), help='抓包网卡名')

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
        help='保留参数兼容性；当前实现始终在启动时重建共享内存。',
    )
    parser.add_argument('--rtp-log-path', default=os.getenv('SNIFF_RTP_LOG_PATH', 'sniff_rtp.log'))
    parser.add_argument('--frame-log-path', default=os.getenv('SNIFF_FRAME_LOG_PATH', 'sniff_frames.log'))
    parser.add_argument('--stats-interval-s', type=float, default=float(os.getenv('SNIFF_STATS_INTERVAL_S', '1')))
    parser.add_argument('-v', '--verbose', action='store_true', default=_env_bool('SNIFF_VERBOSE', False), help='打印逐包 RTP / NAL 明细')
    parser.add_argument('--no-clear', action='store_true', default=_env_bool('SNIFF_NO_CLEAR', False), help='每秒刷新统计时不清屏')
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
        shm_reset_on_boot=True,
        rtp_log_path=args.rtp_log_path,
        frame_log_path=args.frame_log_path,
        stats_interval_s=args.stats_interval_s,
        verbose=bool(args.verbose),
        no_clear=bool(args.no_clear),
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
        reset_existing=True,
    )


def run(argv: list[str] | None = None) -> int:
    from scapy.all import AsyncSniffer

    import callbacks
    import rtp_parser

    config = parse_runtime_config(argv)
    local_ipv4s = resolve_local_ipv4s(config.iface)
    bpf = build_bpf(local_ipv4s)
    shm_bridge = create_shm_bridge(config)
    stats_tracker = SniffStatsTracker()
    rtp_log_writer = LineLogWriter(config.rtp_log_path)
    frame_log_writer = LineLogWriter(config.frame_log_path)
    stop_event = threading.Event()

    def on_rtp(i):
        rtp_line = (
            f"[RTP ] pts={i['pts']:10} {i['codec']:6} "
            f"frag={i['frag']:<4} size={i['size']:>5} "
            f"src={i.get('src_ip') or '-':<15} {i['flow'].key}"
        )
        if config.verbose:
            print(rtp_line)
        rtp_log_writer.write_line(rtp_line)

    def on_frame(f):
        snap = stats_tracker.note_frame(f)
        frame_line = format_frame_line(f, snap)
        if config.verbose:
            print(frame_line)
        frame_log_writer.write_line(frame_line)

        if shm_bridge is None:
            return
        try:
            shm_bridge.handle_frame(f)
        except Exception as exc:
            print(f'[WARN] shared-memory bridge handle_frame failed: {exc}')

    def stats_loop():
        while not stop_event.wait(config.stats_interval_s):
            render_stats_table(
                stats_tracker.snapshot_all(),
                iface=config.iface,
                local_ipv4s=local_ipv4s,
                no_clear=config.no_clear,
            )

    rtp_parser.set_rtp_callback(on_rtp)
    rtp_parser.set_frame_callback(on_frame)
    callbacks.rtp_callback = rtp_parser.process
    if hasattr(callbacks, 'set_local_ipv4s'):
        callbacks.set_local_ipv4s(local_ipv4s)

    sniffer = AsyncSniffer(iface=config.iface, prn=callbacks.dispatch, filter=bpf, store=False)
    sniffer.start()
    render_stats_table({}, iface=config.iface, local_ipv4s=local_ipv4s, no_clear=config.no_clear)
    print(f'[+] rtp log path: {config.rtp_log_path}')
    print(f'[+] frame log path: {config.frame_log_path}')
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
    threading.Thread(target=stats_loop, daemon=True).start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sniffer.stop()
    finally:
        stop_event.set()
        render_stats_table(
            stats_tracker.snapshot_all(),
            iface=config.iface,
            local_ipv4s=local_ipv4s,
            no_clear=True,
        )
        emit_all_stats(stats_tracker, final=True)
        rtp_log_writer.close()
        frame_log_writer.close()
        if shm_bridge is not None:
            try:
                shm_bridge.close()
            except Exception as exc:
                print(f'[WARN] shared-memory bridge close failed: {exc}')
    return 0


if __name__ == '__main__':
    raise SystemExit(run())
