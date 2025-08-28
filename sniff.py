#!/usr/bin/env python3
from scapy.all import AsyncSniffer
import callbacks, rtp_parser, threading, time

IFACE = 'br0'

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

rtp_parser.set_rtp_callback(on_rtp)
rtp_parser.set_frame_callback(on_frame)
callbacks.rtp_callback = rtp_parser.process

sniffer = AsyncSniffer(iface=IFACE, prn=callbacks.dispatch,
                       filter='tcp or udp', store=False)
sniffer.start()
print(f"[+] sniffing on {IFACE}")

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
