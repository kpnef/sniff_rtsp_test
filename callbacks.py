#!/usr/bin/env python3
from __future__ import annotations
import time, re, threading, struct, urllib.parse
from dataclasses import dataclass, field
from typing import Dict, Tuple, Literal, Optional
from scapy.layers.inet import IP, TCP, UDP
from scapy.packet import Raw, Packet
import traceback 

# ─── 对外覆写钩子 ─────────────────────────────────────────────
def rtp_callback(rtp_bytes: bytes, flow: 'FlowEntry', channel: int | None):
    pass

# ─── 基本类型 ─────────────────────────────────────────────────
Key   = Tuple[str, int, str, int, str]
Tag   = Literal['Invalid', 'UDP-RTP', 'TCP-RTSP', 'TCP-Mux']
State = Literal['IDLE', 'PLAY', 'PAUSED', 'CLOSED']

@dataclass
class FlowEntry:
    key: Key
    tag: Tag        = 'Invalid'
    sess_id: Optional[str] = None
    last_ts: float  = field(default_factory=time.time)
    tcp_buf: bytearray     = field(default_factory=bytearray)
    req_map: dict[int,dict] = field(default_factory=dict)   # CSeq → dict

@dataclass
class RtspSession:
    sid: str
    state: State            = 'IDLE'
    last_act: float         = field(default_factory=time.time)
    ctrl_flow_key: Optional[Key]=None


# ─── 全局表 ───────────────────────────────────────────────────
FLOW_TTL, IDLE_TTL, PLAY_TTL = 60, 30, 300
_ft : Dict[Key, FlowEntry]   = {}
_rst: Dict[str, RtspSession] = {}
_lock = threading.RLock()

# ─── 正则 ────────────────────────────────────────────────────
RTSP_MIN_RE  = re.compile(rb'RTSP/\d\.\d.*?CSeq:', re.S)
SESSION_RE   = re.compile(r'Session:\s*([0-9A-Za-z]+)')
CSEQ_RE      = re.compile(r'CSeq:\s*(\d+)', re.I)

CLIENT_RE     = re.compile(
    r'client_port\s*=\s*'               # group(1) = client / server
    r'(\d+)'                            # group(2) = RTP 端口
    r'(?:-(\d+))?'                      # group(3) = 可选 RTCP 端口
, re.I)

CONTENTLEN_RE= re.compile(r'Content-Length:\s*(\d+)', re.I)


# ——— Transport 解析 ————————————————————————————————————
def _parse_transport(s:str)->dict:
    part = s.split(',')[0]
    out  = {}
    for seg in part.split(';'):
        if '=' in seg:
            k,v = seg.split('=',1); out[k.lower().strip()] = v.strip()
        else:
            out[seg.lower().strip()] = ''
    return out


# ─── 五元组工具 ──────────────────────────────────────────────
def five_tuple(pkt: Packet) -> Key:
    l4, proto = (pkt[TCP], 'TCP') if pkt.haslayer(TCP) else (pkt[UDP], 'UDP')
    return (pkt[IP].dst, l4.dport, pkt[IP].src, l4.sport, proto)

def _rev(k: Key) -> Key: return (k[2], k[3], k[0], k[1], k[4])

def add_ft(key: Key, tag: Tag, sid=None):
    fe = FlowEntry(key, tag, sid)
    _ft[key] = fe; _ft[_rev(key)] = fe
    print("======>add flow :",key)
    return fe

def lookup_ft(key): return _ft.get(key) or _ft.get(_rev(key))
def remove_ft(k):   print("del flow:");print(k);_ft.pop(k, None); _ft.pop(_rev(k), None)

# ─── 失效统一出口 ────────────────────────────────────────────
def _remove_session(sid:str):
    print("_remove_session")
    print(sid)
    for k, fe in list(_ft.items()):
        if fe.sess_id == sid: remove_ft(k)
    _rst.pop(sid, None)

def invalidate_flow(flow: FlowEntry, why='err'):
    print("==>invalidate_flow")
    print(f"[-] invalidate {flow.key} ({why})")
    traceback.print_stack()
    if flow.sess_id: _remove_session(flow.sess_id)
    else:            remove_ft(flow.key)


# ─── RTSP 报文提取（跨包）────────────────────────────────────
def _extract_rtsp(buf:bytearray)->bytes|None:
    hdr_end = buf.find(b'\r\n\r\n')
    if hdr_end == -1: return None
    hdr = buf[:hdr_end+4]
    m = CONTENTLEN_RE.search(hdr.decode(errors='ignore'))
    need = int(m.group(1)) if m else 0
    total = hdr_end + 4 + need
    if len(buf) < total: return None
    msg = bytes(buf[:total]); del buf[:total]
    return msg

# ─── 主 RTSP 解析 ───────────────────────────────────────────
def _process_rtsp(msg:bytes, fe:FlowEntry):
    txt = msg.decode(errors='ignore')
    # ——— 通用字段 ———
    m_cseq = CSEQ_RE.search(txt); cseq = int(m_cseq.group(1)) if m_cseq else None
    is_req = not txt.startswith('RTSP/')
    first  = txt.split(' ',1)[0]


    # 3) Session-ID
    m_sid = SESSION_RE.search(txt)
    if m_sid:
        fe.sess_id = sid = m_sid.group(1)
        _rst.setdefault(sid, RtspSession(sid=sid,
                                         ctrl_flow_key=fe.key))

    # 4) SETUP 200——建立/更新 RTSP Session
    if (not is_req) and '200 OK' in txt and 'Transport' in txt:
        sid = fe.sess_id
        if not sid: invalidate_flow(fe,'SETUP no sid'); return
        sess = _rst[sid]
         # 解析响应中的 Transport（拿到 server_port / interleaved）
        m_tr = re.search(r'Transport:\s*([^\r\n]+)', txt, re.I)
        if not (m_tr  is not None):
            invalidate_flow(fe,'SETUP miss field'); return
        rsp_t = _parse_transport(m_tr.group(1))
        # 合并 request / response Transport 信息
        tinfo = {**rsp_t}
        #print(tinfo)

        if 'interleaved' in tinfo:                           # RTP over TCP
            ch = int(tinfo['interleaved'].split('-')[0])
            fe.tag = 'TCP-Mux'
        elif 'client_port' in tinfo and 'server_port' in tinfo:
            cli_port = int(tinfo['client_port'].split('-')[0])
            srv_port = int(tinfo['server_port'].split('-')[0])
            # 建立 UDP Flow：server_ip:srv_port  -> client_ip:cli_port
            srv_ip, cli_ip = fe.key[0], fe.key[2]
            add_ft((cli_ip, cli_port, srv_ip, srv_port, 'UDP'),
                   'UDP-RTP', sid=sid)
        sess.last_act=time.time()

    # 5) PLAY / PAUSE / TEARDOWN
    if is_req and first in ('PLAY','PAUSE','TEARDOWN'):
        sid=fe.sess_id
        if not sid or sid not in _rst:
            invalidate_flow(fe,'ctl no sess'); return
        sess=_rst[sid]
        print("==>sess:")
        print(sess)
        sess.state={'PLAY':'PLAY','PAUSE':'PAUSED','TEARDOWN':'CLOSED'}[first]
        sess.last_act=time.time()
        if first=='TEARDOWN': _remove_session(sid)

# ─── TCP 缓冲解析循环 ─────────────────────────────────────────
def _tcp_consume(fe:FlowEntry):
    buf=fe.tcp_buf
    while buf:
        # RTP over TCP?
        if fe.tag=='TCP-Mux' and buf[0]==0x24:
            if len(buf)<4: print("len<4");break
            plen=struct.unpack('!H',buf[2:4])[0]
            if len(buf)<4+plen: print("wait next tcp");break
            rtp_callback(buf[4:4+plen],fe,buf[1])
            del buf[:4+plen]
            if len(buf)<4:
                continue
            else:
                if buf[0]==0x24:
                    print("re parse rtp!")
                    continue
                print("remain rtsp info",len(buf))
        # RTSP
        rt = _extract_rtsp(buf)
        if rt:
            _process_rtsp(rt,fe); continue
        del buf[:len(buf)]
        break                     # 数据不够

# ─── UDP / TCP 入口 ──────────────────────────────────────────
def _handle_tcp(pkt):
    key=five_tuple(pkt); payload=bytes(pkt[Raw]) if pkt.haslayer(Raw) else b''
    with _lock:
        fe=lookup_ft(key)
        if not fe:
            if payload and RTSP_MIN_RE.search(payload):
                fe=add_ft(key,'TCP-RTSP')
            else:
                return
        fe.last_ts=time.time()
        
        fe.tcp_buf.extend(payload)
        _tcp_consume(fe)

def _handle_udp(pkt):
    if not pkt.haslayer(Raw): return
    key=five_tuple(pkt)
    with _lock:
        fe=lookup_ft(key)
        if fe and fe.tag=='UDP-RTP':
            fe.last_ts=time.time()
            rtp_callback(bytes(pkt[Raw]),fe,None)

def dispatch(pkt:Packet):
    if pkt.haslayer(TCP): _handle_tcp(pkt)
    elif pkt.haslayer(UDP): _handle_udp(pkt)


# ─── GC ─────────────────────────────────────────────────────
def gc():
    now=time.time()
    with _lock:
        for k,e in list(_ft.items()):
            if now-e.last_ts>FLOW_TTL: invalidate_flow(e,'TTL')
        for sid,s in list(_rst.items()):
            ttl=PLAY_TTL if s.state=='PLAY' else IDLE_TTL
            if s.state=='CLOSED' or now-s.last_act>ttl:
                _remove_session(sid)
