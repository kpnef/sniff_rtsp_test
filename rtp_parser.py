#!/usr/bin/env python3
import struct, callbacks

START_CODE = b'\x00\x00\x00\x01'

_on_rtp   = lambda x: None
_on_frame = lambda x: None
def set_rtp_callback(fn):   global _on_rtp;   _on_rtp = fn
def set_frame_callback(fn): global _on_frame; _on_frame = fn

# ---------- RTP header ----------
def _hdr(buf):
    b1,b2,seq,ts,ssrc = struct.unpack('!BBHII', buf[:12])
    if (b1>>6)!=2: raise ValueError('RTP ver')
    cc=b1&0xF
    return {'seq':seq,'ts':ts,'ssrc':ssrc,'pt':b2&0x7F,
            'm':(b2>>7)&1,'hdr_len':12+cc*4}

# ---------- codec ----------
_STATIC_PT={34:'H263',96:'H264',98:'H265',0:'PCMU',8:'PCMA',97:'AAC'}
def _codec(pt,flow):
    return _STATIC_PT.get(pt)

# ---------- emit ----------
def _emit_rtp(info,size,frag,codec):
    _on_rtp({'pts':info['ts'],'size':size,'frag':frag,
             'codec':codec,'flow':info['flow'],'channel':info['channel']})
def _emit_frame(d,info,codec,nalu=None,is_video=True):
    _on_frame({'pts':info['ts'],'codec':codec,'is_video':is_video,
               'nalu_type':nalu,'data':d,'flow':info['flow'],
               'channel':info['channel']})

# ---------- H26x ----------
_frag_h26x={}
def _h26x(rtp,payload,info,codec):
    if codec=='H264':
        nal_hdr=payload[0]; nalu=nal_hdr&0x1F; FU,STAP=28,24
    else:
        nal_hdr=payload[:2]; nalu=(nal_hdr[0]>>1)&0x3F; FU,STAP=49,48
    if nalu not in (FU,STAP):                    # 单 NAL
        _emit_rtp(info,len(payload),'S',codec)
        _emit_frame(START_CODE+payload,info,codec,nalu)
        return
    if nalu==STAP:                               # 聚合
        _emit_rtp(info,len(payload),'STAP',codec)
        off=1 if codec=='H264' else 2
        try:
            while off+2<=len(payload):
                l=struct.unpack('!H',payload[off:off+2])[0]; off+=2
                if off+l>len(payload): raise ValueError
                n=payload[off:off+l]
                nt=n[0]&0x1F if codec=='H264' else (n[0]>>1)&0x3F
                _emit_frame(START_CODE+n,info,codec,nt)
                off+=l
        except: callbacks.invalidate_flow(info['flow'],'STAP err')
        return
    # FU-A
    try:
        if codec=='H264':
            S,E=(payload[1]>>7)&1,(payload[1]>>6)&1; nt=payload[1]&0x1F; frag=payload[2:]
        else:
            S,E=(payload[2]>>7)&1,(payload[2]>>6)&1; nt=(payload[2]>>1)&0x3F; frag=payload[3:]
        _emit_rtp(info,len(payload),'FU',codec)
        key=(rtp['ssrc'],rtp['ts'],codec,info['channel'])
        lst=_frag_h26x.setdefault(key,[]); lst.append((rtp['seq'],frag,S,E))
        if E:
            lst.sort(key=lambda x:x[0])
            data=bytearray()
            if codec=='H264':
                data.append((payload[0]&0xE0)|nt)
            else:
                hdr=bytearray(payload[:2]); hdr[0]&=0x81; hdr[1]=(nt<<1)&0x7E
                data.extend(hdr)
            for _,p,_,_ in lst: data.extend(p)
            _emit_frame(START_CODE+bytes(data),info,codec,nt)
            _frag_h26x.pop(key,None)
    except: callbacks.invalidate_flow(info['flow'],'FU err')

# ---------- H263 ----------
_frag_h263={}
def _h263(rtp,payload,info):
    key=(rtp['ssrc'],rtp['ts'],info['channel'])
    _frag_h263.setdefault(key,[]).append((rtp['seq'],payload))
    _emit_rtp(info,len(payload),'H263','H263')
    if rtp['m']:
        lst=_frag_h263.pop(key)
        lst.sort(key=lambda x:x[0])
        data=b''.join(p for _,p in lst)
        _emit_frame(data,info,'H263',None)

# ---------- SSRC Guard ----------
_ssrc_guard={}

# ---------- 入口 ----------
def process(rtp_bytes: bytes, flow, channel):
    if len(rtp_bytes)<12:
        print("not enough len!!")
        return
        #callbacks.invalidate_flow(flow,'RTP<12'); return
    try: 
        rtp=_hdr(rtp_bytes)
        if rtp['ssrc']==0 and rtp['pt']==0:
            print("heart package\n")
            return 
    except Exception as e:
        callbacks.invalidate_flow(flow,f'hdr {e}'); return
    codec=_codec(rtp['pt'],flow)
    if not codec:
        callbacks.invalidate_flow(flow,f'PT{rtp["pt"]}'); return
    keyg=(flow.key,channel)
    if (ss:=_ssrc_guard.get(keyg)) is None:
        _ssrc_guard[keyg]=rtp['ssrc']
    elif ss!=rtp['ssrc']:
        callbacks.invalidate_flow(flow,'SSRC jump'); return
    rtp.update({'flow':flow,'channel':channel})
    payload=rtp_bytes[rtp['hdr_len']:]
    if codec in ('H264','H265'): _h26x(rtp,payload,rtp,codec)
    elif codec=='H263':          _h263(rtp,payload,rtp)
    else:
        _emit_rtp(rtp,len(payload),'A',codec)
        _emit_frame(payload,rtp,codec,is_video=False)
