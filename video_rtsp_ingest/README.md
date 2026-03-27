# video_rtsp_ingest

主动拉取指定 RTSP 地址，完成解码，并把最新帧写入 `video_shm_core` 统一管理的共享内存。

当前实现已经调整为 **拉流线程** 和 **解码线程** 脱耦：
- 线程 1：从 RTSP 拉取视频码流，放入内存队列
- 线程 2：从队列取码流，解码后写入共享内存
- 独立统计输出：实时打印 `key_frames / nonkey_frames / audio_packets / decoded_frames / video_queue / audio_queue` 等指标

这一路是 **DEMO / 主动拉流入口**：
- RTSP URL 访问只发生在本工程。
- `video_shm_core` 只负责“共享内存 + 解码结果写入”，不再直接访问任何 URL。
- 共享内存默认按 **1080P 原始图像预算** 预分配；如果输入帧超出预算，会在写入前使用 **OpenCV** 缩放到不超过共享内存配置的尺寸。

---

## 1. 能力边界

### 本工程负责
- 访问 RTSP URL
- 对 RTSP 流做 `ffprobe/ffmpeg` 拉流
- 把解码后的 raw frame 交给 `video_shm_core.SharedMemoryFrameSink`
- 由 `video_shm_core` 统一落到共享内存

### 本工程不负责
- 不直接实现共享内存结构
- 不单独定义一套和 sniff 不一致的共享内存配置

---

## 2. 与 sniff 嗅探本体的共享内存配置对齐

`video_rtsp_ingest` 与 `sniff.py` 现在使用同一套共享内存关键参数：

- `base-name`
- `max-channels`
- `blocks-per-channel`
- `width`
- `height`
- `pix-fmt`
- `reset-existing`

对应关系如下：

| 主动拉流 `video_rtsp_ingest` | 嗅探 `sniff.py` |
|---|---|
| `--base-name` | `--shm-base-name` |
| `--max-channels` | `--shm-max-channels` |
| `--blocks-per-channel` | `--shm-blocks-per-channel` |
| `--width` | `--shm-frame-width` |
| `--height` | `--shm-frame-height` |
| `--pix-fmt` | `--shm-pix-fmt` |
| `--reset-existing` | `--shm-reset-on-boot` |

含义也保持一致：
- `width/height` 是共享内存预分配预算尺寸，默认 `1920x1080`
- 输入帧小于等于预算时，按原始尺寸写入
- 输入帧大于预算时，先 resize，再写入
- `pix-fmt` 当前支持 `nv12` 和 `yuv420p`

---

## 3. 快速开始（DEMO）

### 3.1 启动本地 RTSP 服务（推荐使用 MediaMTX）

准备 `mediamtx.yml`：

```yaml
paths:
  live:
```

启动：

```bash
./mediamtx mediamtx.yml
```

默认会监听：

```text
rtsp://127.0.0.1:8554/live
```

### 3.2 用 FFmpeg 推一个测试视频到本地 RTSP 服务

```bash
ffmpeg -re -stream_loop -1 \
  -f lavfi -i testsrc=size=1280x720:rate=10 \
  -c:v libx264 -pix_fmt yuv420p \
  -f rtsp -rtsp_transport tcp \
  rtsp://127.0.0.1:8554/live
```

也可以用超过 1080P 的测试源验证 resize：

```bash
ffmpeg -re -stream_loop -1 \
  -f lavfi -i testsrc=size=2560x1440:rate=10 \
  -c:v libx264 -pix_fmt yuv420p \
  -f rtsp -rtsp_transport tcp \
  rtsp://127.0.0.1:8554/live
```

### 3.3 主动拉流并写入共享内存

实时输出示例：

```text
[video_rtsp_ingest] source=127.0.0.1 channel=0 video_queue=2 video_high=4 audio_queue=1 audio_high=3 key_frames=1 nonkey_frames=27 audio_packets=16 decoded_frames=28 pulled_chunks=31 pulled_bytes=221184 last_pts=2700
```

字段说明：
- `video_queue`：当前视频拉流队列积压
- `video_high`：视频队列历史最高水位
- `audio_queue`：当前音频拉流队列积压
- `audio_high`：音频队列历史最高水位
- `key_frames`：关键帧数
- `nonkey_frames`：非关键帧数
- `audio_packets`：音频帧数
- `decoded_frames`：已解码并写入共享内存的帧数
- `last_pts`：最近一次写入共享内存的 PTS


```bash
python -m video_rtsp_ingest.main rtsp://127.0.0.1:8554/live \
  --base-name sniff_video_shm \
  --max-channels 4 \
  --blocks-per-channel 15 \
  --width 1920 \
  --height 1080 \
  --pix-fmt nv12 \
  --queue-max-chunks 400 \
  --audio-queue-max-chunks 400 \
  --meta-queue-max-items 400 \
  --reset-existing
```

说明：
- 这条命令会自动按 RTSP 地址主机名分配通道
- `--reset-existing` 适合 DEMO 首次启动时清理旧共享内存
- 如果 RTSP 原始视频分辨率超过 `1920x1080`，写入前会自动 resize

说明补充：
- `--queue-max-chunks`：视频拉流队列上限，默认 `400`
- `--audio-queue-max-chunks`：音频拉流队列上限，默认 `400`
- `--meta-queue-max-items`：视频解码元信息队列上限（PTS/关键帧标记），默认 `400`
- 若 RTSP 源包含音频，主动拉流会额外拉取音频并统计 `audio_packets`（仅按包/访问单元统计，不做解码）

---

## 4. 如何检查共享内存最新帧是否正常

工程里建议使用 `SharedVideoRingBuffer.read_latest_frame(channel_id)` 做读取校验。

可参考下面的最小检查脚本：

```python
from video_shm_core import SharedVideoRingBuffer

ring = SharedVideoRingBuffer.attach('sniff_video_shm')
try:
    frame = ring.read_latest_frame(0)
    if frame is None:
        raise SystemExit('no frame available')

    print('channel_id =', frame.channel_id)
    print('width      =', frame.width)
    print('height     =', frame.height)
    print('pix_fmt    =', frame.pix_fmt)
    print('pts        =', frame.pts)
    print('write_seq  =', frame.write_seq)
    print('data_len   =', len(frame.data))

    assert frame.width <= 1920
    assert frame.height <= 1080
    assert len(frame.data) > 0
finally:
    ring.close(unlink=False)
```

如果 `pix_fmt=nv12`，还可以进一步检查：

```python
assert len(frame.data) == frame.width * frame.height * 3 // 2
```

---

## 5. 常用命令

### 初始化共享内存（只建环，不拉流）

```bash
python -m video_shm_core.main init \
  --base-name sniff_video_shm \
  --max-channels 4 \
  --blocks-per-channel 15 \
  --width 1920 \
  --height 1080 \
  --pix-fmt nv12 \
  --queue-max-chunks 400 \
  --audio-queue-max-chunks 400 \
  --meta-queue-max-items 400 \
  --reset-existing
```

### 主动拉流

```bash
python -m video_rtsp_ingest.main rtsp://127.0.0.1:8554/live \
  --base-name sniff_video_shm \
  --queue-max-chunks 400 \
  --audio-queue-max-chunks 400 \
  --meta-queue-max-items 400 \
  --reset-existing
```

---

## 6. 注意事项

- `video_rtsp_ingest` 是 URL 入口；`video_shm_core` 不再负责 URL 访问。
- 如果服务端或网络环境不稳定，建议保持 `RTSP/TCP`。
- 若多路流同时写入，请把 `max-channels` 配置得足够大。
- 共享内存预算设置得越大，单通道占用越高；默认目标是“4 通道 * 15 块 * 1080P YUV420”。
