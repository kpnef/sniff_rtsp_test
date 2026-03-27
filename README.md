# sniff 项目增强说明

当前仓库在原始 RTP/RTSP 嗅探能力基础上，补充了 3 个子工程：

- `video_shm_core/`：共享内存 + 解码结果写入核心
- `video_yolo_client/`：共享内存 CLIENT + YOLOv8 CPU 检测
- `video_rtsp_ingest/`：主动 RTSP 拉流 DEMO，并写入共享内存

同时，`sniff.py` 已接入 `video_shm_core.SniffToSharedMemoryBridge`，嗅探到的 H264/H265 原始视频可按启动参数决定是否进入 FFmpeg 解码并写入共享内存。

---

## 本次调整后的核心原则

### 1) `video_shm_core` 不访问任何 URL
`video_shm_core` 只负责：
- 共享内存环形缓冲管理
- 解码后的原始帧写入
- 嗅探重组出的 AnnexB 码流写入
- 本地文件解码写入

它不再直接访问 RTSP URL。

### 2) URL 访问只在上层入口发生
- 主动拉流：`video_rtsp_ingest`
- 嗅探抓包：`sniff.py`

### 3) 共享内存预算默认按 1080P 原始图像
默认配置：
- `width = 1920`
- `height = 1080`
- `pix_fmt = nv12`

如果输入帧超过该预算，会在写入共享内存前使用 **OpenCV** resize 到不超过预算尺寸。

---

## 主动拉流 DEMO 与 sniff 嗅探本体的对比

### A. 主动拉流 DEMO：`video_rtsp_ingest`

现在主动拉流链路已经改成：
- 拉流线程：只负责从 RTSP 拉 AnnexB/ES 码流并入队
- 解码线程：只负责从队列取码流、解码、写共享内存
- 实时统计线程：持续打印与 sniff 对齐的统计项，重点包括 `key_frames / nonkey_frames / audio_packets`，并显示视频/音频队列水位；相关队列默认上限均为 `400`

适用于：
- 已知 RTSP 地址
- 不需要抓网卡包
- 目标是稳定拉流并写共享内存

命令示例：

```bash
python -m video_rtsp_ingest.main rtsp://127.0.0.1:8554/live \
  --base-name sniff_video_shm \
  --max-channels 4 \
  --blocks-per-channel 15 \
  --width 1920 \
  --height 1080 \
  --pix-fmt nv12 \
  --reset-existing
```

详细 DEMO 说明见：
- `video_rtsp_ingest/README.md`

### B. sniff 嗅探本体：`sniff.py`
适用于：
- 需要在网卡上抓 RTSP/RTP 包
- 需要重组 NAL / Access Unit
- 需要从抓包链路把视频送入共享内存

命令示例：

```bash
python sniff.py --write-shm \
  --iface lo \
  --shm-base-name sniff_video_shm \
  --shm-max-channels 4 \
  --shm-blocks-per-channel 15 \
  --shm-frame-width 1920 \
  --shm-frame-height 1080 \
  --shm-pix-fmt nv12 \
  --shm-reset-on-boot
```

---

## 共享内存配置能力对齐结果

现在两条链路在共享内存配置能力上已经对齐到同一组核心参数：

| 能力 | 主动拉流 DEMO | sniff 嗅探本体 |
|---|---|---|
| 共享内存基名 | `--base-name` | `--shm-base-name` |
| 最大通道数 | `--max-channels` | `--shm-max-channels` |
| 每通道块数 | `--blocks-per-channel` | `--shm-blocks-per-channel` |
| 预算宽度 | `--width` | `--shm-frame-width` |
| 预算高度 | `--height` | `--shm-frame-height` |
| 像素格式 | `--pix-fmt` | `--shm-pix-fmt` |
| 重建共享内存 | `--reset-existing` | `--shm-reset-on-boot` |

对齐后的统一语义：
- 它们都写入同一种共享内存环形结构
- 都支持 `nv12` / `yuv420p`
- 都支持 1080P 预算预分配
- 都支持“超出预算则 resize 后写入”

---

## sniff 主工程启动模式

### 1. 仅做嗅探统计（默认）

```bash
python sniff.py
```

或：

```bash
python sniff.py --stats-only
```

### 2. 嗅探并写入共享内存

```bash
python sniff.py --write-shm
```

可选共享内存参数示例：

```bash
python sniff.py --write-shm \
  --shm-base-name sniff_video_shm \
  --shm-max-channels 4 \
  --shm-blocks-per-channel 15 \
  --shm-frame-width 1920 \
  --shm-frame-height 1080 \
  --shm-pix-fmt nv12
```

---

## 环境变量

以下环境变量仍然支持：

- `SNIFF_IFACE`
- `SNIFF_WRITE_SHM`
- `SNIFF_SHM_BASE_NAME`
- `SNIFF_SHM_MAX_CHANNELS`
- `SNIFF_SHM_BLOCKS_PER_CHANNEL`
- `SNIFF_SHM_FRAME_WIDTH`
- `SNIFF_SHM_FRAME_HEIGHT`
- `SNIFF_SHM_PIX_FMT`
- `SNIFF_SHM_RESET_ON_BOOT`

命令行参数优先用于显式控制启动模式。

---

## 建议的验证顺序

### 1. 先验证共享内存核心与本地文件写入

```bash
pytest -q
```

### 2. 再验证主动拉流 DEMO

- 启动 MediaMTX
- 用 ffmpeg 推测试流
- 用 `video_rtsp_ingest` 拉流写共享内存
- 用 `SharedVideoRingBuffer.read_latest_frame()` 读回检查最新帧

### 3. 最后验证 sniff 嗅探链路

- 启动抓包权限足够的环境
- 对 `lo` 或真实业务网卡抓包
- 检查共享内存里是否出现最新解码帧
