# sniff 项目增强说明

当前仓库在原始 RTP/RTSP 嗅探能力基础上，补充了 3 个子工程：

- `video_shm_core/`：共享内存 + FFmpeg 解码核心
- `video_yolo_client/`：共享内存 CLIENT + YOLOv8 CPU 检测
- `video_rtsp_ingest/`：主动 RTSP 拉流并写入共享内存

同时，`sniff.py` 已接入 `video_shm_core.SniffToSharedMemoryBridge`，嗅探到的 H264/H265 原始视频可按启动参数决定是否进入 FFmpeg 解码并写入共享内存。

## sniff 主工程启动模式

### 1. 仅做嗅探统计（默认）

不初始化共享内存，只进行 RTP/RTSP/NAL 嗅探统计输出。

```bash
python sniff.py
```

也可以显式指定：

```bash
python sniff.py --stats-only
```

### 2. 嗅探并写入共享内存

初始化共享内存，并把嗅探得到的 H264/H265 视频流送入 FFmpeg 解码后写入共享内存，供后续 `video_yolo_client` 等子工程读取。

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

## 环境变量

以下环境变量仍然支持：

- `SNIFF_IFACE`
- `SNIFF_WRITE_SHM`：`1/true/on/yes` 表示默认写入共享内存
- `SNIFF_SHM_BASE_NAME`
- `SNIFF_SHM_MAX_CHANNELS`
- `SNIFF_SHM_BLOCKS_PER_CHANNEL`
- `SNIFF_SHM_FRAME_WIDTH`
- `SNIFF_SHM_FRAME_HEIGHT`
- `SNIFF_SHM_PIX_FMT`
- `SNIFF_SHM_RESET_ON_BOOT`

命令行参数优先用于显式控制启动模式。

详细用法见各子目录 `README.md`。
