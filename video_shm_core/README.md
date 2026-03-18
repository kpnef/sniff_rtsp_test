# video_shm_core

这是第 1 个内部小型工程：负责共享内存多缓存管理、FFmpeg 解码、以及对 sniff / RTSP / 文件输入的统一写入。

## 功能
- 共享内存 MASTER 初始化接口。
- 多通道、多缓存块环形缓存，默认满足：`4 通道 * 15 块 * 1080P YUV420`。
- 每个缓存块包含状态标记：`EMPTY / DECODING / COMPLETE`。
- 每个缓存块包含 `PTS`、通道号、块号、宽高、像素格式、写入序号。
- CLIENT 端只读取“最新且已解码完成”的块，不会读到正在解码的块。
- 支持：
  - 本地文件解码写入共享内存
  - RTSP 拉流解码写入共享内存
  - sniff 重组出的 H264/H265 Annex-B 数据解码写入共享内存

## 快速开始
```bash
python -m video_shm_core.main init --base-name sniff_video_shm --reset-existing
python -m video_shm_core.main decode-file ./sample.mp4 --base-name sniff_video_shm --source-id 192.168.0.10
```

## 主要接口
- `SharedVideoRingBuffer.create(...)`：MASTER 强制初始化
- `SharedVideoRingBuffer.create_or_attach(...)`：优先复用，必要时初始化
- `SharedVideoRingBuffer.attach(...)`：CLIENT 连接
- `SharedVideoRingBuffer.ensure_channel(source_id)`：按 IP / source_id 分配通道
- `SharedVideoRingBuffer.write_frame(...)`：写入解码后的 YUV420 数据
- `SharedVideoRingBuffer.read_latest_frame(channel_id)`：读取最新已完成帧
- `decode_video_file_to_shm(...)`
- `decode_rtsp_to_shm(...)`
- `AnnexBSharedMemoryWriter.push_access_unit(...)`
- `SniffToSharedMemoryBridge.handle_frame(...)`

## 说明
- 默认像素格式为 `nv12`，属于 `YUV420SP`，满足后续 YOLO 客户端适配要求。
- 如需平面格式，可切换到 `yuv420p`。
