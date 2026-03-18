# video_rtsp_ingest

这是第 3 个内部小型工程：主动拉取指定 RTSP 地址，完成解码并写入第 1 个工程的共享内存。

## 功能
- 作为 MASTER 初始化共享内存。
- 对指定 RTSP 地址进行 FFmpeg 拉流。
- 对视频进行解码，输出 `NV12(YUV420SP)` 到共享内存。
- 每个 IP / 主机名映射为一个通道。

## 运行
```bash
python -m video_rtsp_ingest.main rtsp://192.168.0.20/live --base-name sniff_video_shm --reset-existing
```

## 说明
- 若一个地址首次出现，会自动申请一个共享内存通道。
- 输出缓冲由 `video_shm_core` 统一管理。
