# video_yolo_client

这是第 2 个内部小型工程：作为 CLIENT 读取第 1 个工程中的共享内存图像，并做 YOLOv8 CPU 推理。

## 功能
- 自动获取共享内存中的有效视频通道数量。
- 按轮询方式对各通道“最新已完成图像”做等概率 YOLO 检测。
- 读取 `YUV420SP/NV12` 或 `YUV420P`，转换到 BGR，缩放到 YOLOv8 输入尺寸。
- 每次检测单独记录日志，日志文件名默认为：`{pts}ms.log`。
- 每条日志至少包含：`pts`、`channel_id`、检测结果。
- 推理固定使用 CPU。

## 运行
```bash
python -m video_yolo_client.main --base-name sniff_video_shm --model-path yolov8n.pt --iterations 10 --log-dir .
```

## 依赖
- 运行真实 YOLOv8 时需要安装 `ultralytics`。
- 当前工程保留了 `UltralyticsYoloV8Detector` 真实接口；单元测试通过注入 detector 覆盖共享内存、轮询、日志和图像适配链路。
