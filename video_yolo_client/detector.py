from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DetectorBase(ABC):
    input_size: int = 640

    @abstractmethod
    def detect(self, image_bgr) -> list[dict[str, Any]]:
        raise NotImplementedError


class UltralyticsYoloV8Detector(DetectorBase):
    def __init__(self, model_path: str = 'yolov8n.pt', input_size: int = 640) -> None:
        self.input_size = input_size
        try:
            from ultralytics import YOLO
        except ImportError as exc:  # pragma: no cover - depends on runtime env
            raise RuntimeError(
                'ultralytics is required for real YOLOv8 inference. '
                'Install it in the runtime environment before starting the client project.'
            ) from exc
        self._model = YOLO(model_path)

    def detect(self, image_bgr) -> list[dict[str, Any]]:
        results = self._model.predict(image_bgr, device='cpu', verbose=False, imgsz=self.input_size)
        detections: list[dict[str, Any]] = []
        for result in results:
            boxes = getattr(result, 'boxes', None)
            if boxes is None:
                continue
            xyxy = boxes.xyxy.cpu().numpy() if hasattr(boxes.xyxy, 'cpu') else boxes.xyxy
            conf = boxes.conf.cpu().numpy() if hasattr(boxes.conf, 'cpu') else boxes.conf
            cls = boxes.cls.cpu().numpy() if hasattr(boxes.cls, 'cpu') else boxes.cls
            for idx in range(len(xyxy)):
                detections.append(
                    {
                        'xyxy': [float(v) for v in xyxy[idx].tolist()],
                        'conf': float(conf[idx]),
                        'cls': int(cls[idx]),
                    }
                )
        return detections
