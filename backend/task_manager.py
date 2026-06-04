"""
task_manager.py — Quản lý task queue và broadcast WebSocket.

TaskManager là singleton điều phối:
  - Lưu trữ trạng thái tất cả các task (in-memory)
  - Broadcast cập nhật real-time qua WebSocket đến tất cả clients
  - Giới hạn số lượng FFmpeg chạy đồng thời (semaphore)
"""

import asyncio
import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Optional

from fastapi import WebSocket

logger = logging.getLogger("TaskManager")


class TaskStatus(str, Enum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"   # FFmpeg đang cắt
    UPLOADING = "uploading"
    DONE = "done"
    ERROR = "error"
    CANCELLED = "cancelled"


@dataclass
class TaskInfo:
    """Thông tin đầy đủ của một task xử lý video."""
    id: str
    filename: str
    source: str                          # URL hoặc "telegram_file"
    status: TaskStatus = TaskStatus.PENDING
    progress: float = 0.0               # 0–100
    message: str = "Đang chờ xử lý..."
    size: int = 0                        # bytes, cập nhật sau khi download
    parts_total: int = 1
    parts_done: int = 0
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status.value
        return d


class WebSocketManager:
    """Quản lý tập hợp các WebSocket clients và broadcast message."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)
        logger.info(f"🔌 WebSocket kết nối. Tổng: {len(self._connections)}")

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        logger.info(f"🔌 WebSocket ngắt kết nối. Còn: {len(self._connections)}")

    async def broadcast(self, payload: dict) -> None:
        """Gửi JSON đến tất cả clients đang kết nối."""
        dead: list[WebSocket] = []
        for ws in self._connections:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections.discard(ws)

    @property
    def count(self) -> int:
        return len(self._connections)


class TaskManager:
    """
    Singleton quản lý toàn bộ vòng đời của các task.

    Sử dụng:
        manager = TaskManager.get_instance()
        task = manager.create_task(filename="video.mp4", source="http://...")
        await manager.update_task(task.id, status=TaskStatus.DOWNLOADING, progress=50)
    """

    _instance: Optional["TaskManager"] = None

    def __init__(self) -> None:
        self._tasks: dict[str, TaskInfo] = {}
        self.ws_manager = WebSocketManager()
        # Semaphore: chỉ 1 tiến trình FFmpeg chạy đồng thời
        self.ffmpeg_semaphore = asyncio.Semaphore(1)
        # Semaphore: tối đa 2 upload Telegram song song
        self.upload_semaphore = asyncio.Semaphore(2)
        self._last_broadcast: dict[str, float] = {}

    @classmethod
    def get_instance(cls) -> "TaskManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ── CRUD Task ──────────────────────────────────────────────────────────

    def create_task(self, filename: str, source: str) -> TaskInfo:
        """Tạo task mới và thêm vào danh sách."""
        task = TaskInfo(
            id=str(uuid.uuid4())[:8],
            filename=filename,
            source=source,
        )
        self._tasks[task.id] = task
        logger.info(f"📋 Task mới: {task.id} — {filename}")
        return task

    def get_task(self, task_id: str) -> Optional[TaskInfo]:
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> list[TaskInfo]:
        """Trả về tasks theo thứ tự tạo mới nhất trước."""
        return sorted(
            self._tasks.values(),
            key=lambda t: t.created_at,
            reverse=True,
        )

    def remove_task(self, task_id: str) -> bool:
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def clear_completed(self) -> int:
        """Xóa tất cả task đã hoàn tất / lỗi. Trả về số lượng đã xóa."""
        to_remove = [
            tid for tid, t in self._tasks.items()
            if t.status in (TaskStatus.DONE, TaskStatus.ERROR, TaskStatus.CANCELLED)
        ]
        for tid in to_remove:
            del self._tasks[tid]
        return len(to_remove)

    # ── Cập nhật trạng thái & Broadcast ────────────────────────────────────

    async def update_task(
        self,
        task_id: str,
        *,
        status: Optional[TaskStatus] = None,
        progress: Optional[float] = None,
        message: Optional[str] = None,
        size: Optional[int] = None,
        parts_total: Optional[int] = None,
        parts_done: Optional[int] = None,
        error: Optional[str] = None,
        force_broadcast: bool = False,
    ) -> None:
        """
        Cập nhật task và broadcast qua WebSocket.
        Throttle broadcast: tối thiểu 2 giây/lần để tránh spam client.
        Dùng force_broadcast=True cho các sự kiện quan trọng (done, error).
        """
        task = self._tasks.get(task_id)
        if not task:
            return

        if status is not None:
            task.status = status
            if status in (TaskStatus.DONE, TaskStatus.CANCELLED):
                task.completed_at = time.time()
                task.progress = 100.0
        if progress is not None:
            task.progress = round(progress, 1)
        if message is not None:
            task.message = message
        if size is not None:
            task.size = size
        if parts_total is not None:
            task.parts_total = parts_total
        if parts_done is not None:
            task.parts_done = parts_done
        if error is not None:
            task.error = error
            task.status = TaskStatus.ERROR
            task.completed_at = time.time()

        # ── Throttle broadcast ──────────────────────────────────────────
        now = time.monotonic()
        last = self._last_broadcast.get(task_id, 0)
        from backend.config import Config
        if force_broadcast or (now - last >= Config.PROGRESS_UPDATE_INTERVAL):
            self._last_broadcast[task_id] = now
            await self._broadcast_task(task)

    async def _broadcast_task(self, task: TaskInfo) -> None:
        """Gửi trạng thái task đến tất cả WebSocket clients."""
        await self.ws_manager.broadcast({
            "type": "task_update",
            "task": task.to_dict(),
        })

    async def broadcast_system_status(self) -> None:
        """Broadcast thông tin hệ thống (disk, active tasks)."""
        import shutil
        from backend.config import Config
        try:
            usage = shutil.disk_usage(Config.TEMP_DIR)
            active = sum(
                1 for t in self._tasks.values()
                if t.status not in (
                    TaskStatus.DONE, TaskStatus.ERROR,
                    TaskStatus.CANCELLED, TaskStatus.PENDING,
                )
            )
            await self.ws_manager.broadcast({
                "type": "system_status",
                "disk_free": usage.free,
                "disk_total": usage.total,
                "disk_used": usage.used,
                "active_tasks": active,
                "total_tasks": len(self._tasks),
            })
        except Exception as e:
            logger.warning(f"Không lấy được disk info: {e}")
