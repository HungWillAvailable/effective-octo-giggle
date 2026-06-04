"""
downloader.py — Tải video từ URL bằng httpx (pure Python, Python 3.14 compatible).

Progress được báo cáo qua TaskManager thay vì edit Telegram message.
"""

import logging
import re
import time
from pathlib import Path

import aiofiles
import httpx

from backend.config import Config
from backend.task_manager import TaskManager, TaskStatus
from backend.utils import format_size, sanitize_filename

logger = logging.getLogger("Downloader")


class Downloader:

    @staticmethod
    async def download_from_url(
        url: str,
        dest_dir: Path,
        task_id: str,
    ) -> Path:
        """
        Tải file từ URL về dest_dir với báo cáo tiến trình qua TaskManager.

        Dùng httpx streaming để không load toàn bộ file vào RAM.

        Args:
            url      : URL trực tiếp đến file video
            dest_dir : Thư mục lưu file tải về
            task_id  : ID task để cập nhật progress

        Returns:
            Path đến file đã tải về
        """
        manager = TaskManager.get_instance()
        logger.info(f"🌐 Tải từ URL: {url}")

        # Timeout: connect 30s, đọc dữ liệu không giới hạn (None)
        timeout = httpx.Timeout(connect=30.0, read=None, write=None, pool=None)

        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
        ) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()

                # ── Xác định tên file ─────────────────────────────────
                content_disp = resp.headers.get("Content-Disposition", "")
                fn_match = re.findall(
                    r'filename[*]?=["\']?([^"\';\n]+)["\']?', content_disp
                )
                if fn_match:
                    filename = sanitize_filename(fn_match[0].strip())
                else:
                    raw = url.split("?")[0].rstrip("/").split("/")[-1] or "video.mp4"
                    filename = sanitize_filename(raw)
                    if not Path(filename).suffix:
                        ct = resp.headers.get("Content-Type", "")
                        ext = ct.split("/")[-1].split(";")[0].strip()
                        filename += f".{ext}" if ext else ".mp4"

                dest_path = dest_dir / filename
                total_str = resp.headers.get("Content-Length", "0")
                total_size = int(total_str) if total_str.isdigit() else 0

                await manager.update_task(
                    task_id,
                    status=TaskStatus.DOWNLOADING,
                    message=f"Đang tải: {filename}",
                    size=total_size,
                    force_broadcast=True,
                )

                # ── Stream download từng chunk 1MB ────────────────────
                downloaded = 0
                chunk_size = 1024 * 1024  # 1 MB
                last_update = time.monotonic()

                async with aiofiles.open(dest_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size):
                        await f.write(chunk)
                        downloaded += len(chunk)

                        now = time.monotonic()
                        if now - last_update >= Config.PROGRESS_UPDATE_INTERVAL:
                            last_update = now
                            pct = (downloaded / total_size * 100) if total_size else 0
                            await manager.update_task(
                                task_id,
                                progress=pct,
                                message=(
                                    f"Đang tải: {format_size(downloaded)}"
                                    + (f" / {format_size(total_size)}" if total_size else "")
                                ),
                            )

                logger.info(f"✅ Tải xong: {dest_path}")
                return dest_path
