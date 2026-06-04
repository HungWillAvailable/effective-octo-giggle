"""
uploader.py — Upload video lên Telegram channel qua Pyrogram.

Tích hợp với TaskManager để cập nhật tiến trình real-time.
"""

import asyncio
import logging
import time
from pathlib import Path

from pyrogram.errors import FloodWait

from backend.config import Config
from backend.task_manager import TaskManager, TaskStatus
from backend.utils import format_size

logger = logging.getLogger("Uploader")

# Debounce: lưu thời điểm cập nhật cuối cùng theo task_id
_upload_times: dict[str, float] = {}


async def safe_progress(
    current: int,
    total: int,
    task_id: str,
    idx: int,
    total_parts: int,
    manager,
) -> None:
    """Cập nhật tiến trình upload với debounce 3 giây để tránh FloodWait."""
    now = time.time()
    last_update = _upload_times.get(task_id, 0)

    if (now - last_update) > 3 or current == total:
        pct = (current / total * 100) if total else 0
        current_mb = current / (1024 * 1024)
        total_mb = total / (1024 * 1024)

        await manager.update_task(
            task_id,
            progress=pct,
            message=(
                f"Uploading part {idx}/{total_parts}: "
                f"{current_mb:.1f} MB / {total_mb:.1f} MB"
            ),
        )
        _upload_times[task_id] = now

        # Dọn dẹp entry khi upload xong
        if current == total and task_id in _upload_times:
            del _upload_times[task_id]


class Uploader:
    """Upload các part video lên Telegram channel."""

    def __init__(self, pyrogram_client) -> None:
        self.client = pyrogram_client

    async def upload_parts(
        self,
        parts: list[Path],
        task_id: str,
        original_filename: str,
    ) -> list[int]:
        """
        Upload tuần tự từng phần lên CHANNEL_ID.
        Returns: danh sách message_id đã upload thành công.
        """
        manager = TaskManager.get_instance()
        total_parts = len(parts)
        sent_ids: list[int] = []  # collect message IDs

        await manager.update_task(
            task_id,
            status=TaskStatus.UPLOADING,
            parts_total=total_parts,
            parts_done=0,
            force_broadcast=True,
        )

        for idx, part_path in enumerate(parts, start=1):
            part_size = part_path.stat().st_size
            logger.info(
                f"📤 Upload part {idx}/{total_parts}: "
                f"{part_path.name} ({format_size(part_size)})"
            )

            await manager.update_task(
                task_id,
                progress=0.0,
                message=f"Đang upload part {idx}/{total_parts}...",
                parts_done=idx - 1,
                force_broadcast=True,
            )

            async def _progress(current: int, total: int, _idx=idx) -> None:
                await safe_progress(
                    current=current,
                    total=total,
                    task_id=task_id,
                    idx=_idx,
                    total_parts=total_parts,
                    manager=manager,
                )

            msg_id = await self._upload_with_retry(
                part_path=part_path,
                caption=(
                    f"🎬 **{original_filename}**\n"
                    f"📁 Part {idx}/{total_parts}"
                ),
                progress=_progress,
            )
            sent_ids.append(msg_id)

            await manager.update_task(
                task_id,
                parts_done=idx,
                progress=100.0 * idx / total_parts,
                message=f"Đã xong part {idx}/{total_parts}",
                force_broadcast=True,
            )
            logger.info(f"✅ Xong part {idx}/{total_parts}")

        return sent_ids

    async def _upload_with_retry(
        self,
        part_path: Path,
        caption: str,
        progress,
        max_retries: int = 3,
    ) -> int:
        """Upload một file với retry tự động khi gặp FloodWait hoặc lỗi mạng."""
        for attempt in range(1, max_retries + 1):
            try:
                from pyrogram.enums import ParseMode
                sent = await self.client.send_video(
                    chat_id=Config.CHANNEL_ID,
                    video=str(part_path),
                    caption=caption,
                    parse_mode=ParseMode.MARKDOWN,
                    supports_streaming=True,
                    progress=progress,
                )
                return sent.id  # trả về message_id

            except FloodWait as e:
                wait = e.value + 5
                logger.warning(f"⏳ FloodWait {e.value}s → chờ {wait}s (attempt {attempt})")
                await asyncio.sleep(wait)

            except Exception as e:
                if attempt == max_retries:
                    raise
                logger.warning(f"⚠️ Upload lỗi (attempt {attempt}): {e} → thử lại sau 15s")
                await asyncio.sleep(15)


# ── Standalone Pipeline ────────────────────────────────────────────────────
import json
import os
import glob

TELEGRAM_LIMIT = int(1.95 * 1024 ** 3)   # 1.95 GB


async def process_and_upload(
    client,                        # pyrogram.Client đã connect
    file_path: str,
    title: str,
    description: str = "",
    channel_id: int | None = None,
    movies_db_path: str = "movies_db.json",
) -> list[int]:
    """
    Tự động upload video lên Telegram.
    - Nhỏ hơn 1.95 GB → upload thẳng 1 file.
    - Lớn hơn 1.95 GB → dùng FFmpeg chia chunk 20 phút, upload tuần tự.
    Trả về list[message_id] đã gửi.
    """
    from pyrogram.enums import ParseMode

    chat = channel_id or Config.CHANNEL_ID
    file_path = str(file_path)
    file_size = os.path.getsize(file_path)
    sent_ids: list[int] = []

    async def _send(path: str, caption: str) -> int:
        sent = await client.send_video(
            chat_id=chat,
            video=path,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            supports_streaming=True,
        )
        return sent.id

    if file_size < TELEGRAM_LIMIT:
        # ── Nhỏ: upload thẳng ─────────────────────────────────────────
        caption = f"🎬 **{title}**\n📁 Part 1/1"
        msg_id = await _send(file_path, caption)
        sent_ids.append(msg_id)
        logger.info(f"✅ Upload thẳng xong: msg_id={msg_id}")

    else:
        # ── Lớn: FFmpeg chia chunk rồi upload ─────────────────────────
        base_dir = os.path.dirname(file_path) or "."
        pattern  = os.path.join(base_dir, "part%03d.mp4")
        cmd = (
            f'ffmpeg -i "{file_path}" -c copy -map 0 '
            f'-segment_time 00:20:00 -f segment -reset_timestamps 1 '
            f'"{pattern}"'
        )
        logger.info(f"🔪 FFmpeg split: {cmd}")
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"FFmpeg lỗi: {stderr.decode()}")

        chunks = sorted(glob.glob(os.path.join(base_dir, "part*.mp4")))
        total  = len(chunks)
        logger.info(f"📦 Chia thành {total} phần, bắt đầu upload...")

        for idx, chunk in enumerate(chunks, 1):
            caption = f"🎬 **{title}**\n📁 Part {idx}/{total}"
            msg_id  = await _send(chunk, caption)
            sent_ids.append(msg_id)
            os.remove(chunk)   # xoá chunk sau khi upload xong
            logger.info(f"✅ Xong part {idx}/{total}: msg_id={msg_id}")

    # ── Lưu vào movies_db.json ─────────────────────────────────────────
    db_path = movies_db_path
    try:
        db: list[dict] = json.loads(Path(db_path).read_text("utf-8")) if Path(db_path).exists() else []
    except (json.JSONDecodeError, OSError):
        db = []

    movie_id = title.lower().replace(" ", "_").replace(".", "").replace("-", "_")
    if not any(m.get("id") == movie_id for m in db):
        db.append({
            "id":          movie_id,
            "title":       title,
            "desc":        description,
            "thumb":       "",
            "stream":      f"/stream/{sent_ids[0]}" if sent_ids else "",
            "message_ids": sent_ids,
            "channel_id":  chat,
        })
        Path(db_path).write_text(json.dumps(db, ensure_ascii=False, indent=2), "utf-8")
        logger.info(f"🎬 Đã lưu '{title}' vào {db_path}")

    return sent_ids
