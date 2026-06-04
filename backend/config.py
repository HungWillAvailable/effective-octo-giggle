"""
config.py — Tập trung toàn bộ cấu hình từ biến môi trường.
"""

import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── Telegram credentials ──────────────────────────────────
    API_ID: int = int(os.getenv("API_ID", "0"))
    API_HASH: str = os.getenv("API_HASH", "")
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")

    # Channel đích để upload
    CHANNEL_ID: int = int(os.getenv("CHANNEL_ID", "0"))

    # Thư mục lưu file tạm
    TEMP_DIR: Path = Path(os.getenv("TEMP_DIR", "./temp_downloads"))

    # Giới hạn mỗi phần: mặc định 500 MB — file lớn hơn sẽ bị FFmpeg cắt thành playlist
    MAX_PART_SIZE: int = int(
        float(os.getenv("MAX_PART_SIZE_MB", "500")) * 1024 * 1024
    )

    # Danh sách user Telegram được phép dùng bot (rỗng = tất cả)
    ALLOWED_USERS: list[int] = [
        int(uid.strip())
        for uid in os.getenv("ALLOWED_USERS", "").split(",")
        if uid.strip().isdigit()
    ]

    # Khoảng thời gian tối thiểu giữa các lần broadcast WebSocket (giây)
    PROGRESS_UPDATE_INTERVAL: float = 2.0

    # Host & port cho web server
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))

    @classmethod
    def validate(cls) -> list[str]:
        """
        Trả về danh sách lỗi cấu hình (rỗng = hợp lệ).
        Không raise exception để UI có thể hiển thị lỗi đẹp.
        """
        errors: list[str] = []
        if not cls.API_ID:
            errors.append("API_ID chưa được thiết lập")
        if not cls.API_HASH:
            errors.append("API_HASH chưa được thiết lập")
        if not cls.BOT_TOKEN:
            errors.append("BOT_TOKEN chưa được thiết lập")
        if not cls.CHANNEL_ID:
            errors.append("CHANNEL_ID chưa được thiết lập")
        if not shutil.which("ffmpeg"):
            errors.append("FFmpeg không tìm thấy trong PATH")
        if not shutil.which("ffprobe"):
            errors.append("ffprobe không tìm thấy trong PATH")
        return errors

    @classmethod
    def ensure_temp_dir(cls) -> None:
        cls.TEMP_DIR.mkdir(parents=True, exist_ok=True)
