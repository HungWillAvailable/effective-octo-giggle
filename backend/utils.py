"""
utils.py — Các hàm tiện ích dùng chung toàn bộ app.
"""

import re
from pathlib import Path


def format_size(size_bytes: int) -> str:
    """Chuyển bytes → chuỗi dễ đọc (B / KB / MB / GB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / 1024 ** 2:.1f} MB"
    return f"{size_bytes / 1024 ** 3:.2f} GB"


def format_duration(seconds: float) -> str:
    """Chuyển giây → chuỗi HH:MM:SS."""
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def sanitize_filename(name: str) -> str:
    """Loại bỏ ký tự không hợp lệ khỏi tên file, rút gọn nếu quá dài."""
    name = re.sub(r"[^\w.\-]", "_", name)
    stem = Path(name).stem[:80]
    suffix = Path(name).suffix or ".mp4"
    return f"{stem}{suffix}"
