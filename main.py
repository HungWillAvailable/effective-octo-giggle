"""
main.py — Entry point: khởi chạy Uvicorn server.

Cách dùng:
    python main.py
    hoặc: uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import logging
import sys

# Fix encoding UTF-8 cho Windows terminal (tránh lỗi emoji)
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import uvicorn

from backend.api import app   # noqa: F401 — expose app cho uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if __name__ == "__main__":
    from backend.config import Config

    print("=" * 60)
    print("  Telegram Video Uploader - Web UI")
    print("=" * 60)
    print(f"  Dashboard: http://{Config.HOST}:{Config.PORT}")
    print(f"  API Docs : http://{Config.HOST}:{Config.PORT}/docs")
    print("  Ctrl+C to stop")
    print("=" * 60)

    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        reload=False,
        log_level="info",
    )
