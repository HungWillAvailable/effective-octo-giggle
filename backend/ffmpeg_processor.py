"""
ffmpeg_processor.py — Xử lý FFmpeg: lấy thông tin và cắt video.

Tất cả subprocess chạy trong asyncio.to_thread để không block event loop.
"""

import asyncio
import json
import logging
import math
import subprocess
from pathlib import Path

from backend.utils import format_duration, format_size, sanitize_filename

logger = logging.getLogger("FFmpegProcessor")


class FFmpegProcessor:

    @staticmethod
    async def get_video_info(video_path: Path) -> dict:
        """
        Dùng ffprobe để lấy duration, size, bitrate của video.
        Trả về dict: {duration: float, size: int, bitrate: int}
        """
        cmd = [
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            str(video_path),
        ]

        def _run() -> dict:
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding="utf-8",
            )
            # Guard: ffprobe có thể fail hoặc stdout rỗng
            if result.returncode != 0 or not result.stdout:
                err = (result.stderr or "").strip()[:200]
                raise RuntimeError(
                    f"ffprobe thất bại (code {result.returncode}): {err}"
                )
            info = json.loads(result.stdout)
            fmt = info.get("format", {})
            return {
                "duration": float(fmt.get("duration", 0)),
                "size": int(fmt.get("size", 0)),
                "bitrate": int(fmt.get("bit_rate", 0)),
            }

        return await asyncio.to_thread(_run)

    @staticmethod
    async def split_video(
        input_path: Path,
        output_dir: Path,
        max_part_size: int,
        progress_callback=None,   # async callable(part_idx, total_parts)
    ) -> list[Path]:
        """
        Cắt video thành nhiều phần, mỗi phần ≤ max_part_size bytes.

        Thuật toán:
          1. Lấy duration + kích thước thực
          2. Số phần = ceil(size / max_part_size)
          3. Thời lượng mỗi phần = duration / num_parts * 0.95 (buffer 5%)
          4. FFmpeg -c copy từng đoạn

        Args:
            input_path      : File video gốc
            output_dir      : Thư mục lưu các phần
            max_part_size   : Kích thước tối đa mỗi phần (bytes)
            progress_callback: Callback sau mỗi phần cắt xong

        Returns:
            Danh sách Path các file phần theo thứ tự
        """
        logger.info(f"📐 Phân tích: {input_path.name}")
        info = await FFmpegProcessor.get_video_info(input_path)
        duration = info["duration"]
        file_size = input_path.stat().st_size

        if duration <= 0:
            raise ValueError(f"Không đọc được thời lượng: {input_path}")

        logger.info(
            f"📊 {format_size(file_size)}, "
            f"thời lượng: {format_duration(duration)}"
        )

        num_parts = math.ceil(file_size / max_part_size)
        seg_duration = duration / num_parts   # Không nhân 0.95 để cắt đều

        logger.info(f"✂️  {num_parts} phần × ~{format_duration(seg_duration)}")

        output_parts: list[Path] = []
        stem = sanitize_filename(input_path.stem)

        for i in range(num_parts):
            start = i * seg_duration
            # Phần cuối chạy đến hết để tránh mất frame
            duration_arg = seg_duration if i < num_parts - 1 else (duration - start + 1)
            out_file = output_dir / f"{stem}_part{i + 1:02d}.mp4"

            def _cut(s=start, d=duration_arg, out=out_file) -> None:
                cmd = [
                    "ffmpeg", "-y",
                    "-ss", str(s),
                    "-i", str(input_path),
                    "-t", str(d),
                    "-c", "copy",
                    "-avoid_negative_ts", "make_zero",
                    "-map_metadata", "0",
                    str(out),
                ]
                r = subprocess.run(
                    cmd, capture_output=True, text=True, encoding="utf-8",
                )
                if r.returncode != 0:
                    raise RuntimeError(f"FFmpeg cut lỗi: {(r.stderr or '')[:200]}")

            logger.info(
                f"  ▶️  Part {i + 1}/{num_parts}: "
                f"{format_duration(start)} → {format_duration(start + duration_arg)}"
            )
            await asyncio.to_thread(_cut)

            actual_size = out_file.stat().st_size
            logger.info(f"  ✅ Part {i + 1}: {format_size(actual_size)}")
            output_parts.append(out_file)

            if progress_callback:
                await progress_callback(i + 1, num_parts)

        return output_parts
