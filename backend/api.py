"""
api.py — FastAPI application: REST endpoints + WebSocket + Video Streaming.

Orchestrator Pipeline cho mỗi task:
    download → check_size → [ffmpeg_split] → upload → cleanup

WebSocket /ws phát trạng thái real-time đến tất cả browsers.
"""

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import math
import secrets
import shutil
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import (
    FastAPI,
    File,
    HTTPException,
    Request,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import (
    FileResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pyrogram import Client

from backend.config import Config
from backend.downloader import Downloader
from backend.ffmpeg_processor import FFmpegProcessor
from backend.task_manager import TaskManager, TaskStatus
from backend.uploader import Uploader
from backend.utils import format_size, sanitize_filename

logger = logging.getLogger("API")


# ══════════════════════════════════════════════════════════════════════════════
#  AUTH CONFIG
# ══════════════════════════════════════════════════════════════════════════════
_ADMIN_USER     = "admin"
_ADMIN_PASSWORD = "admin123"
_COOKIE_NAME    = "vod_auth"
_SECRET_KEY     = secrets.token_hex(32)


def _make_token(username: str) -> str:
    return hmac.new(_SECRET_KEY.encode(), username.encode(), hashlib.sha256).hexdigest()


def _valid_cookie(request: Request) -> bool:
    token = request.cookies.get(_COOKIE_NAME, "")
    return hmac.compare_digest(token, _make_token(_ADMIN_USER))


# ══════════════════════════════════════════════════════════════════════════════
#  AUTH MIDDLEWARE (Native ASGI — không dùng BaseHTTPMiddleware)
# ══════════════════════════════════════════════════════════════════════════════
class AdminAuthMiddleware:
    """Bảo vệ /admin.html — native ASGI, không gây lỗi TaskGroup với streaming."""
    _PROTECTED = ("/admin.html", "/admin")

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        path = scope.get("path", "").rstrip("/") or "/"
        if any(path == p or path.startswith(p) for p in self._PROTECTED):
            request = Request(scope, receive)
            if not _valid_cookie(request):
                response = RedirectResponse("/login", status_code=302)
                return await response(scope, receive, send)

        return await self.app(scope, receive, send)


# ══════════════════════════════════════════════════════════════════════════════
#  PYROGRAM GLOBALS
# ══════════════════════════════════════════════════════════════════════════════
_pyrogram_client: Optional[Client] = None   # bot: upload / messages
_stream_client:   Optional[Client] = None   # stream: dedicated media session


def get_pyrogram_client() -> Client:
    if _pyrogram_client is None:
        raise HTTPException(503, "Bot client chưa khởi tạo. Kiểm tra .env")
    return _pyrogram_client


def get_stream_client() -> Client:
    """Stream client riêng, fallback sang bot client nếu chưa sẵn sàng."""
    return _stream_client or get_pyrogram_client()


# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND LOOPS
# ══════════════════════════════════════════════════════════════════════════════
async def _system_status_loop() -> None:
    """Broadcast system status mỗi 10s."""
    manager = TaskManager.get_instance()
    while True:
        await asyncio.sleep(10)
        await manager.broadcast_system_status()


async def _media_keepalive_loop() -> None:
    """Ping Telegram mỗi 20s để giữ media DC session sống."""
    while True:
        await asyncio.sleep(20)
        try:
            client = _stream_client or _pyrogram_client
            if client and client.is_connected:
                await client.get_me()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  LIFESPAN — Khởi động & dọn dẹp
# ══════════════════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(application: FastAPI):
    """Start Pyrogram clients 1 lần duy nhất khi Uvicorn start."""
    global _pyrogram_client, _stream_client

    Config.ensure_temp_dir()
    if errors := Config.validate():
        logger.error(f"❌ Lỗi cấu hình: {errors}")

    # ── Bot client ─────────────────────────────────────────────────────────
    try:
        _pyrogram_client = Client(
            name="video_uploader_web",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            workdir=str(Config.TEMP_DIR.parent),
        )
        await _pyrogram_client.start()
        logger.info("✅ Bot client đã kết nối Telegram")
    except Exception as exc:
        logger.error(f"❌ Bot client thất bại: {exc}")
        _pyrogram_client = None

    # ── Stream client (session riêng) ──────────────────────────────────────
    try:
        _stream_client = Client(
            name="video_stream_session",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            bot_token=Config.BOT_TOKEN,
            workdir=str(Config.TEMP_DIR.parent),
        )
        await _stream_client.start()
        logger.info("✅ Stream client sẵn sàng")
    except Exception as exc:
        logger.warning(f"⚠️ Stream client lỗi, fallback sang bot client: {exc}")
        _stream_client = None

    # ── Background tasks ───────────────────────────────────────────────────
    bg_tasks = [
        asyncio.create_task(_system_status_loop()),
        asyncio.create_task(_media_keepalive_loop()),
    ]

    yield  # ══════════ SERVER ĐANG CHẠY ══════════

    # ── Shutdown ───────────────────────────────────────────────────────────
    for t in bg_tasks:
        t.cancel()
    await asyncio.gather(*bg_tasks, return_exceptions=True)

    for client, label in ((_stream_client, "Stream"), (_pyrogram_client, "Bot")):
        if client:
            try:
                await client.stop()
                logger.info(f"🛑 {label} client đã dừng")
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
#  FASTAPI APP
# ══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="Telegram Video Uploader", version="2.0.0", lifespan=lifespan)
app.add_middleware(AdminAuthMiddleware)


# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET
# ══════════════════════════════════════════════════════════════════════════════
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    manager = TaskManager.get_instance()
    await manager.ws_manager.connect(ws)
    try:
        await ws.send_json({
            "type": "task_list",
            "tasks": [t.to_dict() for t in manager.get_all_tasks()],
        })
        await manager.broadcast_system_status()
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.ws_manager.disconnect(ws)


# ══════════════════════════════════════════════════════════════════════════════
#  REST API — Health & Status
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/api/health")
async def health() -> dict:
    errors = Config.validate()
    return {
        "ok": len(errors) == 0,
        "config_errors": errors,
        "pyrogram_connected": _pyrogram_client is not None,
        "channel_id": Config.CHANNEL_ID,
        "max_part_size": format_size(Config.MAX_PART_SIZE),
    }


@app.get("/api/status")
async def system_status() -> dict:
    Config.ensure_temp_dir()
    usage = shutil.disk_usage(Config.TEMP_DIR)
    manager = TaskManager.get_instance()
    tasks = manager.get_all_tasks()

    active = sum(1 for t in tasks if t.status not in (
        TaskStatus.DONE, TaskStatus.ERROR,
        TaskStatus.CANCELLED, TaskStatus.PENDING,
    ))
    done   = sum(1 for t in tasks if t.status == TaskStatus.DONE)
    errors = sum(1 for t in tasks if t.status == TaskStatus.ERROR)

    return {
        "disk_free": usage.free,
        "disk_total": usage.total,
        "disk_used": usage.used,
        "active_tasks": active,
        "done_tasks": done,
        "error_tasks": errors,
        "total_tasks": len(tasks),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  REST API — Tasks
# ══════════════════════════════════════════════════════════════════════════════
class UrlRequest(BaseModel):
    url: str


@app.get("/api/tasks")
async def list_tasks() -> dict:
    manager = TaskManager.get_instance()
    return {"tasks": [t.to_dict() for t in manager.get_all_tasks()]}


@app.post("/api/tasks")
async def submit_url(req: UrlRequest) -> dict:
    url = req.url.strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL không hợp lệ")

    manager = TaskManager.get_instance()
    filename = url.split("?")[0].rstrip("/").split("/")[-1] or "video.mp4"
    filename = sanitize_filename(filename)

    task = manager.create_task(filename=filename, source=url)
    asyncio.create_task(
        _run_pipeline(task_id=task.id, source_url=url, uploaded_file=None)
    )
    return {"task_id": task.id, "filename": filename}


@app.post("/api/tasks/file")
async def submit_file(file: UploadFile = File(...)) -> dict:
    manager = TaskManager.get_instance()
    filename = sanitize_filename(file.filename or "video.mp4")

    if file.content_type and not file.content_type.startswith("video/"):
        raise HTTPException(400, f"File không phải video: {file.content_type}")

    task = manager.create_task(filename=filename, source="browser_upload")
    task_dir = Config.TEMP_DIR / task.id
    task_dir.mkdir(parents=True, exist_ok=True)
    dest = task_dir / filename

    import aiofiles
    chunk_size = 1024 * 1024
    written = 0

    await manager.update_task(
        task.id,
        status=TaskStatus.DOWNLOADING,
        message="Đang nhận file từ browser...",
        force_broadcast=True,
    )

    async with aiofiles.open(dest, "wb") as f:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            await f.write(chunk)
            written += len(chunk)

    asyncio.create_task(
        _run_pipeline(task_id=task.id, source_url=None, uploaded_file=dest)
    )
    return {"task_id": task.id, "filename": filename}


@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str) -> dict:
    manager = TaskManager.get_instance()
    if not manager.remove_task(task_id):
        raise HTTPException(404, "Task không tồn tại")
    return {"ok": True}


@app.post("/api/cleanup")
async def manual_cleanup() -> dict:
    manager = TaskManager.get_instance()
    removed_tasks = manager.clear_completed()
    if Config.TEMP_DIR.exists():
        shutil.rmtree(Config.TEMP_DIR)
    Config.ensure_temp_dir()
    return {"ok": True, "tasks_removed": removed_tasks}


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
async def _run_pipeline(
    task_id: str,
    source_url: Optional[str],
    uploaded_file: Optional[Path],
) -> None:
    """download → check_size → [ffmpeg_split] → upload → cleanup"""
    manager = TaskManager.get_instance()
    task_dir = Config.TEMP_DIR / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    files_to_cleanup: list[Path] = []

    try:
        # ── Bước 1: Download ───────────────────────────────────────────
        if source_url:
            video_path = await Downloader.download_from_url(
                url=source_url, dest_dir=task_dir, task_id=task_id,
            )
        else:
            video_path = uploaded_file

        if not video_path or not video_path.exists():
            raise FileNotFoundError("File không tồn tại sau khi tải")

        files_to_cleanup.append(video_path)
        file_size = video_path.stat().st_size

        await manager.update_task(
            task_id,
            size=file_size,
            message=f"Tải xong: {format_size(file_size)}",
            force_broadcast=True,
        )

        # ── Bước 2: Cắt nếu cần ───────────────────────────────────────
        if file_size <= Config.MAX_PART_SIZE:
            parts_to_upload = [video_path]
        else:
            await manager.update_task(
                task_id,
                status=TaskStatus.PROCESSING,
                message=f"FFmpeg đang cắt ({format_size(file_size)})...",
                force_broadcast=True,
            )

            async def _split_progress(done: int, total: int) -> None:
                pct = done / total * 100
                await manager.update_task(
                    task_id, progress=pct,
                    message=f"FFmpeg: đã cắt {done}/{total} phần",
                    parts_total=total, parts_done=done,
                )

            async with manager.ffmpeg_semaphore:
                parts_to_upload = await FFmpegProcessor.split_video(
                    input_path=video_path,
                    output_dir=task_dir,
                    max_part_size=Config.MAX_PART_SIZE,
                    progress_callback=_split_progress,
                )
            files_to_cleanup.extend(parts_to_upload)

        # ── Bước 3: Upload ─────────────────────────────────────────────
        if _pyrogram_client is None:
            raise RuntimeError("Pyrogram client không khả dụng")

        async with manager.upload_semaphore:
            uploader = Uploader(_pyrogram_client)
            sent_message_ids = await uploader.upload_parts(
                parts=parts_to_upload,
                task_id=task_id,
                original_filename=video_path.stem,
            )

        # ── Bước 4: Thêm vào movies_db ────────────────────────────────
        movie_id = sanitize_filename(video_path.stem).replace(" ", "_").lower()
        stream_url = f"/stream/{sent_message_ids[0]}" if sent_message_ids else ""

        db = await asyncio.to_thread(_read_movies)
        if not any(m.get("id") == movie_id for m in db):
            db.append({
                "id":          movie_id,
                "title":       video_path.stem.replace("_", " ").replace("-", " "),
                "desc":        f"Uploaded {len(sent_message_ids)} part(s)",
                "thumb":       "",
                "stream":      stream_url,
                "message_ids": sent_message_ids,
                "channel_id":  Config.CHANNEL_ID,
            })
            await _write_movies(db)
            logger.info(f"🎬 Đã thêm '{movie_id}' vào movies_db")

        # ── Bước 5: Hoàn tất ──────────────────────────────────────────
        await manager.update_task(
            task_id,
            status=TaskStatus.DONE, progress=100.0,
            message=f"✅ Đã upload {len(parts_to_upload)} phần thành công!",
            force_broadcast=True,
        )
        logger.info(f"🎉 Task {task_id} hoàn tất!")

    except Exception as e:
        logger.error(f"❌ Task {task_id} lỗi: {e}", exc_info=True)
        await manager.update_task(
            task_id,
            error=str(e)[:300],
            message=f"Lỗi: {str(e)[:100]}",
            force_broadcast=True,
        )

    finally:
        logger.info(f"🧹 Dọn dẹp task {task_id}...")

        def _cleanup() -> None:
            for path in files_to_cleanup:
                try:
                    if path.is_file():
                        path.unlink()
                except Exception:
                    pass
            try:
                if task_dir.exists():
                    shutil.rmtree(task_dir, ignore_errors=True)
            except Exception:
                pass

        await asyncio.to_thread(_cleanup)
        await manager.broadcast_system_status()


# ══════════════════════════════════════════════════════════════════════════════
#  MOVIE LIBRARY API
# ══════════════════════════════════════════════════════════════════════════════
_MOVIES_DB = Path("movies_db.json")
_movies_lock = asyncio.Lock()


def _read_movies() -> list[dict]:
    if not _MOVIES_DB.exists():
        return []
    try:
        return json.loads(_MOVIES_DB.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return []


async def _write_movies(data: list[dict]) -> None:
    async with _movies_lock:
        import aiofiles
        payload = json.dumps(data, ensure_ascii=False, indent=2)
        async with aiofiles.open(_MOVIES_DB, "w", encoding="utf-8") as f:
            await f.write(payload)


class MovieRequest(BaseModel):
    title: str
    desc: str = ""
    thumb: str = ""
    stream: str
    id: Optional[str] = None


@app.get("/api/movies")
async def get_movies() -> list:
    return await asyncio.to_thread(_read_movies)


@app.post("/api/movies", status_code=201)
async def add_movie(req: MovieRequest) -> dict:
    movie = {
        "id":     req.id or req.title.lower().replace(" ", "_"),
        "title":  req.title,
        "desc":   req.desc,
        "thumb":  req.thumb,
        "stream": req.stream,
    }
    db = await asyncio.to_thread(_read_movies)
    db.append(movie)
    await _write_movies(db)
    return movie


@app.delete("/api/movies/{movie_id}")
async def delete_movie(movie_id: str) -> dict:
    db = await asyncio.to_thread(_read_movies)
    new_db = [m for m in db if m.get("id") != movie_id]
    if len(new_db) == len(db):
        raise HTTPException(404, "Phim không tồn tại")
    await _write_movies(new_db)
    return {"ok": True}


# ── Message metadata cache — tránh gọi get_messages lặp lại ───────────────
_msg_cache: dict[int, tuple[object, float]] = {}  # {msg_id: (msg, timestamp)}
_MSG_CACHE_TTL = 300  # 5 phút


async def _get_cached_message(client: Client, chat: int, msg_id: int):
    """Cache message object để tránh gọi Telegram API mỗi Range request."""
    now = time.time()
    cached = _msg_cache.get(msg_id)
    if cached and (now - cached[1]) < _MSG_CACHE_TTL:
        return cached[0]

    raw = await client.get_messages(chat, msg_id)
    msg = raw[0] if isinstance(raw, (list, tuple)) else raw
    _msg_cache[msg_id] = (msg, now)

    # Dọn cache cũ (giữ tối đa 50 entry)
    if len(_msg_cache) > 50:
        oldest_key = min(_msg_cache, key=lambda k: _msg_cache[k][1])
        _msg_cache.pop(oldest_key, None)

    return msg


@app.get("/stream/{message_id}")
async def stream_video(
    request: Request,
    message_id: int,
    channel_id: Optional[int] = None,
) -> StreamingResponse:
    """
    Stream video từ Telegram — HTTP 206 Partial Content.
    Tối ưu: cache metadata, chunk 2MB, no-buffering headers.
    """
    client = get_stream_client()
    chat   = channel_id or Config.CHANNEL_ID

    # ── 1. Lấy message (cached) ───────────────────────────────────────────
    try:
        msg = await _get_cached_message(client, chat, message_id)
    except Exception as e:
        raise HTTPException(404, f"Lỗi khi lấy tin nhắn: {e}")

    media = (
        getattr(msg, "video", None)
        or getattr(msg, "document", None)
        or getattr(msg, "animation", None)
    )
    if not msg or not media:
        raise HTTPException(404, f"Không tìm thấy media msg_id={message_id}")

    total_size: int = getattr(media, "file_size", 0) or 0
    mime_type:  str = getattr(media, "mime_type", "video/mp4") or "video/mp4"

    # ── 2. Parse & Clamp Range ─────────────────────────────────────────────
    range_header = request.headers.get("Range", "")
    start = 0
    end   = total_size - 1 if total_size else 0

    if range_header and range_header.startswith("bytes="):
        raw_range = range_header[6:]
        s, _, e = raw_range.partition("-")
        start = int(s) if s else 0
        end   = int(e) if e else (total_size - 1 if total_size else start + 2 * 1024 * 1024)

    if total_size and end >= total_size:
        end = total_size - 1
    if start > end:
        raise HTTPException(416, "Requested Range Not Satisfiable")

    content_length = end - start + 1
    status = 206 if range_header else 200

    # ── 3. Response headers (tối ưu cho reverse proxy) ─────────────────────
    headers = {
        "Accept-Ranges":               "bytes",
        "Content-Type":                mime_type,
        "Content-Length":              str(content_length),
        "Access-Control-Allow-Origin": "*",
        "Cache-Control":               "public, max-age=3600",      # cache 1h thay vì no-cache
        "X-Accel-Buffering":           "no",                        # Nginx: tắt buffering, stream thẳng
        "X-Content-Type-Options":      "nosniff",
    }
    if total_size:
        headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"

    # ── 4. Chunk-based generator ───────────────────────────────────────────
    PYROGRAM_CHUNK = 1024 * 1024           # Pyrogram dùng 1MB/chunk nội bộ
    chunk_offset   = start // PYROGRAM_CHUNK
    skip_bytes     = start %  PYROGRAM_CHUNK

    async def byte_generator():
        remaining = content_length
        need_skip = skip_bytes

        try:
            async for chunk in client.stream_media(msg, offset=chunk_offset):
                if remaining <= 0:
                    break

                # Skip byte đầu chunk đầu tiên nếu Range lệch biên 1MB
                if need_skip > 0:
                    chunk = chunk[need_skip:]
                    need_skip = 0

                # Cắt chunk cuối nếu thừa
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

                yield chunk
                remaining -= len(chunk)

                if remaining <= 0:
                    break
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, BrokenPipeError):
            pass     # client đóng kết nối — bình thường
        except Exception as exc:
            logger.error(f"Stream error msg={message_id} [{start}-{end}]: {exc}")

    return StreamingResponse(
        byte_generator(),
        status_code=status,
        headers=headers,
        media_type=mime_type,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  AUTH ROUTES
# ══════════════════════════════════════════════════════════════════════════════
class _LoginBody(BaseModel):
    username: str
    password: str


@app.get("/login")
async def login_page():
    return FileResponse("frontend/login.html")


@app.post("/api/login")
async def do_login(body: _LoginBody, response: Response):
    if body.username == _ADMIN_USER and body.password == _ADMIN_PASSWORD:
        token = _make_token(body.username)
        response.set_cookie(
            key=_COOKIE_NAME, value=token,
            httponly=True, samesite="lax", max_age=86400 * 7,
        )
        return {"ok": True}
    raise HTTPException(status_code=401, detail="Sai tài khoản hoặc mật khẩu")


@app.post("/api/logout")
async def do_logout(response: Response):
    response.delete_cookie(_COOKIE_NAME)
    return {"ok": True}


# ══════════════════════════════════════════════════════════════════════════════
#  STATIC FILES (mount cuối cùng — sau tất cả API routes)
# ══════════════════════════════════════════════════════════════════════════════
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
