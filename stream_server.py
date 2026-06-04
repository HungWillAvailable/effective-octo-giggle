# stream_server.py — HLS Streaming Bridge for Telegram parts
import asyncio
from aiohttp import web

# ── 1. Message ID Tracker ─────────────────────────────────────
uploaded_parts: list[int] = []  # stores message_id in order

def track_message(message) -> None:
    """Call this right after each send_video/send_document succeeds."""
    uploaded_parts.append(message.id)


# ── 2. M3U8 Playlist Generator ────────────────────────────────
def generate_m3u8(
    message_ids: list[int],
    base_url: str = "http://localhost:8080",
    target_duration: int = 7200,
) -> str:
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target_duration}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:VOD",
    ]
    for mid in message_ids:
        lines.append(f"#EXTINF:{target_duration},")
        lines.append(f"{base_url}/stream/{mid}")
    lines.append("#EXT-X-ENDLIST")
    return "\n".join(lines)


def save_m3u8(
    message_ids: list[int],
    output_path: str = "playlist.m3u8",
    base_url: str = "http://localhost:8080",
) -> str:
    content = generate_m3u8(message_ids, base_url)
    with open(output_path, "w") as f:
        f.write(content)
    return output_path


# ── 3. aiohttp Streaming Bridge ───────────────────────────────
CHUNK_SIZE = 1024 * 512  # 512 KB

def create_stream_app(pyrogram_client, channel_id: int) -> web.Application:
    async def stream_handler(request: web.Request) -> web.StreamResponse:
        message_id = int(request.match_info["message_id"])
        chat_id = int(request.query.get("chat_id", channel_id))

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "video/mp4",
                "Accept-Ranges": "bytes",
                "Access-Control-Allow-Origin": "*",
            },
        )
        await response.prepare(request)

        async for chunk in pyrogram_client.stream_media(
            (chat_id, message_id),
            limit=CHUNK_SIZE,
        ):
            await response.write(chunk)

        await response.write_eof()
        return response

    async def playlist_handler(request: web.Request) -> web.Response:
        base_url = f"{request.scheme}://{request.host}"
        content = generate_m3u8(uploaded_parts, base_url)
        return web.Response(
            text=content,
            content_type="application/vnd.apple.mpegurl",
            headers={"Access-Control-Allow-Origin": "*"},
        )

    async def health_handler(request: web.Request) -> web.Response:
        return web.json_response({
            "status": "ok",
            "parts_tracked": len(uploaded_parts),
            "message_ids": uploaded_parts,
        })

    app = web.Application()
    app.router.add_get("/stream/{message_id}", stream_handler)
    app.router.add_get("/playlist.m3u8", playlist_handler)
    app.router.add_get("/health", health_handler)
    return app


async def start_stream_server(
    pyrogram_client,
    channel_id: int,
    port: int = 8080,
) -> None:
    app = create_stream_app(pyrogram_client, channel_id)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"[StreamServer] Running at http://0.0.0.0:{port}")
    print(f"[StreamServer] Playlist : http://localhost:{port}/playlist.m3u8")
    print(f"[StreamServer] Health   : http://localhost:{port}/health")
