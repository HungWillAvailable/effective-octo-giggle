"""
run_stream.py — Chay Stream Server.

Cach dung:
    # Truyen message_id qua argument:
    python run_stream.py 123 124 125

    # Hoac de trong, server chay voi playlist rong (them sau qua /health):
    python run_stream.py
"""

import asyncio
import sys

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
load_dotenv()

from backend.config import Config
from stream_server import start_stream_server, uploaded_parts, save_m3u8
from pyrogram import Client

STREAM_PORT = 8080


async def main():
    # Doc message_id tu command line argument neu co
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            try:
                uploaded_parts.append(int(arg))
            except ValueError:
                print(f"[!] Bo qua gia tri khong hop le: {arg}")
        print(f"[OK] Da load {len(uploaded_parts)} parts tu argument: {uploaded_parts}")
    else:
        print("[i] Khong co message_id nao. Chay server voi playlist rong.")
        print("[i] Them sau: GET http://localhost:8080/health de kiem tra")

    print()
    print("=" * 55)
    print("  Telegram HLS Stream Server")
    print("=" * 55)

    client = Client(
        name="stream_session",
        api_id=Config.API_ID,
        api_hash=Config.API_HASH,
        bot_token=Config.BOT_TOKEN,
    )

    async with client:
        print("[OK] Pyrogram da ket noi")

        if uploaded_parts:
            playlist_file = save_m3u8(
                uploaded_parts,
                output_path="playlist.m3u8",
                base_url=f"http://localhost:{STREAM_PORT}",
            )
            print(f"[OK] Playlist: {playlist_file}")

        await start_stream_server(
            pyrogram_client=client,
            channel_id=Config.CHANNEL_ID,
            port=STREAM_PORT,
        )

        print()
        print("  Server dang chay. Nhan Ctrl+C de dung.")
        print(f"  Playlist : http://localhost:{STREAM_PORT}/playlist.m3u8")
        print(f"  Stream   : http://localhost:{STREAM_PORT}/stream/{{message_id}}")
        print(f"  Health   : http://localhost:{STREAM_PORT}/health")
        print()

        await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Da dung server.")
