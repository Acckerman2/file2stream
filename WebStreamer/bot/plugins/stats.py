import time
import shutil
from pathlib import Path
from WebStreamer.bot import StreamBot, work_loads
from WebStreamer import StartTime
import importlib
stream_routes = importlib.import_module("WebStreamer.server.stream_routes")
from WebStreamer.utils import get_readable_time
from WebStreamer.vars import Var
from WebStreamer.database import db
from pyrogram import filters
from pyrogram.types import Message


def _get_storage():
    try:
        total, used, free = shutil.disk_usage(str(Path.cwd()))
        # convert to human readable
        def hr(n):
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if n < 1024.0:
                    return f"{n:.2f}{unit}"
                n /= 1024.0
        return f"{hr(used)} / {hr(free)}"
    except Exception:
        return "N/A"


def _get_active_connections():
    try:
        return sum(work_loads.values()) if work_loads else 0
    except Exception:
        return 0


@StreamBot.on_message(filters.command(["stats"]) & filters.private)
async def stats_handler(_, m: Message):
    """Return bot statistics."""
    # total users from database
    try:
        total_users = await db.total_users_count()
    except Exception:
        total_users = 0

    # uptime
    uptime = get_readable_time(time.time() - StartTime)

    # ping (measure a quick API call)
    ping = "N/A"
    try:
        t0 = time.time()
        await StreamBot.get_me()
        ping = f"{int((time.time() - t0) * 1000)} ms"
    except Exception:
        ping = "N/A"

    # cpu/ram (psutil optional)
    cpu_ram = "psutil not installed"
    try:
        import psutil

        cpu = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()

        def _hr(n):
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if n < 1024.0:
                    return f"{n:.2f}{unit}"
                n /= 1024.0

        mem_total = _hr(mem.total)
        mem_used = _hr(mem.used)
        cpu_ram = f"CPU: {cpu}% | RAM: {mem.percent}% ({mem_used}/{mem_total})"
    except Exception:
        pass

    active = _get_active_connections()
    storage = _get_storage()
    requests = getattr(stream_routes, "requests_served", "N/A")

    text = (
        "📊 <b>Bot Statistics (v2.1.0 [Stable])</b>\n"
        "──────────────────────\n"
        f"👤 <b>Total Users:</b> {total_users}\n\n"
        f"⌛Ping  : {ping}\n\n"
        f"⚙️ <b>CPU & RAM:</b> {cpu_ram}\n\n"
        f"🚀 <b>Bot Speed:</b> N/A\n\n"
        f"🌐 <b>Active Connections:</b> {active}\n\n"
        f"💾 <b>Storage Used / Free:</b> {storage}\n\n"
        f"🔁 <b>Requests Served:</b> {requests}\n"
        "──────────────────────\n"
        f"🕓 <b>Uptime:</b> {uptime}\n"
        "🧠 <b>Creator:</b> Acckerman"
    )

    # Some Pyrogram builds reject certain parse_mode values (ValueError: Invalid parse mode)
    # to keep the command robust across installs send as plain text.
    await m.reply_text(text, quote=True)
