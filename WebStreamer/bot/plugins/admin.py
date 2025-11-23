import json
from pathlib import Path
from typing import Set

from pyrogram import filters
from pyrogram.types import Message

from WebStreamer.bot import StreamBot
from WebStreamer.vars import Var
import asyncio
import json
from pathlib import Path
from pyrogram.errors import FloodWait, RPCError


BAN_FILE = Path(__file__).resolve().parent.parent.parent / "banned_users.json"


def _load_banned() -> Set[int]:
    if not BAN_FILE.exists():
        return set()
    try:
        data = json.loads(BAN_FILE.read_text(encoding="utf-8"))
        return set(int(x) for x in data)
    except Exception:
        return set()


def _save_banned(banned: Set[int]) -> None:
    BAN_FILE.write_text(json.dumps(sorted(list(banned))), encoding="utf-8")


def _is_admin(user) -> bool:
    if not Var.ALLOWED_USERS:
        return True
    username = getattr(user, "username", None)
    return (str(user.id) in Var.ALLOWED_USERS) or (username in Var.ALLOWED_USERS)


@StreamBot.on_message(filters.command(["ban"]) & filters.private)
async def ban_user(_, m: Message):
    if not _is_admin(m.from_user):
        return await m.reply("You are not allowed to use this command.")

    target_id: int | None = None
    # Prefer replied user
    if m.reply_to_message and m.reply_to_message.from_user:
        target_id = m.reply_to_message.from_user.id
    # Or parse /ban <user_id>
    if not target_id and len(m.command) >= 2:
        try:
            target_id = int(m.command[1])
        except ValueError:
            pass

    if not target_id:
        return await m.reply("Usage: /ban <user_id> (or reply to a user's message)")

    banned = _load_banned()
    banned.add(int(target_id))
    _save_banned(banned)
    await m.reply(f"✅ Banned user: <code>{target_id}</code>", quote=True)


@StreamBot.on_message(filters.command(["unban"]) & filters.private)
async def unban_user(_, m: Message):
    if not _is_admin(m.from_user):
        return await m.reply("You are not allowed to use this command.")

    target_id: int | None = None
    if m.reply_to_message and m.reply_to_message.from_user:
        target_id = m.reply_to_message.from_user.id
    if not target_id and len(m.command) >= 2:
        try:
            target_id = int(m.command[1])
        except ValueError:
            pass

    if not target_id:
        return await m.reply("Usage: /unban <user_id> (or reply to a user's message)")

    banned = _load_banned()
    banned.discard(int(target_id))
    _save_banned(banned)
    await m.reply(f"✅ Unbanned user: <code>{target_id}</code>", quote=True)


def is_banned(user_id: int) -> bool:
    return int(user_id) in _load_banned()


@StreamBot.on_message(filters.command(["broadcast"]) & filters.private)
async def broadcast(_, m: Message):
    """Admin-only: broadcast a message to all known users.

    Usage:
    - Reply to a message with /broadcast to forward that message to all users.
    - /broadcast Your text here  to send plain text to all users.
    """
    if not _is_admin(m.from_user):
        return await m.reply("You are not allowed to use this command.")

    # load users
    data_dir = Path(__file__).resolve().parents[2] / "data"
    users_file = data_dir / "users.json"
    try:
        users = json.loads(users_file.read_text(encoding="utf-8")) if users_file.exists() else []
    except Exception:
        users = []

    if not users:
        return await m.reply("No users recorded to broadcast to.")

    sent = 0
    failed = 0

    # Broadcast by replying to a message (forward/copy) or by text
    if m.reply_to_message:
        src = m.reply_to_message
        for uid in users:
            try:
                # copy_message to preserve media and formatting
                await StreamBot.copy_message(int(uid), src.chat.id, src.message_id)
                sent += 1
            except FloodWait as e:
                await asyncio.sleep(e.x)
                try:
                    await StreamBot.copy_message(int(uid), src.chat.id, src.message_id)
                    sent += 1
                except Exception:
                    failed += 1
            except RPCError:
                failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
    else:
        # text after command
        if len(m.command) < 2:
            return await m.reply("Usage: /broadcast <message> (or reply to a message)")
        message = m.text.split(None, 1)[1]
        for uid in users:
            try:
                await StreamBot.send_message(int(uid), message)
                sent += 1
            except FloodWait as e:
                await asyncio.sleep(e.x)
                try:
                    await StreamBot.send_message(int(uid), message)
                    sent += 1
                except Exception:
                    failed += 1
            except RPCError:
                failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)

    await m.reply(f"Broadcast completed. Sent: {sent}, Failed: {failed}")


