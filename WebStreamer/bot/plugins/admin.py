import asyncio
from pyrogram import filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError

from WebStreamer.bot import StreamBot
from WebStreamer.vars import Var
from WebStreamer.database import db


def _is_admin(user) -> bool:
    """Return True only if the user is permitted to run admin commands.

    Priority:
    1) If Var.ADMIN is set (>0), only that user ID is allowed.
    2) Else, fall back to legacy Var.ALLOWED_USERS list (ids or usernames).
    3) If neither is configured, deny by default.
    """
    uid = getattr(user, "id", None)
    username = getattr(user, "username", None)

    # 1) Explicit single admin id has highest priority
    try:
        if getattr(Var, "ADMIN", 0):
            return int(uid) == int(Var.ADMIN)
    except Exception:
        return False

    # 2) Fallback: legacy allowed users list (ids or usernames)
    if getattr(Var, "ALLOWED_USERS", None):
        return (str(uid) in Var.ALLOWED_USERS) or (username in Var.ALLOWED_USERS)

    # 3) Default deny if nothing configured
    return False


@StreamBot.on_message(filters.command(["ban"]) & filters.private)
async def ban_user(_, m: Message):
    if not _is_admin(m.from_user):
        return await m.reply("You are not allowed to use this command.")

    target_id: int | None = None
    reason: str | None = None
    
    # Prefer replied user
    if m.reply_to_message and m.reply_to_message.from_user:
        target_id = m.reply_to_message.from_user.id
        # Check for reason after command
        if len(m.command) >= 2:
            reason = m.text.split(None, 1)[1]
    # Or parse /ban <user_id> [reason]
    if not target_id and len(m.command) >= 2:
        try:
            target_id = int(m.command[1])
            if len(m.command) >= 3:
                reason = " ".join(m.command[2:])
        except ValueError:
            pass

    if not target_id:
        return await m.reply("Usage: /ban <user_id> [reason] (or reply to a user's message)")

    try:
        await db.add_banned_user(target_id, banned_by=m.from_user.id, reason=reason)
        await m.reply(f"✅ Banned user: <code>{target_id}</code>" + (f"\nReason: {reason}" if reason else ""), quote=True)
    except Exception as e:
        await m.reply(f"❌ Failed to ban user: {e}", quote=True)


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

    try:
        removed = await db.remove_banned_user(target_id)
        if removed:
            await m.reply(f"✅ Unbanned user: <code>{target_id}</code>", quote=True)
        else:
            await m.reply(f"⚠️ User <code>{target_id}</code> was not banned.", quote=True)
    except Exception as e:
        await m.reply(f"❌ Failed to unban user: {e}", quote=True)


async def is_banned(user_id: int) -> bool:
    """Check if user is banned (async)"""
    try:
        ban_info = await db.is_user_banned(user_id)
        return ban_info is not None
    except Exception:
        return False


@StreamBot.on_message(filters.command(["broadcast"]) & filters.private)
async def broadcast(_, m: Message):
    """Admin-only: broadcast a message to all known users.

    Usage:
    - Reply to a message with /broadcast to forward that message to all users.
    - /broadcast Your text here  to send plain text to all users.
    """
    if not _is_admin(m.from_user):
        return await m.reply("You are not allowed to use this command.")

    # Get all users from database
    try:
        users_cursor = db.get_all_users()
        users = []
        async for user in users_cursor:
            users.append(user.get('id'))
    except Exception as e:
        return await m.reply(f"Failed to load users from database: {e}")

    if not users:
        return await m.reply("No users recorded to broadcast to.")

    status_msg = await m.reply(f"📢 Broadcasting to {len(users)} users...")
    sent = 0
    failed = 0
    deleted = 0

    # Broadcast by replying to a message (forward/copy) or by text
    if m.reply_to_message:
        src = m.reply_to_message
        for uid in users:
            try:
                await StreamBot.copy_message(int(uid), src.chat.id, src.message_id)
                sent += 1
            except FloodWait as e:
                await asyncio.sleep(e.x)
                try:
                    await StreamBot.copy_message(int(uid), src.chat.id, src.message_id)
                    sent += 1
                except Exception:
                    failed += 1
            except RPCError as e:
                # User blocked bot or deleted account
                if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                    try:
                        await db.delete_user(int(uid))
                        deleted += 1
                    except Exception:
                        pass
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
            except RPCError as e:
                if "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                    try:
                        await db.delete_user(int(uid))
                        deleted += 1
                    except Exception:
                        pass
                failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)

    result_text = f"✅ Broadcast completed!\n\n📤 Sent: {sent}\n❌ Failed: {failed}"
    if deleted > 0:
        result_text += f"\n🗑 Removed inactive users: {deleted}"
    await status_msg.edit_text(result_text)


