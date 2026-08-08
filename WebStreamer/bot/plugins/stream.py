import logging
import re
from pyrogram import filters, errors
from WebStreamer.vars import Var
from urllib.parse import quote_plus
from WebStreamer.bot import StreamBot, logger
from WebStreamer.utils import get_hash, get_name
from WebStreamer.utils.file_properties import get_media_from_message
from WebStreamer.bot.plugins.admin import is_banned
from WebStreamer.database import db
from pyrogram.enums.parse_mode import ParseMode
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

def clean_link(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    cloud_domains = [".koyeb.app", ".herokuapp.com", ".render.com", ".onrender.com", ".railway.app"]
    if any(cd in url.lower() for cd in cloud_domains) and url.startswith("http://"):
        url = "https://" + url[7:]
    url = re.sub(r'(https?://[^/:]+):\d+', r'\1', url)
    return url

def humanbytes(size):
    if not size:
        return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'Ki', 2: 'Mi', 3: 'Gi', 4: 'Ti'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'

@StreamBot.on_message(
    filters.private
    & (
        filters.document
        | filters.video
        | filters.audio
        | filters.animation
        | filters.voice
        | filters.video_note
        | filters.photo
        | filters.sticker
    ),
    group=4,
)
async def media_receive_handler(_, m: Message):
    if Var.ALLOWED_USERS and not ((str(m.from_user.id) in Var.ALLOWED_USERS) or (m.from_user.username in Var.ALLOWED_USERS)):
        return await m.reply("You are not <b>allowed to use</b> this <a href='https://github.com/EverythingSuckz/TG-FileStreamBot'>bot</a>.", quote=True)
    if await is_banned(m.from_user.id):
        return await m.reply("🚫 You are banned by admin.", quote=True)
    
    # Save user to database
    try:
        await db.add_user(m.from_user.id)
    except Exception as e:
        logger.warning(f"Failed to save user {m.from_user.id} to database: {e}")
    
    log_msg = await m.forward(chat_id=Var.BIN_CHANNEL)
    file_hash = get_hash(log_msg, Var.HASH_LENGTH)
    stream_link = clean_link(f"{Var.URL}{log_msg.id}/{quote_plus(get_name(m))}?hash={file_hash}")
    short_link = clean_link(f"{Var.URL}{file_hash}{log_msg.id}")
    logger.info(f"Generated link: {stream_link} for {m.from_user.first_name}")
    # Send a details note to BIN_CHANNEL only
    try:
        requester_name = (
            ("@" + m.from_user.username) if getattr(m.from_user, "username", None) else m.from_user.first_name
        )
        details_text = (
            "<b>RᴇQᴜᴇꜱᴛᴇᴅ ʙʏ:</b> {}\n" 
            "<b>Uꜱᴇʀ ɪᴅ:</b> <code>{}</code>\n"
            "<b>Stream ʟɪɴᴋ:</b> <a href=\"{}\">open</a>\n".format(
                requester_name, m.from_user.id, stream_link
            )
        )
        await StreamBot.send_message(Var.BIN_CHANNEL, details_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_to_message_id=log_msg.id)
    except Exception as e:
        logger.warning(f"Failed to send details to BIN_CHANNEL: {e}")
    try:
        watch_link = clean_link(f"{Var.URL}watch/{log_msg.id}/{quote_plus(get_name(m))}?hash={file_hash}")
        file_name = get_name(m)
        file_size = humanbytes(get_media_from_message(m).file_size)

        msg_text = (
            "<b>🚀 DOWNLOAD READY</b>\n"
            "✅ Your file has been processed successfully.\n\n"
            "<b>📁 FILE NAME</b>\n"
            f"<code>{file_name}</code>\n\n"
            "<b>📊 FILE SIZE</b>\n"
            f"<code>{file_size}</code>\n\n"
            "<b>🔗 SECURE DOWNLOAD</b>\n"
            f"<code>{stream_link}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "<b>⚡『ACCKERMAN°ツ</b>\n"
            "<i>🚀 Engineered for speed & reliability</i>"
        )

        await m.reply_text(
            text=msg_text,
            quote=True,
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Download / Stream", url=watch_link)]
                ]
            ),
        )
    except errors.ButtonUrlInvalid:
        await m.reply_text(
            text=msg_text,
            quote=True,
            parse_mode=ParseMode.HTML,
        )
