from pyrogram import filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton  # <-- ADDED IMPORTS

from WebStreamer.vars import Var 
from WebStreamer.bot import StreamBot
from pathlib import Path
import json

# path to store known users (unique starters)
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
USERS_FILE = DATA_DIR / "users.json"

def _add_user(user_id: int):
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if USERS_FILE.exists():
            data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        else:
            data = []
        if user_id not in data:
            data.append(user_id)
            USERS_FILE.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass

@StreamBot.on_message(filters.command(["start", "help"]) & filters.private)
async def start(_, m: Message):
    if Var.ALLOWED_USERS and not ((str(m.from_user.id) in Var.ALLOWED_USERS) or (m.from_user.username in Var.ALLOWED_USERS)):
        return await m.reply(
            "You are not in the allowed list of users who can use me. \
            Check <a href='https://github.com/EverythingSuckz/TG-FileStreamBot#optional-vars'>this link</a> for more info.",
            disable_web_page_preview=True, quote=True
        )
    
    # track user who ran /start
    try:
        _add_user(m.from_user.id)
    except Exception:
        pass

    await m.reply_photo(
        photo="https://envs.sh/NEV.jpg",
        caption="✨ ʜɪ ɪ'ᴍ Sydney Sweeney! 📁🔗\n\n"
                "🚀 ᴜᴘʟᴏᴀᴅ ᴀɴʏ ꜰɪʟᴇ ᴀɴᴅ ɢᴇᴛ ɪɴꜱᴛᴀɴᴛ ᴅɪʀᴇᴄᴛ ʟɪɴᴋꜱ 🌐\n\n"
                "💎 ꜰᴀꜱᴛ ⚡ | ꜱᴇᴄᴜʀᴇ 🔒 | ᴇᴀꜱʏ ᴛᴏ ᴜꜱᴇ 💫\n\n"
                "💬 ᴊᴜꜱᴛ ꜱᴇɴᴅ ᴀ ᴘʜᴏᴛᴏ, ᴠɪᴅᴇᴏ, ᴏʀ ᴅᴏᴄ — ᴀɴᴅ ɪ'ʟʟ ʜᴀɴᴅʟᴇ ᴛʜᴇ ʀᴇꜱᴛ 😎",
        
        # --- THIS IS THE NEW PART ---
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("👑 Owner", url="https://t.me/Acckerman_r2")]
                # You can change the URL to your own Telegram link
            ]
        )
        # ----------------------------
    )
