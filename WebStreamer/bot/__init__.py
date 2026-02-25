import os
import os.path
from ..vars import Var
import logging
from pyrogram import Client

logger = logging.getLogger("bot")

sessions_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")
if Var.USE_SESSION_FILE:
    logger.info("Using session files")
    logger.info("Session folder path: {}".format(sessions_dir))
    if not os.path.isdir(sessions_dir):
        os.makedirs(sessions_dir)

StreamBot = Client(
    name="WebStreamer",
    api_id=Var.API_ID,
    api_hash=Var.API_HASH,
    workdir=sessions_dir if Var.USE_SESSION_FILE else "WebStreamer",
    plugins={"root": "WebStreamer/bot/plugins"},
    bot_token=Var.BOT_TOKEN,
    sleep_threshold=5,  # Low threshold for faster response (like filter2)
    workers=150,  # Fixed 150 workers like filter2's TechVJXBot
    in_memory=not Var.USE_SESSION_FILE,
)

multi_clients = {}
work_loads = {}
