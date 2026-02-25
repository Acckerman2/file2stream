import time
import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from .vars import Var
from WebStreamer.bot.clients import StreamBot

__version__ = "2.2.4"
StartTime = time.time()
