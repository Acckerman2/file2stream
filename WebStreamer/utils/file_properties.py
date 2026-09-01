import hashlib
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.file_id import FileId
from typing import Any, Optional, Union
from pyrogram.raw.types.messages import Messages
from WebStreamer.server.exceptions import FIleNotFound
from datetime import datetime


import time
import asyncio
from WebStreamer.vars import Var

class AsyncFileCache:
    def __init__(self, max_size: int = 5000):
        self._cache: dict = {}
        self._lock = asyncio.Lock()
        self.max_size = max_size

    async def get(self, message_id: int) -> Optional[FileId]:
        entry = self._cache.get(message_id)
        if entry:
            file_id, timestamp = entry
            if time.time() - timestamp < Var.CACHE_TTL:
                return file_id
            else:
                self._cache.pop(message_id, None)
        return None

    async def set(self, message_id: int, file_id: FileId):
        async with self._lock:
            if len(self._cache) >= self.max_size:
                # Evict expired items or oldest 10%
                now = time.time()
                expired = [k for k, v in self._cache.items() if now - v[1] > Var.CACHE_TTL]
                for k in expired:
                    self._cache.pop(k, None)
                if len(self._cache) >= self.max_size:
                    # Remove oldest entries
                    sorted_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][1])
                    for k in sorted_keys[: self.max_size // 10]:
                        self._cache.pop(k, None)
            self._cache[message_id] = (file_id, time.time())

global_file_cache = AsyncFileCache()


async def parse_file_id(message: "Message") -> Optional[FileId]:
    media = get_media_from_message(message)
    if media:
        return FileId.decode(media.file_id)

async def parse_file_unique_id(message: "Messages") -> Optional[str]:
    media = get_media_from_message(message)
    if media:
        return media.file_unique_id

async def get_file_ids(client: Client, chat_id: int, message_id: int) -> Optional[FileId]:
    cached = await global_file_cache.get(message_id)
    if cached:
        return cached

    async with global_file_cache._lock:
        cached = await global_file_cache.get(message_id)
        if cached:
            return cached

        message = await client.get_messages(chat_id, message_id)
        if message.empty:
            raise FIleNotFound
        media = get_media_from_message(message)
        file_unique_id = await parse_file_unique_id(message)
        file_id = await parse_file_id(message)
        setattr(file_id, "file_size", getattr(media, "file_size", 0))
        setattr(file_id, "mime_type", getattr(media, "mime_type", ""))
        setattr(file_id, "file_name", getattr(media, "file_name", ""))
        setattr(file_id, "unique_id", file_unique_id)

        await global_file_cache.set(message_id, file_id)
        return file_id

def get_media_from_message(message: "Message") -> Any:
    media_types = (
        "audio",
        "document",
        "photo",
        "sticker",
        "animation",
        "video",
        "voice",
        "video_note",
    )
    for attr in media_types:
        media = getattr(message, attr, None)
        if media:
            return media


def get_hash(media_msg: Union[str, Message], length: int) -> str:
    if isinstance(media_msg, Message):
        media = get_media_from_message(media_msg)
        unique_id = getattr(media, "file_unique_id", "")
    else:
        unique_id = media_msg
    long_hash = hashlib.sha256(unique_id.encode("UTF-8")).hexdigest()
    return long_hash[:length]


def get_name(media_msg: Union[Message, FileId]) -> str:

    if isinstance(media_msg, Message):
        media = get_media_from_message(media_msg)
        file_name = getattr(media, "file_name", "")

    elif isinstance(media_msg, FileId):
        file_name = getattr(media_msg, "file_name", "")

    if not file_name:
        if isinstance(media_msg, Message) and media_msg.media:
            media_type = media_msg.media.value
        elif media_msg.file_type:
            media_type = media_msg.file_type.name.lower()
        else:
            media_type = "file"

        formats = {
            "photo": "jpg", "audio": "mp3", "voice": "ogg",
            "video": "mp4", "animation": "mp4", "video_note": "mp4",
            "sticker": "webp"
        }

        ext = formats.get(media_type)
        ext = "." + ext if ext else ""

        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"{media_type}-{date}{ext}"

    return file_name
