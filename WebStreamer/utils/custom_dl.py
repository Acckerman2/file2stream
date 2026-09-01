import math
import asyncio
import logging
from WebStreamer import Var
from typing import Dict, Optional, Union
from WebStreamer.bot import work_loads
from pyrogram import Client, utils, raw
from .file_properties import get_file_ids
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid, FloodWait
from WebStreamer.server.exceptions import FIleNotFound
from pyrogram.file_id import FileId, FileType, ThumbnailSource

logger = logging.getLogger("streamer")

class ByteStreamer:
    def __init__(self, client: Client):
        """A custom class that holds the cache of a specific client and class functions.
        attributes:
            client: the client that the cache is for.
            cached_file_ids: a dict of cached file IDs.
            cached_file_properties: a dict of cached file properties.
        
        functions:
            generate_file_properties: returns the properties for a media of a specific message contained in Tuple.
            generate_media_session: returns the media session for the DC that contains the media file.
            yield_file: yield a file from telegram servers for streaming.
            
        This is a modified version of the <https://github.com/eyaadh/megadlbot_oss/blob/master/mega/telegram/utils/custom_download.py>
        Thanks to Eyaadh <https://github.com/eyaadh>
        """
        self.clean_timer = 30 * 60  # 30 minutes cache - same as filter2
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, message_id: int) -> FileId:
        """
        Returns the properties of a media of a specific message in a FIleId class.
        if the properties are cached, then it'll return the cached results.
        or it'll generate the properties from the Message ID and cache them.
        """
        if message_id not in self.cached_file_ids:
            await self.generate_file_properties(message_id)
            logger.debug(f"Cached file properties for message with ID {message_id}")
        return self.cached_file_ids[message_id]
    
    async def generate_file_properties(self, message_id: int) -> FileId:
        """
        Generates the properties of a media file on a specific message.
        returns ths properties in a FIleId class.
        """
        file_id = await get_file_ids(self.client, Var.BIN_CHANNEL, message_id)
        logger.debug(f"Generated file ID and Unique ID for message with ID {message_id}")
        if not file_id:
            logger.debug(f"Message with ID {message_id} not found")
            raise FIleNotFound
        self.cached_file_ids[message_id] = file_id
        logger.debug(f"Cached media message with ID {message_id}")
        return self.cached_file_ids[message_id]

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        """
        Generates the media session for the DC that contains the media file.
        This is required for getting the bytes from Telegram servers.
        """

        media_session = client.media_sessions.get(file_id.dc_id, None)

        if media_session is None:
            if file_id.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await Auth(
                        client, file_id.dc_id, await client.storage.test_mode()
                    ).create(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()

                for attempt in range(6):
                    try:
                        exported_auth = await client.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id)
                        )
                    except FloodWait as e:
                        logger.warning(f"FloodWait {e.value}s during auth export for DC {file_id.dc_id}")
                        await asyncio.sleep(e.value + 1)
                        continue

                    try:
                        await media_session.send(
                            raw.functions.auth.ImportAuthorization(
                                id=exported_auth.id, bytes=exported_auth.bytes
                            )
                        )
                        break
                    except AuthBytesInvalid:
                        logger.debug(
                            f"Invalid authorization bytes for DC {file_id.dc_id}, attempt {attempt + 1}/6"
                        )
                        await asyncio.sleep(1)  # Small delay before retry
                        continue
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid
            else:
                media_session = Session(
                    client,
                    file_id.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True,
                )
                await media_session.start()
            logger.debug(f"Created media session for DC {file_id.dc_id}")
            client.media_sessions[file_id.dc_id] = media_session
        else:
            logger.debug(f"Using cached media session for DC {file_id.dc_id}")
        return media_session


    @staticmethod
    async def get_location(file_id: FileId) -> Union[raw.types.InputPhotoFileLocation,
                                                     raw.types.InputDocumentFileLocation,
                                                     raw.types.InputPeerPhotoFileLocation,]:
        """
        Returns the file location for the media file.
        """
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id, access_hash=file_id.chat_access_hash
                )
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash,
                    )

            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        return location

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int,
    ) -> Union[str, None]:
        """
        Custom generator that yields the bytes of the media file.
        Uses retry-with-backoff on every chunk fetch and prefetches the next
        chunk while the current one is being sent to the client, eliminating
        the round-trip stall that causes buffering and intermittent pauses.
        """
        client = self.client
        work_loads[index] += 1
        logger.debug(f"Starting to yield file with client {index}.")
        media_session = await self.generate_media_session(client, file_id)

        current_part = 1
        location = await self.get_location(file_id)

        # ── Retry helper ────────────────────────────────────────────────────
        async def _fetch_chunk(off: int, max_retries: int = 5) -> Optional[raw.types.upload.File]:
            """Fetch one chunk with exponential-backoff retry.

            Handles FloodWait, transient timeouts, and generic network errors
            so a momentary hiccup does not abort the entire download.
            """
            last_exc: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    r = await media_session.send(
                        raw.functions.upload.GetFile(
                            location=location, offset=off, limit=chunk_size
                        ),
                    )
                    return r
                except FloodWait as e:
                    wait = min(e.value, 30) + 1
                    logger.warning(
                        f"FloodWait {e.value}s on chunk offset={off}, "
                        f"sleeping {wait}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(wait)
                    last_exc = e
                except (TimeoutError, asyncio.TimeoutError) as e:
                    wait = 2 ** attempt          # 1 s, 2 s, 4 s, 8 s, 16 s
                    logger.warning(
                        f"Timeout on chunk offset={off}, "
                        f"retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    last_exc = e
                except (ConnectionResetError, BrokenPipeError, OSError) as e:
                    wait = 2 ** attempt
                    logger.warning(
                        f"Connection error on chunk offset={off}: {e}, "
                        f"retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    last_exc = e
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    wait = 2 ** attempt
                    logger.warning(
                        f"Unexpected error on chunk offset={off}: {e}, "
                        f"retry {attempt + 1}/{max_retries} in {wait}s"
                    )
                    await asyncio.sleep(wait)
                    last_exc = e
            raise last_exc or RuntimeError(f"Failed to fetch chunk at offset={off}")

        # ── Multi-chunk prefetch-pipeline streaming ─────────────────────────
        prefetch_depth = max(1, getattr(Var, "PREFETCH_CHUNKS", 2))
        prefetch_tasks: list = []

        try:
            # Fill initial prefetch queue
            curr_offset = offset
            for p in range(min(prefetch_depth, part_count)):
                prefetch_tasks.append(asyncio.create_task(_fetch_chunk(curr_offset)))
                curr_offset += chunk_size

            next_fetch_part = len(prefetch_tasks) + 1

            while current_part <= part_count and prefetch_tasks:
                task = prefetch_tasks.pop(0)
                r = await task

                if not isinstance(r, raw.types.upload.File) or not r.bytes:
                    break

                chunk = r.bytes

                # Schedule next prefetch task if remaining parts exist
                if next_fetch_part <= part_count:
                    prefetch_tasks.append(asyncio.create_task(_fetch_chunk(curr_offset)))
                    curr_offset += chunk_size
                    next_fetch_part += 1

                # Slice the chunk according to the requested byte range
                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif current_part == 1:
                    yield chunk[first_part_cut:]
                elif current_part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk

                current_part += 1

        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, GeneratorExit):
            logger.debug("Download stream was cancelled or reset mid-transfer")
        except Exception as e:
            logger.warning(f"Stream error after {current_part} parts: {e}")
        finally:
            # Cancel all in-flight prefetch tasks
            for task in prefetch_tasks:
                if not task.done():
                    task.cancel()
            if prefetch_tasks:
                await asyncio.gather(*prefetch_tasks, return_exceptions=True)
            logger.debug(f"Finished yielding file with {current_part} parts.")
            work_loads[index] = max(0, work_loads[index] - 1)

    
    async def clean_cache(self) -> None:
        """
        function to clean the cache to reduce memory usage
        """
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            logger.debug("Cleaned the cache")
