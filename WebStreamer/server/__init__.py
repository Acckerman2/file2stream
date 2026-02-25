import asyncio
import logging
from aiohttp import web
from .stream_routes import routes

logger = logging.getLogger("server")


def _silence_connection_reset(loop, context):
    """Suppress noisy 'socket.send() raised exception' warnings from client disconnects."""
    exception = context.get("exception")
    if isinstance(exception, (ConnectionResetError, BrokenPipeError, OSError)):
        # Client closed connection; ignore.
        return
    # Fallback to default handler for other exceptions
    loop.default_exception_handler(context)


def web_server():
    logger.info("Initializing..")

    # Install custom exception handler to suppress socket warnings
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.set_exception_handler(_silence_connection_reset)

    # Match filter2's client_max_size exactly
    web_app = web.Application(
        client_max_size=30000000  # 30MB - same as filter2
    )
    web_app.add_routes(routes)
    logger.info("Added routes")
    return web_app
