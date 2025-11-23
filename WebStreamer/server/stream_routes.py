import re
import time
import math
import logging
import secrets
import mimetypes
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from WebStreamer.bot import multi_clients, work_loads
from WebStreamer.server.exceptions import FIleNotFound, InvalidHash
from WebStreamer import Var, utils, StartTime, __version__, StreamBot

logger = logging.getLogger("routes")


routes = web.RouteTableDef()
# Simple counter for total requests served by the HTTP server (watch + stream)
requests_served = 0

@routes.get("/", allow_head=True)
async def root_route_handler(_):
    return web.json_response(
        {
            "server_status": "running",
            "uptime": utils.get_readable_time(time.time() - StartTime),
            "telegram_bot": "@" + StreamBot.username,
            "connected_bots": len(multi_clients),
            "loads": dict(
                ("bot" + str(c + 1), l)
                for c, (_, l) in enumerate(
                    sorted(work_loads.items(), key=lambda x: x[1], reverse=True)
                )
            ),
            "version": f"v{__version__}",
        }
    )


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def watch_page_handler(request: web.Request):
    global requests_served
    requests_served += 1
    try:
        path = request.match_info["path"]
        match = re.search(r"^([0-9a-f]{%s})(\d+)$" % (Var.HASH_LENGTH), path)
        if match:
            secure_hash = match.group(1)
            message_id = int(match.group(2))
            display_name = None
        else:
            # Path can be "{message_id}/{name}" or just "{message_id}"
            id_match = re.search(r"(\d+)(?:\/\S+)?", path)
            message_id = int(id_match.group(1))
            secure_hash = request.rel_url.query.get("hash")
            # Optional filename part for nicer title
            parts = path.split("/", 1)
            display_name = parts[1] if len(parts) > 1 else None

        if not secure_hash:
            raise InvalidHash

        # Build the direct stream URL this page will load in the player
        # Preserve pretty name if present. Use the absolute base URL so the
        # player loads the same fully-qualified URL that the bot sends to users.
        name_part = f"/{display_name}" if display_name else ""
        base_url = Var.URL.rstrip('/')
        stream_src = f"{base_url}/{message_id}{name_part}?hash={secure_hash}"

        title = (display_name or f"Media {message_id}")

        html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta http-equiv=\"X-UA-Compatible\" content=\"IE=edge\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0, maximum-scale=1.0\">
    <title>Watch __FILE_NAME__ - YOUR CHANNEL NAME </title>
    <link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/plyr/3.7.8/plyr.css\"/>
    <link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css\">
<style>
    body { font-family: sans-serif; font-size: 16px; margin: 0; padding: 0; }
    #banner { position: relative; height: 55px; color: #ffffff; background-color: #2962ff; box-shadow: rgba(50, 50, 93, 0.25) 0px 2px 5px -1px, rgba(0, 0, 0, 0.3) 0px 1px 3px -1px; display: flex; justify-content: space-between; align-items: center; padding: 0 20px; }
    .left-content { flex: 1; text-align: center; }
    .black-text { color: white; }
    .right-content { margin-left: 20px; }
    .button { display: inline-block; padding: 10px 20px; background-color: #ff9b05; color: white; text-decoration: none; }
    .button:hover { box-shadow: 0 12px 16px 0 rgba(0,0,0,0.24), 0 17px 50px 0 rgba(0,0,0,0.19); }
    #player-container { margin: 1px; }
    .containerbox { overflow: hidden; *zoom: 1; margin: 0 10px; }
    .playercontainerbox { overflow: hidden; *zoom: 1; margin: 0 1px; }
    .site-content { padding: 20px; }
    #text { margin: 20px; }
    #button-container1 { display: flex; align-items: center; flex-wrap: wrap; }
    #button-container1 button { flex: 1 0 auto; height: 40px; padding: 0 20px; margin: 5px; font-size: 16px; border: none; border-radius: 4px; cursor: pointer; transition: box-shadow 0.3s, background-color 0.3s, color 0.3s; }
    #button-container1 button:hover { box-shadow: 0 0 10px rgba(0, 0, 0, 0.2); }
    #button-container1 button#vlc-btn, #button-container1 button#pi-btn, #button-container1 button#sp-btn, #button-container1 button#mx-btn { background-color: Lavender; color: #ffffff; }
    #button-container2 { display: flex; flex-direction: column; align-items: stretch; }
    #button-container2 button { height: 40px; padding: 0 20px; margin: 5px; font-size: 16px; border: none; border-radius: 4px; cursor: pointer; transition: box-shadow 0.3s, background-color 0.3s, color 0.3s; }
    #button-container2 button:hover { box-shadow: 0 0 10px rgba(0, 0, 0, 0.2); }
    .modal { display: none; position: fixed; z-index: 1; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0, 0, 0, 0.4); }
    #info-box { border: 1px solid #8A8A8A; border-radius: 10px; padding: 5px; width: auto; text-align: left; margin: 10px; box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1); box-sizing: border-box; word-wrap: break-word; }
    .modal-content { background-color: #fefefe; margin: 15% auto; padding: 20px; border: 1px solid #888; width: 300px; text-align: center; }
    .close { color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }
    #mx-btn { display: flex; align-items: center; justify-content: center; height: 40px; padding: 0 20px; margin: 5px; font-size: 16px; border: none; border-radius: 4px; cursor: pointer; transition: box-shadow 0.3s, background-color 0.3s, color 0.3s; background-color: #f0f0f0; color: #000000; }
    #mx-btn svg { width: 20px; height: 20px; margin-right: 5px; }
    #vlc-btn:hover, #mx-btn:hover { box-shadow: 0 0 10px rgba(0, 0, 0, 0.2); }
    #dl-btn:hover, #tg-btn:hover { background-color: #E8E8E8; }
</style>
</head>

<body>
<div id=\"banner\">
    <div class=\"left-content\">
        <span class=\"black-text\">Online Streaming</span>
    </div>
</div>
<div style=\"margin-bottom: 5px; margin-top: 5px\"></div>
<div class=\"site-content\">
    <div class=\"playercontainerbox\"> 
        <div id=\"player-container\">
            <video id=\"player\" playsinline controls>
                <source src=\"__FILE_URL__\" type=\"video/mp4\">
            </video>
        </div>
    </div>
    <div class=\"containerbox\"> 
        <div id=\"info-box\">
           <p style=\"font-size: 16px; color:#7b89a3;\">File Details :</p>
           <p style=\"font-size: 13px; color:#7b89a3;\">Name : __FILE_NAME__</p>
        </div>
        <div style=\"margin-bottom: 5px; margin-top: 10px\">Stream Options :</div>
        <div id=\"button-container1\">
            <button id=\"vlc-btn\" onclick=\"playOnline()\">
                <img src=\"https://i.ibb.co/GtnGhBV/videolan-vlc-logo-icon-170258.png\" alt=\"vlc_logo\" width=\"110px\" height=\"40px\"/>
            </button>
            <button id=\"mx-btn\" onclick=\"playOnlineMx()\"><img src=\"https://i.ibb.co/djV3Fn8/mxlogo.png\" alt=\"mx_logo\" width=\"120px\"> </button>
        </div>
        <div style=\"margin-bottom: 40px; margin-top: 5px\">
        <div id=\"button-container1\">
            <button id=\"sp-btn\" onclick=\"playOnlinesp()\">
                <img src=\"https://i.ibb.co/vZxWgz5/ZKTgV9HV.png\" alt=\"splayer_logo\" width=\"130px\">
            </button>
            <button id=\"pi-btn\" onclick=\"playOnlinepi()\"><img src=\"https://i.ibb.co/JsDNHgz/c0C4vnAa.png\" alt=\"playit_logo\" width=\"140px\"> </button>
        </div>
        </div>
        <div style=\"margin-bottom: 5px; margin-top: 40px\">Download Options :</div>
        <div id=\"button-container2\">
            <button id=\"dl-btn\" onclick=\"openModal()\"> <i class=\"fas fa-download\"></i> Download Now </button>
        </div>
        <div id=\"button-container2\">
            <button class=\"block\" id=\"tg-btn\" onclick=\"openTgBot()\"> <i class=\"fab fa-telegram\"></i> Create Link Like This </button>
        </div>
        <script src=\"https://cdnjs.cloudflare.com/ajax/libs/plyr/3.7.8/plyr.js\"></script>
        <script>
            const player = new Plyr('#player', { controls: ['play', 'progress', 'rewind', 'fast-forward', 'current-time', 'fullscreen', 'settings'] });
        </script>
        </div>    
        <div>
        <script>
    // Use the server-generated absolute stream URL so client-side
    // redirects always point to the exact resource (including hash).
    const streamUrl = "__FILE_URL__";
    const encodedStreamUrl = encodeURIComponent(streamUrl);
    const encodedTitle = encodeURIComponent('__FILE_NAME__');

    function playOnline() {
        // VLC (desktop/mobile) open scheme. Use encoded URL to avoid
        // issues with spaces and special characters.
        const vlcUrl = `vlc://open?url=${encodedStreamUrl}`;
        window.location.href = vlcUrl;
    }

    function playOnlineMx() {
        // MX Player Android intent format. Provide encoded URL and title.
        const mxUrl = `intent:${streamUrl}#Intent;package=com.mxtech.videoplayer.ad;S.title=${encodedTitle};end`;
        window.location.href = mxUrl;
    }
    
    function playOnlinesp() {
        // Simple Player intent (Android) using the absolute stream URL.
        const spUrl = `intent:${streamUrl}#Intent;action=com.young.simple.player.playback_online;package=com.young.simple.player;end`;
        window.location.href = spUrl;
    }
    
    function playOnlinepi() {
        // PlayIt scheme (mobile). Use encoded URL and title.
        const piUrl = `playit://playerv2/video?url=${encodedStreamUrl}&title=${encodedTitle}`;
        window.location.href = piUrl;
    }

    function openTgBot() { window.open('https://telegram.dog/', '_blank'); }

    function openModal() {
        const modal = document.getElementById('myModal');
        if (!modal) { window.location.href = streamUrl; return; }
        modal.style.display = 'block';
    }
</script>
<script>
    document.addEventListener('contextmenu', function (e) { e.preventDefault(); });
    document.addEventListener('keydown', function (e) {
        if (e.key === 'F12' || (e.ctrlKey && e.shiftKey && e.key === 'I') || (e.ctrlKey && e.key === 'u')) { e.preventDefault(); }
    });
</script>
<footer>
        <p style=\"font-size: 15px; color:#7b89a3;padding-top: 30px ;margin:0px 10px; border-top: 1px solid #c2cee3;\">
      This website only provides a service to convert Telegram files to online streaming.
      <p style=\"text-align:center\">&copy; Copyright</p>
</footer>    
</body>
</html>"""

        html = html.replace("__FILE_NAME__", title).replace("__FILE_URL__", stream_src)

        return web.Response(text=html, content_type="text/html")
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except Exception as e:
        logger.critical(str(e), exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))

@routes.get(r"/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        global requests_served
        requests_served += 1
        path = request.match_info["path"]
        match = re.search(r"^([0-9a-f]{%s})(\d+)$" % (Var.HASH_LENGTH), path)
        if match:
            secure_hash = match.group(1)
            message_id = int(match.group(2))
        else:
            message_id = int(re.search(r"(\d+)(?:\/\S+)?", path).group(1))
            secure_hash = request.rel_url.query.get("hash")
        return await media_streamer(request, message_id, secure_hash)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logger.critical(str(e), exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))

class_cache = {}

async def media_streamer(request: web.Request, message_id: int, secure_hash: str):
    range_header = request.headers.get("Range", 0)
    
    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]
    
    if Var.MULTI_CLIENT:
        logger.info(f"Client {index} is now serving {request.remote}")

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        logger.debug(f"Using cached ByteStreamer object for client {index}")
    else:
        logger.debug(f"Creating new ByteStreamer object for client {index}")
        tg_connect = utils.ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    logger.debug("before calling get_file_properties")
    file_id = await tg_connect.get_file_properties(message_id)
    logger.debug("after calling get_file_properties")
    
    
    if utils.get_hash(file_id.unique_id, Var.HASH_LENGTH) != secure_hash:
        logger.debug(f"Invalid hash for message with ID {message_id}")
        raise InvalidHash
    
    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    body = tg_connect.yield_file(
        file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )
    mime_type = file_id.mime_type
    file_name = utils.get_name(file_id)
    disposition = "attachment"

    if not mime_type:
        mime_type = mimetypes.guess_type(file_name)[0] or "application/octet-stream"

    if "video/" in mime_type or "audio/" in mime_type or "/html" in mime_type:
        disposition = "inline"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        },
    )
