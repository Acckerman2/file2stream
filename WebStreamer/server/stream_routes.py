import asyncio
import re
import time
import math
import json
import html
import logging
import secrets
import mimetypes
from urllib.parse import unquote_plus
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from WebStreamer.bot import multi_clients, work_loads
from WebStreamer.server.exceptions import FIleNotFound, InvalidHash
from WebStreamer import Var, utils, StartTime, __version__, StreamBot

logger = logging.getLogger("routes")


routes = web.RouteTableDef()
# Simple counter for total requests served by the HTTP server (watch + stream)
requests_served = 0
class_cache = {}


def _human_readable_size(size: int) -> str:
    if not size:
        return "Unknown"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    step = 1024.0
    current = float(size)
    for unit in units:
        if current < step or unit == units[-1]:
            if unit == "B":
                return f"{int(current)} {unit}"
            return f"{current:.2f} {unit}"
        current /= step


def _get_streamer_for_client(client):
    if client in class_cache:
        return class_cache[client]
    class_cache[client] = utils.ByteStreamer(client)
    return class_cache[client]


async def _resolve_file_context(message_id: int, secure_hash: str, remote_addr: str = None):
    if not secure_hash:
        raise InvalidHash

    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]

    if Var.MULTI_CLIENT and remote_addr:
        logger.info(f"Client {index} is now serving {remote_addr}")

    tg_connect = _get_streamer_for_client(faster_client)
    logger.debug(f"Resolving file context for message {message_id} using client {index}")
    file_id = await tg_connect.get_file_properties(message_id)

    if utils.get_hash(file_id.unique_id, Var.HASH_LENGTH) != secure_hash:
        logger.debug(f"Invalid hash for message with ID {message_id}")
        raise InvalidHash

    return file_id, tg_connect, index


WATCH_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">
    <title>Watch __FILE_NAME_HTML__ - Stream</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/plyr/3.7.8/plyr.css"/>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css">
<style>
    :root {
        --bg: #06080f;
        --panel: rgba(255, 255, 255, 0.04);
        --panel-strong: rgba(255, 255, 255, 0.08);
        --text: #f7f9ff;
        --muted: #a8b4ce;
        --accent: #5fd1b5;
        --accent-2: #7ac5ff;
        --shadow: 0 20px 70px rgba(0,0,0,0.4);
        --radius: 18px;
        --blur: 18px;
        --border: 1px solid rgba(255,255,255,0.06);
    }

    * { box-sizing: border-box; }
    body {
        margin: 0;
        font-family: 'Manrope', sans-serif;
        background: radial-gradient(circle at 12% 18%, rgba(95,209,181,0.06), transparent 30%),
                    radial-gradient(circle at 82% 8%, rgba(122,197,255,0.08), transparent 34%),
                    linear-gradient(145deg, #0a1020, #06080f 55%, #070913);
        color: var(--text);
        min-height: 100vh;
        position: relative;
        overflow-x: hidden;
        opacity: 0;
        transform: translateY(26px) scale(0.98);
        animation: fadeIn 1.05s cubic-bezier(0.22, 0.61, 0.36, 1) forwards;
    }

    .grid-accent {
        position: fixed;
        inset: 0;
        background: radial-gradient(circle at 30% 70%, rgba(95,209,181,0.06), transparent 35%),
                    radial-gradient(circle at 72% 78%, rgba(122,197,255,0.05), transparent 32%);
        pointer-events: none;
        z-index: 0;
        animation: aurora 14s ease-in-out infinite alternate;
    }

    .page {
        position: relative;
        z-index: 1;
        max-width: 820px;
        margin: 0 auto;
        padding: 0 18px 32px;
    }

    header {
        padding: 14px 20px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        backdrop-filter: blur(var(--blur));
        background: rgba(6, 8, 15, 0.85);
        border-bottom: var(--border);
        position: sticky;
        top: 0;
        z-index: 5;
    }

    .brand {
        font-weight: 700;
        letter-spacing: 0.6px;
        display: flex;
        align-items: center;
        gap: 10px;
        font-size: 15px;
    }
    .brand .dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background: linear-gradient(135deg, var(--accent), var(--accent-2));
        box-shadow: 0 0 14px rgba(95,209,181,0.9);
    }

    .headline {
        margin: 22px 0 16px;
        display: grid;
        grid-template-columns: 1fr;
        gap: 18px;
    }

    .hero {
        background: var(--panel);
        border: var(--border);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        backdrop-filter: blur(var(--blur));
        padding: 18px 16px;
        position: relative;
        overflow: hidden;
    }

    .hero::before {
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(circle at 20% 20%, rgba(95,209,181,0.12), transparent 40%),
                    radial-gradient(circle at 82% 0%, rgba(122,197,255,0.12), transparent 42%);
        pointer-events: none;
        opacity: 0.8;
    }
    .hero::after {
        content: "";
        position: absolute;
        inset: 1px;
        border-radius: calc(var(--radius) - 2px);
        border: 1px solid rgba(255,255,255,0.03);
        pointer-events: none;
    }

    .hero-content { position: relative; z-index: 1; }
    .hero-header { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 8px; }
    .eyebrow { color: var(--text); letter-spacing: 0.8px; text-transform: uppercase; font-size: 11px; margin: 0; padding: 6px 10px; border-radius: 999px; background: rgba(95,209,181,0.16); border: 1px solid rgba(95,209,181,0.25); box-shadow: 0 6px 16px rgba(95,209,181,0.18); }
    .hero-badge { padding: 6px 10px; border-radius: 999px; background: rgba(122,197,255,0.16); color: var(--text); border: 1px solid rgba(122,197,255,0.3); font-size: 11px; letter-spacing: 0.4px; }
    .hero h1 { margin: 0 0 6px; font-size: 22px; letter-spacing: 0.5px; }
    .hero .lead { margin: 0 0 10px; color: var(--muted); line-height: 1.5; font-size: 14px; }

    .chip-row { display: flex; gap: 8px; flex-wrap: wrap; margin: 12px 0 6px; }
    .chip {
        padding: 9px 12px;
        border-radius: 12px;
        background: linear-gradient(135deg, rgba(95,209,181,0.14), rgba(122,197,255,0.12));
        color: var(--text);
        font-size: 13px;
        border: 1px solid rgba(255,255,255,0.1);
        box-shadow: 0 6px 18px rgba(0,0,0,0.25);
    }

    .cta-row { display: flex; gap: 14px; flex-wrap: wrap; margin-top: 12px; justify-content: center; }
    .ghost {
        border: var(--border);
        background: transparent;
        color: var(--text);
        border-radius: 10px;
        padding: 12px 18px;
        cursor: pointer;
        font-weight: 700;
        font-size: 14px;
        transition: border-color 0.18s ease, color 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
        position: relative;
        overflow: hidden;
        opacity: 0;
        transform: translateY(10px);
        animation: btnRise 0.55s ease forwards;
    }
    .ghost.copy-btn {
        border-color: rgba(95,209,181,0.45);
        background: linear-gradient(135deg, rgba(95,209,181,0.2), rgba(122,197,255,0.12));
        border-radius: 999px;
        min-width: 220px;
        box-shadow: 0 12px 30px rgba(95,209,181,0.18);
    }
    .ghost.download-btn {
        border-color: rgba(122,197,255,0.5);
        background: linear-gradient(135deg, rgba(122,197,255,0.26), rgba(95,209,181,0.12));
        border-radius: 18px;
        min-width: 220px;
        box-shadow: 0 12px 30px rgba(122,197,255,0.2);
    }
    .ghost:hover { border-color: rgba(95,209,181,0.65); color: var(--accent); transform: translateY(-1px); box-shadow: 0 10px 26px rgba(95,209,181,0.18); }
    .ghost.copy-btn:hover { box-shadow: 0 16px 36px rgba(95,209,181,0.25); }
    .ghost.download-btn:hover { box-shadow: 0 16px 36px rgba(122,197,255,0.28); border-color: rgba(122,197,255,0.65); }
    .ghost:active { transform: translateY(0); }
    .ghost::after {
        content: "";
        position: absolute;
        inset: -2px;
        background: linear-gradient(120deg, transparent 0%, rgba(255,255,255,0.14) 40%, transparent 70%);
        transform: translateX(-120%);
        transition: transform 0.5s ease;
    }
    .ghost:hover::after { transform: translateX(120%); }
    .cta-row .ghost:first-child { animation-delay: 0.2s; }
    .cta-row .ghost:last-child { animation-delay: 0.28s; }

    main {
        margin-top: 12px;
        display: grid;
        grid-template-columns: 2.2fr 1fr;
        gap: 18px;
    }

    @media (max-width: 1100px) {
        .headline { grid-template-columns: 1fr; }
        main { grid-template-columns: 1fr; }
    }

    .card {
        background: var(--panel);
        border: var(--border);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        backdrop-filter: blur(var(--blur));
        padding: 14px;
        opacity: 0;
        transform: translateY(12px);
        animation: rise 0.7s ease forwards;
        animation-delay: 0.15s;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .card:hover { transform: translateY(-2px); box-shadow: 0 24px 70px rgba(0,0,0,0.35); }

    .top-player { margin-top: 18px; }

    .hero { animation-delay: 0.25s; }
    .headline .hero:last-child { animation-delay: 0.32s; }
    main .card { animation-delay: 0.36s; }

    .pill,
    .chip,
    .pill-small {
        animation: btnRise 0.6s ease forwards;
        opacity: 0;
        transform: translateY(10px);
    }
    .chip-row .chip:nth-child(1) { animation-delay: 0.18s; }
    .chip-row .chip:nth-child(2) { animation-delay: 0.22s; }
    .chip-row .chip:nth-child(3) { animation-delay: 0.26s; }
    .chip-row .chip:nth-child(4) { animation-delay: 0.3s; }
    .pill-row .pill:nth-child(1) { animation-delay: 0.18s; }
    .pill-row .pill:nth-child(2) { animation-delay: 0.22s; }
    .pill-row .pill:nth-child(3) { animation-delay: 0.26s; }
    .pill-small { animation-delay: 0.16s; }

    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(26px) scale(0.98); }
        to { opacity: 1; transform: translateY(0) scale(1); }
    }
    @keyframes rise {
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes btnRise {
        from { opacity: 0; transform: translateY(12px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes aurora {
        0% { transform: translate3d(0,0,0) scale(1); filter: blur(0px) saturate(1); }
        100% { transform: translate3d(6px, -10px, 0) scale(1.04); filter: blur(1px) saturate(1.1); }
    }
    @keyframes floatUpDown {
        0% { transform: translateY(0); }
        50% { transform: translateY(-6px); }
        100% { transform: translateY(0); }
    }
    @keyframes mobilePop {
        0% { opacity: 0; transform: translateY(14px) scale(0.98); }
        60% { opacity: 1; transform: translateY(-2px) scale(1.01); }
        100% { opacity: 1; transform: translateY(0) scale(1); }
    }
    @keyframes wiggle {
        0%, 100% { transform: translateY(0) rotate(0deg); }
        15% { transform: translateY(-1px) rotate(-1deg); }
        30% { transform: translateY(1px) rotate(1.2deg); }
        45% { transform: translateY(-1px) rotate(-1deg); }
        60% { transform: translateY(1px) rotate(1deg); }
        75% { transform: translateY(-0.5px) rotate(-0.6deg); }
    }

    @keyframes pulseGlow {
        0% { box-shadow: 0 16px 40px rgba(78,211,167,0.32); transform: translateY(0); }
        40% { box-shadow: 0 20px 48px rgba(122,197,255,0.35); transform: translateY(-1px); }
        70% { box-shadow: 0 18px 44px rgba(78,211,167,0.3); transform: translateY(-0.5px); }
        100% { box-shadow: 0 16px 40px rgba(78,211,167,0.32); transform: translateY(0); }
    }

    .card-title {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 12px;
        font-weight: 700;
        letter-spacing: 0.3px;
    }

    .pill-small { padding: 6px 10px; border-radius: 10px; background: rgba(95,209,181,0.12); color: var(--accent); border: 1px solid rgba(95,209,181,0.35); font-size: 12px; }

    .player-shell {
        position: relative;
        overflow: hidden;
        border-radius: calc(var(--radius) - 6px);
        background: #05070f;
        border: 1px solid rgba(255,255,255,0.07);
        animation: floatUpDown 6s ease-in-out infinite;
    }

    #player {
        width: 100%;
        height: clamp(200px, 35vw, 340px);
        background: #000;
    }

    .meta { margin-top: 12px; }
    .meta h1 {
        margin: 0 0 4px;
        font-size: 17px;
        letter-spacing: 0.2px;
    }
    .meta p { margin: 4px 0; color: var(--muted); }

    .pill-row { display: flex; gap: 8px; flex-wrap: wrap; margin: 10px 0 6px; }
    .pill { padding: 8px 12px; border-radius: 12px; background: var(--panel-strong); color: var(--text); font-size: 13px; border: var(--border); }

    .section-title { margin: 18px 0 10px; font-weight: 700; letter-spacing: 0.3px; }

    .grid { display: grid; gap: 10px; }
    .grid.two { grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); }
    .grid.full { grid-template-columns: 1fr; }

    .action {
        width: 100%;
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 12px;
        padding: 12px 14px;
        background: linear-gradient(145deg, rgba(15,25,42,0.9), rgba(24,32,52,0.9));
        color: var(--text);
        font-weight: 700;
        letter-spacing: 0.2px;
        cursor: pointer;
        transition: transform 0.18s ease, box-shadow 0.18s ease, border-color 0.18s ease, background-position 0.4s ease;
        position: relative;
        overflow: hidden;
        opacity: 0;
        transform: translateY(10px);
        animation: btnRise 0.6s ease forwards;
        box-shadow: 0 10px 26px rgba(0,0,0,0.3);
        display: inline-block;
        text-decoration: none;
        text-align: center;
    }
    .action.pc-vlc {
        background: linear-gradient(145deg, rgba(30,48,79,0.95), rgba(18,30,54,0.95));
        border-color: rgba(122,197,255,0.5);
        box-shadow: 0 14px 36px rgba(122,197,255,0.26);
        animation: btnRise 0.6s ease forwards, pulseGlow 3s ease-in-out infinite;
        position: relative;
    }
    .action.pc-vlc::before {
        content: "";
        position: absolute;
        inset: -1px;
        background: linear-gradient(120deg, rgba(122,197,255,0), rgba(122,197,255,0.3), rgba(122,197,255,0));
        transform: translateX(-140%);
        opacity: 0.8;
        transition: transform 0.65s ease;
        pointer-events: none;
    }
    .action.pc-vlc:hover::before { transform: translateX(140%); }
    .action:hover {
        transform: translateY(-2px);
        box-shadow: 0 14px 32px rgba(0,0,0,0.34);
        border-color: rgba(95,209,181,0.35);
        background: linear-gradient(145deg, rgba(30,48,79,0.95), rgba(18,30,54,0.95));
    }
    .action:active { transform: translateY(0); }

    .grid.two .action:nth-child(1) { border-color: rgba(95,209,181,0.35); }
    .grid.two .action:nth-child(2) { border-color: rgba(122,197,255,0.35); }
    .grid.two .action:nth-child(3) { border-color: rgba(176,123,255,0.35); }
    .grid.two .action:nth-child(4) { border-color: rgba(255,189,105,0.35); }
    .grid.two .action:nth-child(1):hover { box-shadow: 0 14px 34px rgba(95,209,181,0.25); }
    .grid.two .action:nth-child(2):hover { box-shadow: 0 14px 34px rgba(122,197,255,0.25); }
    .grid.two .action:nth-child(3):hover { box-shadow: 0 14px 34px rgba(176,123,255,0.25); }
    .grid.two .action:nth-child(4):hover { box-shadow: 0 14px 34px rgba(255,189,105,0.25); }

    .grid.full .action.accent {
        background: linear-gradient(135deg, #4ed3a7, #7ac5ff);
        color: #041019;
        box-shadow: 0 16px 40px rgba(78,211,167,0.32);
        border-color: rgba(78,211,167,0.65);
        animation: btnRise 0.6s ease forwards;
    }
    .grid.full .action.accent:hover {
        transform: translateY(-2px);
        box-shadow: 0 20px 50px rgba(78,211,167,0.4);
    }
    .grid.two .action:nth-child(1) { animation-delay: 0.24s; }
    .grid.two .action:nth-child(2) { animation-delay: 0.28s; }
    .grid.two .action:nth-child(3) { animation-delay: 0.32s; }
    .grid.two .action:nth-child(4) { animation-delay: 0.36s; }
    .grid.full .action:nth-child(1) { animation-delay: 0.32s; }
    .grid.full .action:nth-child(2) { animation-delay: 0.36s; }

    .accent { background: linear-gradient(135deg, #4ed3a7, #7ac5ff); }
    .accent-2 { background: linear-gradient(135deg, #7ac5ff, #4ed3a7); }

    .tg-link-btn {
        background: linear-gradient(135deg, #229ED9, #2bb4ff);
        border-color: rgba(34,158,217,0.6);
        color: #041019;
        box-shadow: 0 14px 36px rgba(34,158,217,0.26);
    }
    .tg-link-btn:hover {
        transform: translateY(-2px);
        box-shadow: 0 18px 44px rgba(34,158,217,0.32);
        border-color: rgba(43,180,255,0.75);
    }

    .modal {
        display: none;
        position: fixed;
        inset: 0;
        background: rgba(0,0,0,0.6);
        backdrop-filter: blur(10px);
        z-index: 10;
        align-items: center;
        justify-content: center;
    }
    .modal-content {
        background: #0f1424;
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 16px;
        padding: 20px 22px;
        width: min(360px, 90vw);
        color: var(--text);
        box-shadow: var(--shadow);
        text-align: center;
    }
    .modal-content h3 { margin: 0 0 10px; }
    .modal-content p { margin: 6px 0; color: var(--muted); }

    footer {
        padding: 30px 28px 24px;
        color: var(--muted);
        text-align: center;
        border-top: 1px solid rgba(255,255,255,0.05);
        margin-top: 30px;
        background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0));
    }
    footer a { color: var(--accent); }

    @media (max-width: 720px) {
        .page { padding: 0 16px 32px; }
        header { padding: 14px 16px; }
        .headline { margin: 16px 0 10px; }
        .hero h1 { font-size: 22px; }
        .hero .lead { font-size: 14px; line-height: 1.5; }
        .chip-row { gap: 8px; }
        .chip-row .chip { flex: 1 1 48%; text-align: center; }
        .cta-row { flex-direction: column; align-items: stretch; }
        .ghost { width: 100%; text-align: center; animation: mobilePop 0.55s ease forwards; }
        .action { border-radius: 14px; animation-duration: 0.55s; }
        .card { padding: 16px; }
        #player { height: clamp(220px, 60vw, 420px); }
        .grid.two { grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); }
        .stat .value { font-size: 15px; }
        .top-player { margin-top: 12px; }

        .grid-accent { animation: none; }
        .player-shell { animation: none; }
        .grid.full .action.accent {
            animation: btnRise 0.5s ease forwards;
            box-shadow: 0 12px 28px rgba(78,211,167,0.26);
        }
        .action { box-shadow: 0 8px 20px rgba(0,0,0,0.25); }
        .action:hover { transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0,0,0,0.28); }
        .ghost::after, .action::after { display: none; }
    }
</style>
</head>

<body>
    <div class="grid-accent"></div>
    <header>
        <div class="brand"><span class="dot"></span>Stream Panel</div>
        <div style="color: var(--muted); font-size: 14px;">Secure • Fast • Ad-lite</div>
    </header>

    <div class="page">
        <section class="card top-player">
            <div class="card-title">
                <span>Now streaming</span>
                <span class="pill-small">Adaptive</span>
            </div>
            <div class="player-shell">
                <video id="player" playsinline controls>
                    <source src="__FILE_URL_HTML__" type="video/mp4">
                </video>
            </div>
            <div class="meta">
                <h1>__FILE_NAME_HTML__</h1>
                <p>Size: __FILE_SIZE__</p>
                <div class="pill-row">
                    <span class="pill">Stream ready</span>
                    <span class="pill">Direct download</span>
                    <span class="pill">Telegram bridge</span>
                </div>
            </div>
        </section>

        <section class="headline">
            <div class="hero">
                <div class="hero-content">
                    <div class="hero-header">
                        <p class="eyebrow">Stream ready</p>
                        <span class="hero-badge"><i class="fas fa-bolt"></i> Live</span>
                    </div>
                    <h1>__FILE_NAME_HTML__</h1>
                    <p class="lead">Stream instantly, share securely, and jump to your preferred player without ads or popups.</p>
                    <div class="chip-row">
                        <span class="chip">Size: __FILE_SIZE__</span>
                        <span class="chip">No signup</span>
                        <span class="chip">TLS protected</span>
                        <span class="chip">Optimized for mobile</span>
                    </div>
                    <div class="cta-row">
                        <button class="ghost copy-btn" onclick="copyLink()"><i class="fas fa-link"></i> Copy secure link</button>
                        <button class="ghost download-btn" onclick="openModal()"><i class="fas fa-arrow-circle-down"></i> Start download</button>
                    </div>
                </div>
            </div>
        </section>

        <section class="card">
            <div class="section-title">Stream options</div>
            <div class="grid two">
                <button class="action" onclick="playOnline()"><i class="fas fa-play-circle"></i> VLC</button>
                <button class="action" onclick="playOnlineMx()">MX Player</button>
                <button class="action" onclick="playOnlinesp()">SPlayer</button>
                <button class="action" onclick="playOnlinepi()">PlayIt</button>
            </div>

            <div class="section-title" style="margin-top:16px;">PC stream</div>
            <div class="grid full">
                <a id="pc-vlc-link" class="action pc-vlc" href="#"><i class="fas fa-desktop"></i> VLC (PC)</a>
            </div>

            <div class="section-title" style="margin-top:16px;">Download</div>
            <div class="grid full">
                <button class="action accent" onclick="openModal()"><i class="fas fa-download"></i> Download now</button>
                <button class="action tg-link-btn" onclick="openTgBot()"><i class="fab fa-telegram"></i> Create a page like this</button>
            </div>
        </section>
    </div>

    <div class="modal" id="myModal">
        <div class="modal-content">
            <h3>Working on it...</h3>
            <p id="timer">Please wait</p>
        </div>
    </div>

    <footer>
        This service streams your Telegram files securely. Report issues <a href="https://telegram.dog" target="_blank" rel="noopener">here</a>.
        <div style="margin-top:6px;">© 『ACCKERMAN°ツ</div>
    </footer>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/plyr/3.7.8/plyr.js"></script>
    <script>
        const player = new Plyr('#player', {
            controls: ['play', 'progress', 'rewind', 'fast-forward', 'current-time', 'fullscreen', 'settings']
        });
    </script>
    <script>
        const fileName = __FILE_NAME_JS__;
        const currentUrl = window.location.href;
        const finalUrl = currentUrl.replace("/watch/", "/");
        const streamRoot = finalUrl.endsWith('/') ? finalUrl.slice(0, -1) : finalUrl;

        const pcVlcLinkEl = document.getElementById("pc-vlc-link");
        if (pcVlcLinkEl) {
            pcVlcLinkEl.setAttribute("href", `vlc://${finalUrl}`);
        }

        async function copyLink() {
            try {
                await navigator.clipboard.writeText(finalUrl);
                alert("Secure link copied.");
            } catch (err) {
                console.error("Copy failed", err);
                alert("Unable to copy. Please copy manually: " + finalUrl);
            }
        }

        function playOnline() {
            const vlcUrl = `vlc://${finalUrl}`;
            window.location.href = vlcUrl;
        }

        function playOnlineMx() {
            const mxUrl = `intent:${finalUrl}#Intent;package=com.mxtech.videoplayer.ad;S.title=${fileName};end`;
            window.location.href = mxUrl;
        }
        
        function playOnlinesp() {
            const spUrl = `intent:${finalUrl}#Intent;action=com.young.simple.player.playback_online;package=com.young.simple.player;end`;
            window.location.href = spUrl;
        }
        
        function playOnlinepi() {
            const piUrl = `playit://playerv2/video?url=${finalUrl}&title=${fileName}`;
            window.location.href = piUrl;
        }

        function openTgBot() {
            const createLinkUrl = "https://telegram.dog/sydney_sweeney_robot";
            window.open(createLinkUrl, "_blank");
        }

        function openModal() {
            const modal = document.getElementById("myModal");
            modal.style.display = "flex";

            const words = [
                "Received download request... ✅",
                "Processing your request... ⌛",
                "Generating your link... 🔗",
                "Sending link to you... 🚀",
                "Download has started... 😉"
            ];

            let index = 0;
            const timerElement = document.getElementById("timer");
            timerElement.innerText = words[index];

            const timer = setInterval(() => {
                index++;
                if (index < words.length) {
                    timerElement.innerText = words[index];
                } else {
                    clearInterval(timer);
                    download();
                    modal.style.display = "none";
                }
            }, 1000);
        }

        function download() {
            const downloadUrl = finalUrl + (finalUrl.includes('?') ? '&' : '?') + 'dl=1';
            window.location.href = downloadUrl;
        }

        document.addEventListener('contextmenu', function (e) {
            e.preventDefault();
        });

        document.addEventListener('keydown', function (e) {
            if (
                e.key === 'F12' ||
                (e.ctrlKey && e.shiftKey && e.key === 'I') ||
                (e.ctrlKey && e.key === 'u') ||
                e.ctrlKey ||
                e.shiftKey ||
                e.altKey
            ) {
                e.preventDefault();
            }
        });
    </script>
</body>
</html>"""


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
            raw_name = None
        else:
            # Path can be "{message_id}/{name}" or just "{message_id}"
            id_match = re.search(r"(\d+)(?:\/\S+)?", path)
            message_id = int(id_match.group(1))
            secure_hash = request.rel_url.query.get("hash")
            # Optional filename part for nicer title
            parts = path.split("/", 1)
            raw_name = parts[1] if len(parts) > 1 else None

        if not secure_hash:
            raise InvalidHash

        name_part = f"/{raw_name}" if raw_name else ""
        base_url = Var.URL.rstrip('/')
        stream_src = f"{base_url}/{message_id}{name_part}?hash={secure_hash}"

        file_id, _, _ = await _resolve_file_context(message_id, secure_hash, request.remote)
        file_size_text = _human_readable_size(getattr(file_id, "file_size", 0))

        display_name = unquote_plus(raw_name) if raw_name else None
        file_name_for_display = display_name or utils.get_name(file_id)

        page_html = WATCH_PAGE_TEMPLATE
        page_html = page_html.replace("__FILE_NAME_HTML__", html.escape(file_name_for_display))
        page_html = page_html.replace("__FILE_NAME_JS__", json.dumps(file_name_for_display))
        page_html = page_html.replace("__FILE_URL_HTML__", html.escape(stream_src, quote=True))
        page_html = page_html.replace("__FILE_URL_JS__", json.dumps(stream_src))
        page_html = page_html.replace("__FILE_SIZE__", file_size_text)

        return web.Response(text=page_html, content_type="text/html")
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
        return web.Response(status=499, text="Client closed request")
    except Exception as e:
        logger.critical(str(e), exc_info=True)
        raise web.HTTPInternalServerError(text=str(e))


async def media_streamer(request: web.Request, message_id: int, secure_hash: str):
    range_header = request.headers.get("Range", 0)
    force_download = request.rel_url.query.get("dl") == "1"  # Check for download parameter

    file_id, tg_connect, index = await _resolve_file_context(message_id, secure_hash, request.remote)
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

    chunk_size = 1024 * 1024  # Fixed 1MB chunk size - Telegram's optimal limit
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

    # Only set inline if not forced download and is media file
    if not force_download and ("video/" in mime_type or "audio/" in mime_type or "/html" in mime_type):
        disposition = "inline"

    headers = {
        "Content-Type": f"{mime_type}",
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{file_name}"',
        "Accept-Ranges": "bytes",
    }

    response = web.StreamResponse(status=206 if range_header else 200, headers=headers)
    await response.prepare(request)

    try:
        async for chunk in body:
            await response.write(chunk)
    except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError, BadStatusLine):
        logger.debug("Client disconnected while streaming %s", file_name)
    except OSError as e:
        # Handle socket errors (e.g., "socket.send() raised exception")
        if e.errno in (10053, 10054, 32, 104):  # Connection aborted/reset codes
            logger.debug("Client disconnected (OSError) while streaming %s", file_name)
        else:
            logger.exception("OS error while streaming %s", file_name)
    except Exception:
        logger.exception("Unexpected error while streaming %s", file_name)
    finally:
        try:
            await response.write_eof()
        except (ConnectionResetError, BrokenPipeError, RuntimeError, asyncio.CancelledError, OSError):
            # Client already closed the connection; nothing else to do.
            pass

    return response
