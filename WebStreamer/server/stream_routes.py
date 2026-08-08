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
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0">
    <title>__FILE_NAME_HTML__ — AckerStreamX</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=Playfair+Display:wght@700;800;900&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/plyr/3.7.8/plyr.css"/>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
    <style>
    /* ═══════════════════════════════════════════════
       CINEMATIC DESIGN SYSTEM — AckerStreamX
       Deep blacks, warm gold, dramatic spotlights
       ═══════════════════════════════════════════════ */
    :root {
        --bg-void: #020104;
        --bg-deep: #06050a;
        --bg-base: #0c0a12;
        --bg-elevated: #12101c;
        --bg-card: rgba(18, 16, 28, 0.65);
        --bg-card-hover: rgba(25, 22, 40, 0.75);
        --glass-border: rgba(212, 168, 83, 0.08);
        --glass-border-hover: rgba(212, 168, 83, 0.18);
        --glass-border-subtle: rgba(255,255,255, 0.04);

        --text-primary: #f5f0e8;
        --text-secondary: #8a7f6e;
        --text-tertiary: #514a3e;
        --text-gold: #d4a853;

        /* Cinema palette */
        --gold: #d4a853;
        --gold-light: #e8c97a;
        --gold-deep: #b8922f;
        --gold-glow: rgba(212, 168, 83, 0.35);
        --gold-subtle: rgba(212, 168, 83, 0.06);

        --warm-white: #fff8ef;
        --cream: #f0e6d3;

        --crimson: #c62828;
        --crimson-light: #ef5350;
        --crimson-glow: rgba(198, 40, 40, 0.3);

        --burgundy: #880e4f;
        --wine: #4a1942;

        --ember: #ff6b35;
        --ember-glow: rgba(255, 107, 53, 0.25);

        --teal-cinema: #00897b;
        --teal-glow: rgba(0, 137, 123, 0.3);

        --steel: #78909c;

        --gradient-gold: linear-gradient(135deg, #b8922f, #d4a853, #e8c97a);
        --gradient-warm: linear-gradient(135deg, #d4a853, #ff6b35, #c62828);
        --gradient-curtain: linear-gradient(180deg, rgba(136,14,79,0.15), rgba(198,40,40,0.08), transparent);
        --gradient-download: linear-gradient(135deg, #d4a853, #e8c97a);
        --gradient-spotlight: radial-gradient(ellipse at 50% 0%, rgba(212,168,83,0.12) 0%, transparent 60%);

        --radius-xs: 6px;
        --radius-sm: 10px;
        --radius-md: 14px;
        --radius-lg: 18px;
        --radius-xl: 22px;
        --radius-2xl: 28px;

        --ease: cubic-bezier(0.4, 0, 0.2, 1);
        --ease-dramatic: cubic-bezier(0.16, 1, 0.3, 1);
        --ease-spring: cubic-bezier(0.34, 1.56, 0.64, 1);
        --dur: 0.35s;
        --dur-slow: 0.7s;
    }

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    html { scroll-behavior: smooth; -webkit-text-size-adjust: 100%; }

    body {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        background: var(--bg-void);
        color: var(--text-primary);
        min-height: 100vh;
        overflow-x: hidden;
        -webkit-font-smoothing: antialiased;
        line-height: 1.5;
    }

    /* ── Film grain overlay ── */
    body::before {
        content: "";
        position: fixed;
        inset: 0;
        z-index: 0;
        opacity: 0.035;
        background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 512 512' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='g'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='6' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23g)'/%3E%3C/svg%3E");
        background-repeat: repeat;
        pointer-events: none;
        animation: grainShift 0.5s steps(4) infinite;
    }

    /* ── Vignette ── */
    body::after {
        content: "";
        position: fixed;
        inset: 0;
        z-index: 0;
        background: radial-gradient(ellipse at 50% 50%, transparent 50%, rgba(2,1,4,0.6) 100%);
        pointer-events: none;
    }

    /* ── Cinematic spotlights ── */
    .spotlights {
        position: fixed;
        inset: 0;
        pointer-events: none;
        z-index: 0;
        overflow: hidden;
    }
    .spot {
        position: absolute;
        border-radius: 50%;
        filter: blur(100px);
        will-change: transform, opacity;
    }
    /* Golden top spotlight */
    .spot-1 {
        width: 800px; height: 500px;
        background: radial-gradient(ellipse, rgba(212,168,83,0.1) 0%, transparent 70%);
        top: -20%; left: 50%;
        transform: translateX(-50%);
        animation: spotPulse 8s ease-in-out infinite;
    }
    /* Crimson side glow */
    .spot-2 {
        width: 500px; height: 600px;
        background: radial-gradient(ellipse, rgba(198,40,40,0.06) 0%, transparent 70%);
        bottom: -10%; left: -8%;
        animation: spotDrift 20s ease-in-out infinite alternate;
    }
    /* Deep wine accent */
    .spot-3 {
        width: 400px; height: 400px;
        background: radial-gradient(ellipse, rgba(136,14,79,0.05) 0%, transparent 70%);
        top: 40%; right: -5%;
        animation: spotDrift 25s ease-in-out infinite alternate-reverse;
    }
    /* Subtle warm glow center */
    .spot-4 {
        width: 600px; height: 300px;
        background: radial-gradient(ellipse, rgba(255,107,53,0.035) 0%, transparent 70%);
        top: 60%; left: 30%;
        animation: spotPulse 12s ease-in-out infinite;
        animation-delay: -4s;
    }

    /* ── Curtain top decoration ── */
    .curtain-top {
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        height: 200px;
        background: var(--gradient-curtain);
        pointer-events: none;
        z-index: 0;
    }

    /* ── Navbar ── */
    .nav {
        position: sticky;
        top: 0;
        z-index: 100;
        backdrop-filter: blur(30px) saturate(1.6);
        -webkit-backdrop-filter: blur(30px) saturate(1.6);
        background: rgba(6,5,10,0.75);
        border-bottom: 1px solid var(--glass-border);
    }
    .nav-inner {
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 32px;
        height: 70px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .brand {
        display: flex;
        align-items: center;
        gap: 14px;
        text-decoration: none;
        color: var(--text-primary);
    }
    .brand-mark {
        width: 40px;
        height: 40px;
        border-radius: var(--radius-sm);
        background: var(--gradient-gold);
        display: grid;
        place-items: center;
        position: relative;
        overflow: hidden;
        box-shadow: 0 2px 16px var(--gold-glow);
    }
    .brand-mark i { font-size: 14px; color: var(--bg-void); position: relative; z-index: 1; }
    .brand-mark::after {
        content: "";
        position: absolute;
        inset: 0;
        background: linear-gradient(180deg, rgba(255,255,255,0.25) 0%, transparent 50%);
    }
    .brand-name {
        font-family: 'Playfair Display', serif;
        font-weight: 800;
        font-size: 22px;
        letter-spacing: -0.5px;
        background: var(--gradient-gold);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .nav-right {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .badge {
        padding: 6px 14px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.8px;
        text-transform: uppercase;
        border: 1px solid var(--glass-border);
        background: var(--gold-subtle);
        color: var(--text-secondary);
        display: flex;
        align-items: center;
        gap: 7px;
    }
    .badge i { font-size: 10px; }
    .badge-live {
        background: rgba(212,168,83,0.1);
        border-color: rgba(212,168,83,0.2);
        color: var(--gold);
    }
    .badge-live .pulse-dot {
        width: 6px;
        height: 6px;
        border-radius: 50%;
        background: var(--gold);
        position: relative;
    }
    .badge-live .pulse-dot::after {
        content: "";
        position: absolute;
        inset: -3px;
        border-radius: 50%;
        background: var(--gold);
        opacity: 0;
        animation: pingPulse 2s cubic-bezier(0, 0, 0.2, 1) infinite;
    }
    .badge-premium {
        background: linear-gradient(135deg, rgba(212,168,83,0.12), rgba(232,201,122,0.08));
        border-color: rgba(212,168,83,0.25);
        color: var(--gold-light);
    }

    /* ── Scrolling Ticker ── */
    .ticker-wrap {
        position: relative;
        z-index: 1;
        overflow: hidden;
        background: rgba(212,168,83,0.03);
        border-bottom: 1px solid var(--glass-border);
        border-top: 1px solid var(--glass-border);
    }
    .ticker {
        display: flex;
        gap: 50px;
        white-space: nowrap;
        animation: tickerScroll 30s linear infinite;
        padding: 10px 0;
    }
    .ticker-item {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 1.5px;
        text-transform: uppercase;
        color: var(--text-tertiary);
        flex-shrink: 0;
    }
    .ticker-item i { color: var(--gold); font-size: 8px; }
    .ticker-item.t-gold { color: var(--gold); }
    @keyframes tickerScroll {
        0% { transform: translateX(0); }
        100% { transform: translateX(-50%); }
    }

    /* ── Film strip divider ── */
    .film-strip {
        position: relative;
        z-index: 1;
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 32px;
    }
    .film-strip-inner {
        height: 3px;
        background: linear-gradient(90deg,
            transparent 0%,
            var(--glass-border) 10%,
            var(--gold-subtle) 30%,
            rgba(212,168,83,0.12) 50%,
            var(--gold-subtle) 70%,
            var(--glass-border) 90%,
            transparent 100%
        );
        border-radius: 2px;
        position: relative;
    }
    .film-strip-inner::before {
        content: "";
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        width: 60px;
        height: 3px;
        background: var(--gradient-gold);
        border-radius: 2px;
        box-shadow: 0 0 20px var(--gold-glow);
        animation: glowPulse 3s ease-in-out infinite;
    }
    @keyframes glowPulse {
        0%, 100% { box-shadow: 0 0 20px var(--gold-glow); width: 60px; }
        50% { box-shadow: 0 0 40px var(--gold-glow), 0 0 60px rgba(212,168,83,0.15); width: 90px; }
    }

    /* ── Main layout ── */
    .main {
        position: relative;
        z-index: 1;
        max-width: 1200px;
        margin: 0 auto;
        padding: 28px 32px 0;
    }
    .grid-layout {
        display: flex;
        flex-direction: column;
        gap: 24px;
    }

    /* ── Player ── */
    .player-section {
        border-radius: var(--radius-2xl);
        overflow: hidden;
        background: var(--bg-card);
        border: 1px solid var(--glass-border);
        backdrop-filter: blur(20px);
        box-shadow:
            0 0 0 1px rgba(212,168,83,0.04),
            0 16px 80px rgba(0,0,0,0.6),
            0 0 120px rgba(212,168,83,0.04);
        animation: cinemaReveal 1s var(--ease-dramatic) forwards;
        opacity: 0;
        position: relative;
    }

    /* Animated gradient border glow */
    .player-section::before {
        content: "";
        position: absolute;
        top: -1px;
        left: 10%;
        right: 10%;
        height: 2px;
        background: linear-gradient(90deg, transparent, var(--gold), var(--gold-light), var(--gold), transparent);
        background-size: 200% 100%;
        animation: borderGlow 4s linear infinite;
        z-index: 2;
        filter: blur(0.5px);
    }
    @keyframes borderGlow {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }

    .video-frame {
        position: relative;
        background: #000;
        aspect-ratio: 16/9;
    }
    .video-frame video { width: 100%; height: 100%; display: block; object-fit: contain; }

    /* Ensure Plyr fills container properly */
    .video-frame .plyr { width: 100%; height: 100%; }
    .video-frame .plyr__video-wrapper { width: 100%; height: 100%; }

    /* Letterbox bars (cinematic widescreen feel) */
    .video-frame::before,
    .video-frame::after {
        content: "";
        position: absolute;
        left: 0;
        right: 0;
        height: 0%;
        background: #000;
        z-index: 1;
        pointer-events: none;
        transition: height 0.6s var(--ease);
    }
    .video-frame::before { top: 0; }
    .video-frame::after { bottom: 0; }

    /* Now Showing bar */
    .now-showing {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 14px 22px;
        background: linear-gradient(90deg, rgba(212,168,83,0.06), rgba(198,40,40,0.03), transparent);
        border-bottom: 1px solid var(--glass-border);
    }
    .ns-icon {
        width: 28px;
        height: 28px;
        border-radius: 50%;
        background: var(--gradient-gold);
        display: grid;
        place-items: center;
        flex-shrink: 0;
        box-shadow: 0 0 12px var(--gold-glow);
    }
    .ns-icon i { font-size: 9px; color: var(--bg-void); }
    .ns-text {
        font-size: 11px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 2.5px;
        color: var(--gold);
    }
    .ns-line {
        flex: 1;
        height: 1px;
        background: linear-gradient(90deg, var(--glass-border), transparent);
    }
    .ns-reel {
        font-size: 13px;
        color: var(--text-tertiary);
        animation: reelSpin 3s linear infinite;
    }

    /* Player info */
    .pinfo {
        padding: 26px 28px 30px;
        background: linear-gradient(180deg, var(--bg-card) 0%, rgba(12,10,18,0.4) 100%);
        position: relative;
    }

    /* Subtle spotlight on info area */
    .pinfo::before {
        content: "";
        position: absolute;
        top: 0;
        left: 50%;
        transform: translateX(-50%);
        width: 60%;
        height: 100%;
        background: radial-gradient(ellipse at 50% 0%, rgba(212,168,83,0.03) 0%, transparent 70%);
        pointer-events: none;
    }

    .pinfo-top {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 20px;
        margin-bottom: 20px;
        position: relative;
    }
    .pinfo-title {
        font-family: 'Playfair Display', serif;
        font-size: 26px;
        font-weight: 800;
        letter-spacing: -0.3px;
        line-height: 1.25;
        color: var(--warm-white);
        word-break: break-word;
        text-shadow: 0 2px 20px rgba(212,168,83,0.1);
    }
    .size-badge {
        flex-shrink: 0;
        padding: 8px 18px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 800;
        letter-spacing: 0.5px;
        background: var(--gold-subtle);
        border: 1px solid rgba(212,168,83,0.15);
        color: var(--gold);
        white-space: nowrap;
        display: flex;
        align-items: center;
        gap: 7px;
    }
    .size-badge i { font-size: 11px; opacity: 0.7; }

    /* Cinema Tags */
    .cinema-tags {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 24px;
    }
    .ctag {
        padding: 5px 14px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.6px;
        text-transform: uppercase;
        border: 1px solid var(--glass-border);
        background: rgba(18,16,28,0.6);
        color: var(--text-secondary);
        display: inline-flex;
        align-items: center;
        gap: 6px;
        transition: all var(--dur) var(--ease);
    }
    .ctag i { font-size: 9px; }
    .ctag:hover { border-color: var(--glass-border-hover); transform: translateY(-1px); }
    .ctag-gold { color: var(--gold); border-color: rgba(212,168,83,0.15); background: rgba(212,168,83,0.04); }
    .ctag-crimson { color: var(--crimson-light); border-color: rgba(198,40,40,0.15); background: rgba(198,40,40,0.04); }
    .ctag-teal { color: var(--teal-cinema); border-color: rgba(0,137,123,0.15); background: rgba(0,137,123,0.04); }
    .ctag-steel { color: var(--steel); border-color: rgba(120,144,156,0.15); background: rgba(120,144,156,0.04); }

    /* CTA buttons */
    .cta-grid {
        display: grid;
        grid-template-columns: 1fr;
        gap: 10px;
    }

    /* Button base */
    .btn-base {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 10px;
        padding: 15px 20px;
        border-radius: var(--radius-md);
        font-family: inherit;
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 0.3px;
        cursor: pointer;
        border: none;
        text-decoration: none;
        position: relative;
        overflow: hidden;
        transition: all var(--dur) var(--ease);
        -webkit-tap-highlight-color: transparent;
    }
    .btn-base i { font-size: 13px; }

    /* Ripple */
    .btn-base .ripple {
        position: absolute;
        border-radius: 50%;
        background: rgba(255,255,255,0.2);
        transform: scale(0);
        animation: rippleOut 0.6s ease-out;
        pointer-events: none;
    }

    .btn-gold {
        background: var(--gradient-gold);
        color: var(--bg-void);
        font-weight: 800;
        box-shadow: 0 4px 24px var(--gold-glow), inset 0 1px 0 rgba(255,255,255,0.2);
    }
    .btn-gold:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 36px var(--gold-glow), inset 0 1px 0 rgba(255,255,255,0.2);
    }
    .btn-gold:active { transform: translateY(0); }

    .btn-dark {
        background: var(--bg-elevated);
        color: var(--text-primary);
        border: 1px solid var(--glass-border);
    }
    .btn-dark:hover {
        background: var(--bg-card-hover);
        border-color: var(--glass-border-hover);
        transform: translateY(-1px);
    }

    .btn-hero {
        grid-column: 1 / -1;
        background: var(--gradient-gold);
        color: var(--bg-void);
        font-size: 14px;
        font-weight: 800;
        padding: 17px 24px;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        box-shadow: 0 6px 30px var(--gold-glow), inset 0 1px 0 rgba(255,255,255,0.2);
        position: relative;
    }
    .btn-hero::before {
        content: "";
        position: absolute;
        inset: 0;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.15), transparent);
        transform: translateX(-100%);
        transition: transform 0.6s var(--ease);
    }
    .btn-hero:hover::before { transform: translateX(100%); }
    .btn-hero:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 44px var(--gold-glow), inset 0 1px 0 rgba(255,255,255,0.2);
    }
    .btn-hero:active { transform: translateY(0); }

    /* ── Bottom sections ── */
    .sidebar {
        display: flex;
        flex-direction: column;
        gap: 20px;
        animation: cinemaReveal 1s 0.15s var(--ease-dramatic) forwards;
        opacity: 0;
    }

    .s-card {
        background: var(--bg-card);
        border: 1px solid var(--glass-border);
        border-radius: var(--radius-xl);
        padding: 24px;
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        transition: border-color var(--dur) var(--ease), box-shadow var(--dur) var(--ease);
        position: relative;
        overflow: hidden;
    }
    .s-card::before {
        content: "";
        position: absolute;
        top: 0;
        left: 20%;
        right: 20%;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(212,168,83,0.15), transparent);
    }
    .s-card::after {
        content: "";
        position: absolute;
        inset: 0;
        border-radius: inherit;
        background: radial-gradient(circle at var(--mouse-x, 50%) var(--mouse-y, 50%), rgba(212,168,83,0.06) 0%, transparent 50%);
        opacity: 0;
        transition: opacity 0.4s var(--ease);
        pointer-events: none;
    }
    .s-card:hover::after { opacity: 1; }
    .s-card:hover {
        border-color: var(--glass-border-hover);
        box-shadow: 0 8px 40px rgba(0,0,0,0.3), 0 0 60px rgba(212,168,83,0.04);
        transform: translateY(-2px);
    }
    .s-card { transition: border-color var(--dur) var(--ease), box-shadow var(--dur) var(--ease), transform var(--dur) var(--ease); }

    .s-heading {
        font-size: 10px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 2px;
        color: var(--gold);
        margin-bottom: 18px;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .s-heading i { font-size: 12px; }
    .s-heading-line {
        flex: 1;
        height: 1px;
        background: linear-gradient(90deg, var(--glass-border), transparent);
    }

    /* Player buttons — single horizontal row */
    .ext-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 10px;
    }
    .ext-btn {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 13px 14px;
        border-radius: var(--radius-sm);
        font-family: inherit;
        font-size: 12.5px;
        font-weight: 700;
        cursor: pointer;
        border: 1px solid var(--glass-border-subtle);
        background: rgba(18,16,28,0.5);
        color: var(--text-primary);
        transition: all var(--dur) var(--ease);
        -webkit-tap-highlight-color: transparent;
        letter-spacing: 0.2px;
    }
    .ext-btn:hover {
        border-color: var(--glass-border-hover);
        background: var(--bg-card-hover);
        transform: translateY(-3px) scale(1.02);
        box-shadow: 0 12px 32px rgba(0,0,0,0.4), 0 0 20px rgba(212,168,83,0.06);
    }
    .ext-btn:active { transform: translateY(0) scale(0.98); }
    .ext-icon {
        width: 30px;
        height: 30px;
        border-radius: var(--radius-xs);
        display: grid;
        place-items: center;
        font-size: 11px;
        flex-shrink: 0;
    }
    .ext-icon.vlc { background: rgba(212,168,83,0.1); color: var(--gold); }
    .ext-icon.mx { background: rgba(0,137,123,0.1); color: var(--teal-cinema); }
    .ext-icon.sp { background: rgba(198,40,40,0.1); color: var(--crimson-light); }
    .ext-icon.pi { background: rgba(255,107,53,0.1); color: var(--ember); }



    /* Stats mini-grid */
    .stats {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
        gap: 1px;
        background: var(--glass-border);
        border-radius: var(--radius-sm);
        overflow: hidden;
        margin-bottom: 18px;
    }
    .stat-cell {
        padding: 16px 10px;
        background: var(--bg-elevated);
        text-align: center;
        transition: all var(--dur) var(--ease);
        position: relative;
    }
    .stat-cell:hover { background: rgba(25,22,40,0.8); }
    .stat-cell:hover .stat-val { transform: scale(1.1); }
    .stat-cell .stat-label {
        font-size: 9px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: var(--text-tertiary);
        margin-bottom: 6px;
    }
    .stat-cell .stat-val {
        font-size: 14px;
        font-weight: 800;
        transition: transform var(--dur) var(--ease-spring);
    }
    .stat-val.c-gold { color: var(--gold); }
    .stat-val.c-green { color: var(--teal-cinema); }
    .stat-val.c-ember { color: var(--ember); }
    .stat-val.c-crimson { color: var(--crimson-light); }
    .stat-val.c-steel { color: var(--steel); }

    /* Stream info card */
    .stream-info {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 14px;
    }
    .info-block {
        padding: 16px;
        border-radius: var(--radius-sm);
        background: rgba(18,16,28,0.5);
        border: 1px solid var(--glass-border-subtle);
        transition: all var(--dur) var(--ease);
    }
    .info-block:hover {
        border-color: var(--glass-border-hover);
        background: var(--bg-card-hover);
        transform: translateY(-1px);
    }
    .info-block-label {
        font-size: 9px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        color: var(--text-tertiary);
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .info-block-label i { font-size: 10px; color: var(--gold); }
    .info-block-val {
        font-size: 15px;
        font-weight: 800;
        color: var(--text-primary);
    }

    /* Animated live viewers */
    .live-indicator {
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .live-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--teal-cinema);
        animation: livePulse 1.5s ease-in-out infinite;
        box-shadow: 0 0 10px var(--teal-glow);
    }
    @keyframes livePulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.5; transform: scale(0.8); }
    }

    /* Floating particles */
    .particles {
        position: fixed;
        inset: 0;
        pointer-events: none;
        z-index: 0;
        overflow: hidden;
    }
    .particle {
        position: absolute;
        width: 2px;
        height: 2px;
        background: var(--gold);
        border-radius: 50%;
        opacity: 0;
        animation: particleFloat var(--p-dur, 8s) var(--p-delay, 0s) ease-in-out infinite;
    }
    @keyframes particleFloat {
        0% { opacity: 0; transform: translateY(100vh) scale(0); }
        10% { opacity: 0.6; }
        90% { opacity: 0.6; }
        100% { opacity: 0; transform: translateY(-10vh) scale(1); }
    }

    /* Features list */
    .features {
        display: flex;
        flex-direction: column;
        gap: 10px;
        margin-bottom: 20px;
    }
    .feat-row {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 11px 14px;
        border-radius: var(--radius-xs);
        background: rgba(18,16,28,0.4);
        border: 1px solid transparent;
        transition: all var(--dur) var(--ease);
    }
    .feat-row:hover {
        border-color: var(--glass-border);
        background: rgba(25,22,40,0.5);
        transform: translateX(6px);
    }
    .feat-icon {
        width: 34px;
        height: 34px;
        border-radius: 50%;
        display: grid;
        place-items: center;
        font-size: 12px;
        flex-shrink: 0;
    }
    .feat-icon.fi-gold { background: rgba(212,168,83,0.08); color: var(--gold); border: 1px solid rgba(212,168,83,0.1); }
    .feat-icon.fi-teal { background: rgba(0,137,123,0.08); color: var(--teal-cinema); border: 1px solid rgba(0,137,123,0.1); }
    .feat-icon.fi-ember { background: rgba(255,107,53,0.08); color: var(--ember); border: 1px solid rgba(255,107,53,0.1); }
    .feat-text { font-size: 13px; font-weight: 600; color: var(--text-secondary); letter-spacing: 0.1px; }

    /* Telegram CTA */
    .tg-cta {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 10px;
        width: 100%;
        padding: 15px;
        border-radius: var(--radius-sm);
        font-family: inherit;
        font-size: 13px;
        font-weight: 700;
        cursor: pointer;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        color: var(--warm-white);
        border: 1px solid rgba(212,168,83,0.2);
        background: linear-gradient(135deg, rgba(212,168,83,0.1), rgba(198,40,40,0.06));
        box-shadow: 0 4px 20px rgba(212,168,83,0.08);
        transition: all var(--dur) var(--ease);
        -webkit-tap-highlight-color: transparent;
    }
    .tg-cta:hover {
        transform: translateY(-2px);
        border-color: rgba(212,168,83,0.35);
        box-shadow: 0 8px 30px rgba(212,168,83,0.15);
        background: linear-gradient(135deg, rgba(212,168,83,0.15), rgba(198,40,40,0.08));
    }
    .tg-cta:active { transform: translateY(0); }

    /* ── Modal ── */
    .modal-bg {
        display: none;
        position: fixed;
        inset: 0;
        background: rgba(2,1,4,0.85);
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        z-index: 500;
        place-items: center;
    }
    .modal-bg.active { display: grid; animation: fadeIn 0.3s var(--ease); }

    .modal-card {
        background: var(--bg-elevated);
        border: 1px solid var(--glass-border);
        border-radius: var(--radius-xl);
        padding: 40px 36px 36px;
        width: min(440px, 92vw);
        box-shadow: 0 40px 120px rgba(0,0,0,0.8), 0 0 80px rgba(212,168,83,0.06);
        text-align: center;
        animation: modalCinema 0.5s var(--ease-dramatic);
        position: relative;
        overflow: hidden;
    }
    .modal-card::before {
        content: "";
        position: absolute;
        top: 0;
        left: 20%;
        right: 20%;
        height: 1px;
        background: var(--gradient-gold);
        opacity: 0.5;
    }
    .modal-ring {
        width: 70px;
        height: 70px;
        border-radius: 50%;
        background: var(--gradient-gold);
        display: grid;
        place-items: center;
        margin: 0 auto 22px;
        position: relative;
        box-shadow: 0 0 50px var(--gold-glow);
    }
    .modal-ring i { font-size: 26px; color: var(--bg-void); animation: bounceArrow 1.5s ease infinite; }
    .modal-ring::after {
        content: "";
        position: absolute;
        inset: -5px;
        border-radius: 50%;
        border: 2px solid transparent;
        border-top-color: var(--gold-light);
        animation: spinRing 1.5s linear infinite;
    }
    .modal-card h3 {
        font-family: 'Playfair Display', serif;
        font-size: 22px;
        font-weight: 800;
        margin-bottom: 6px;
        letter-spacing: -0.3px;
        color: var(--warm-white);
    }
    .modal-card .modal-sub {
        color: var(--text-secondary);
        font-size: 13px;
        margin-bottom: 28px;
        min-height: 22px;
        letter-spacing: 0.2px;
    }
    .prog-outer {
        width: 100%;
        height: 6px;
        border-radius: 999px;
        background: rgba(212,168,83,0.06);
        overflow: hidden;
        position: relative;
    }
    .prog-inner {
        height: 100%;
        border-radius: 999px;
        background: var(--gradient-gold);
        width: 0%;
        transition: width 0.7s var(--ease);
        position: relative;
    }
    .prog-inner::after {
        content: "";
        position: absolute;
        inset: 0;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
        animation: shimmer 1.5s infinite;
    }
    .modal-pct {
        margin-top: 14px;
        font-size: 12px;
        font-weight: 800;
        color: var(--gold);
        letter-spacing: 1px;
    }

    /* ── Toast ── */
    .toast-bar {
        position: fixed;
        bottom: 28px;
        left: 50%;
        z-index: 600;
        padding: 14px 24px;
        border-radius: 999px;
        background: var(--bg-elevated);
        border: 1px solid rgba(212,168,83,0.15);
        box-shadow: 0 20px 60px rgba(0,0,0,0.6), 0 0 30px rgba(212,168,83,0.06);
        color: var(--text-primary);
        font-family: inherit;
        font-weight: 700;
        font-size: 13px;
        display: flex;
        align-items: center;
        gap: 10px;
        white-space: nowrap;
        transform: translateX(-50%) translateY(120px);
        transition: transform 0.5s var(--ease-spring);
        pointer-events: none;
        letter-spacing: 0.2px;
    }
    .toast-bar.active { transform: translateX(-50%) translateY(0); }
    .toast-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        background: var(--gold);
        box-shadow: 0 0 10px var(--gold-glow);
    }

    /* ── Mobile bottom bar ── */
    .mobile-bar {
        display: none;
        position: fixed;
        bottom: 0;
        left: 0;
        right: 0;
        z-index: 90;
        padding: 12px 16px calc(12px + env(safe-area-inset-bottom, 0px));
        background: rgba(6,5,10,0.9);
        backdrop-filter: blur(24px);
        -webkit-backdrop-filter: blur(24px);
        border-top: 1px solid var(--glass-border);
        gap: 10px;
    }
    .mobile-bar .mb-btn {
        flex: 1;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        padding: 14px 8px;
        border-radius: var(--radius-sm);
        font-family: inherit;
        font-size: 12px;
        font-weight: 700;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        cursor: pointer;
        border: none;
        -webkit-tap-highlight-color: transparent;
        transition: all var(--dur) var(--ease);
    }
    .mb-copy {
        background: var(--bg-elevated);
        color: var(--text-primary);
        border: 1px solid var(--glass-border) !important;
    }
    .mb-dl {
        background: var(--gradient-gold);
        color: var(--bg-void);
        font-weight: 800;
    }
    .mb-dl:active, .mb-copy:active { transform: scale(0.97); }

    /* ── Footer ── */
    .ft {
        position: relative;
        z-index: 1;
        max-width: 1200px;
        margin: 70px auto 0;
        padding: 32px 32px 40px;
        border-top: 1px solid var(--glass-border);
    }
    .ft::before {
        content: "";
        position: absolute;
        top: -1px;
        left: 30%;
        right: 30%;
        height: 1px;
        background: var(--gradient-gold);
        opacity: 0.3;
    }
    .ft-inner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 16px;
    }
    .ft-brand {
        display: flex;
        align-items: center;
        gap: 10px;
        font-family: 'Playfair Display', serif;
        font-weight: 800;
        font-size: 16px;
    }
    .ft-brand .ft-icon {
        width: 24px;
        height: 24px;
        border-radius: 6px;
        background: var(--gradient-gold);
        display: grid;
        place-items: center;
    }
    .ft-brand .ft-icon i { font-size: 8px; color: var(--bg-void); }
    .ft-brand-text {
        background: var(--gradient-gold);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .ft-links { display: flex; gap: 28px; }
    .ft-links a {
        color: var(--text-secondary);
        text-decoration: none;
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        transition: color var(--dur) var(--ease);
    }
    .ft-links a:hover { color: var(--gold); }
    .ft-copy {
        font-size: 12px;
        color: var(--text-tertiary);
        font-weight: 600;
        letter-spacing: 0.3px;
    }

    /* ═══ Keyframes ═══ */
    @keyframes cinemaReveal {
        0%   { opacity: 0; transform: translateY(40px) scale(0.96); filter: blur(8px); }
        60%  { filter: blur(0px); }
        100% { opacity: 1; transform: translateY(0) scale(1); filter: blur(0px); }
    }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
    @keyframes pingPulse {
        75%, 100% { transform: scale(2.5); opacity: 0; }
    }
    @keyframes spotPulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.6; }
    }
    @keyframes spotDrift {
        0%   { transform: translate(0, 0) scale(1); }
        50%  { transform: translate(30px, -20px) scale(1.08); }
        100% { transform: translate(-15px, 15px) scale(0.95); }
    }
    @keyframes reelSpin {
        to { transform: rotate(360deg); }
    }
    @keyframes grainShift {
        0%   { transform: translate(0, 0); }
        25%  { transform: translate(-2px, 1px); }
        50%  { transform: translate(1px, -1px); }
        75%  { transform: translate(-1px, -2px); }
        100% { transform: translate(2px, 1px); }
    }
    @keyframes spinRing {
        to { transform: rotate(360deg); }
    }
    @keyframes bounceArrow {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(3px); }
    }
    @keyframes shimmer {
        0% { transform: translateX(-100%); }
        100% { transform: translateX(200%); }
    }
    @keyframes modalCinema {
        0%   { opacity: 0; transform: scale(0.9) translateY(20px); filter: blur(4px); }
        100% { opacity: 1; transform: scale(1) translateY(0); filter: blur(0); }
    }
    @keyframes rippleOut {
        to { transform: scale(3); opacity: 0; }
    }
    @keyframes titleGlow {
        0%, 100% { text-shadow: 0 2px 20px rgba(212,168,83,0.1); }
        50% { text-shadow: 0 4px 40px rgba(212,168,83,0.3), 0 0 60px rgba(212,168,83,0.1); }
    }
    @keyframes cardFloat {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-4px); }
    }
    @keyframes slideInLeft {
        0% { opacity: 0; transform: translateX(-40px) rotate(-1deg); }
        100% { opacity: 1; transform: translateX(0) rotate(0); }
    }
    @keyframes slideInRight {
        0% { opacity: 0; transform: translateX(40px) rotate(1deg); }
        100% { opacity: 1; transform: translateX(0) rotate(0); }
    }
    @keyframes scaleIn {
        0% { opacity: 0; transform: scale(0.3) rotate(-5deg); }
        60% { transform: scale(1.08) rotate(1deg); }
        100% { opacity: 1; transform: scale(1) rotate(0); }
    }
    @keyframes breathe {
        0%, 100% { box-shadow: 0 0 20px rgba(212,168,83,0.08); }
        50% { box-shadow: 0 0 50px rgba(212,168,83,0.18), 0 0 100px rgba(212,168,83,0.06); }
    }
    @keyframes textReveal {
        0% { clip-path: inset(0 100% 0 0); }
        100% { clip-path: inset(0 0 0 0); }
    }
    @keyframes countUp {
        0% { opacity: 0; transform: translateY(10px) scale(0.8); }
        60% { transform: translateY(-2px) scale(1.05); }
        100% { opacity: 1; transform: translateY(0) scale(1); }
    }
    @keyframes floatUpDown {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-8px); }
    }
    @keyframes waveTag {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-3px); }
    }
    @keyframes neonFlicker {
        0%, 19%, 21%, 23%, 25%, 54%, 56%, 100% { opacity: 1; }
        20%, 24%, 55% { opacity: 0.6; }
    }
    @keyframes magneticPull {
        0% { transform: translateY(0) scale(1); }
        30% { transform: translateY(-4px) scale(1.03); }
        60% { transform: translateY(-2px) scale(1.01); }
        100% { transform: translateY(0) scale(1); }
    }
    @keyframes auroraShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    @keyframes typewriterBlink {
        0%, 49% { border-color: var(--gold); }
        50%, 100% { border-color: transparent; }
    }
    @keyframes badgeShimmer {
        0% { background-position: -200% center; }
        100% { background-position: 200% center; }
    }
    @keyframes iconBounce {
        0%, 100% { transform: translateY(0) rotate(0); }
        25% { transform: translateY(-3px) rotate(-5deg); }
        75% { transform: translateY(-1px) rotate(3deg); }
    }
    @keyframes glowRipple {
        0% { box-shadow: 0 0 0 0 rgba(212,168,83,0.3); }
        70% { box-shadow: 0 0 0 15px rgba(212,168,83,0); }
        100% { box-shadow: 0 0 0 0 rgba(212,168,83,0); }
    }
    @keyframes revealUp {
        0% { opacity: 0; transform: translateY(30px) scale(0.95); filter: blur(6px); }
        100% { opacity: 1; transform: translateY(0) scale(1); filter: blur(0); }
    }
    @keyframes slideUp {
        0% { opacity: 0; transform: translateY(20px); }
        100% { opacity: 1; transform: translateY(0); }
    }
    @keyframes pulseGold {
        0%, 100% { border-color: rgba(212,168,83,0.08); }
        50% { border-color: rgba(212,168,83,0.25); }
    }

    /* ── Animated section dividers ── */
    .section-divider {
        position: relative;
        height: 2px;
        background: linear-gradient(90deg, transparent, var(--glass-border), transparent);
        margin: 4px 0;
        overflow: hidden;
    }
    .section-divider::after {
        content: "";
        position: absolute;
        top: 0;
        left: -100%;
        width: 60%;
        height: 100%;
        background: linear-gradient(90deg, transparent, var(--gold), transparent);
        animation: scanLine 3s ease-in-out infinite;
    }
    @keyframes scanLine {
        0% { left: -60%; }
        100% { left: 100%; }
    }

    /* ── Card entrance stagger ── */
    .s-card.visible { animation: revealUp 0.9s var(--ease-dramatic) forwards; }
    .feat-row.visible { animation: slideInLeft 0.5s var(--ease-dramatic) forwards; opacity: 0; }
    .stat-cell.visible .stat-val { animation: countUp 0.6s var(--ease-spring) forwards; }
    .ext-btn.visible { animation: scaleIn 0.5s var(--ease-spring) forwards; opacity: 0; }
    .info-block.visible { animation: slideUp 0.5s var(--ease-dramatic) forwards; opacity: 0; }

    /* ── Glow hover for brand ── */
    .brand:hover .brand-mark {
        box-shadow: 0 2px 24px var(--gold-glow), 0 0 40px rgba(212,168,83,0.15);
        transform: scale(1.1) rotate(-3deg);
        transition: all 0.4s var(--ease-spring);
        animation: glowRipple 1s ease-out;
    }
    .brand-mark { transition: all 0.3s var(--ease); }
    .brand:hover .brand-name { filter: brightness(1.3); }

    /* ── Cinema tags wave ── */
    .ctag { animation: waveTag 3s ease-in-out infinite; }
    .ctag:nth-child(1) { animation-delay: 0s; }
    .ctag:nth-child(2) { animation-delay: 0.15s; }
    .ctag:nth-child(3) { animation-delay: 0.3s; }
    .ctag:nth-child(4) { animation-delay: 0.45s; }

    /* ── Badge shimmer ── */
    .badge-premium {
        background-size: 200% 100%;
        background-image: linear-gradient(90deg, rgba(212,168,83,0.12) 0%, rgba(232,201,122,0.2) 50%, rgba(212,168,83,0.12) 100%);
        animation: badgeShimmer 4s linear infinite;
    }
    .badge-live .pulse-dot { animation: pingPulse 2s cubic-bezier(0, 0, 0.2, 1) infinite, neonFlicker 3s ease-in-out infinite; }

    /* ── Feature icon bounce on hover ── */
    .feat-row:hover .feat-icon { animation: iconBounce 0.5s var(--ease-spring); }
    .feat-row:hover .feat-icon i { animation: neonFlicker 0.3s ease-out; }

    /* ── Now showing typing cursor ── */
    .ns-text {
        border-right: 2px solid var(--gold);
        padding-right: 4px;
        animation: typewriterBlink 1s step-end infinite;
    }

    /* ── Reel icon enhanced spin ── */
    .ns-reel i { animation: reelSpin 2s linear infinite; }
    .ns-reel:hover i { animation-duration: 0.5s; }

    /* ── Ext button magnetic hover ── */
    .ext-btn:hover .ext-icon { animation: magneticPull 0.6s var(--ease-spring); }
    .ext-btn:hover .ext-icon i { animation: neonFlicker 0.4s ease-out; }

    /* ── Info block hover glow ── */
    .info-block:hover .info-block-label i { animation: iconBounce 0.5s var(--ease-spring); }
    .info-block:hover .info-block-val { color: var(--gold-light); transition: color 0.3s var(--ease); }

    /* ── Stats cell hover ripple ── */
    .stat-cell:hover { animation: magneticPull 0.5s var(--ease-spring); }

    /* ── Tg CTA aurora background ── */
    .tg-cta {
        background-size: 300% 300%;
        animation: auroraShift 6s ease infinite;
    }

    /* ── Card border pulse ── */
    .s-card { animation: pulseGold 5s ease-in-out infinite; }

    /* ── Title glow animation ── */
    .pinfo-title { animation: titleGlow 4s ease-in-out infinite; }

    /* ── Breathing glow on player ── */
    .player-section { animation: cinemaReveal 1s var(--ease-dramatic) forwards, breathe 6s ease-in-out 1.5s infinite; }

    /* ── Size badge float ── */
    .size-badge { animation: floatUpDown 4s ease-in-out infinite; }

    /* ── Now showing icon glow ── */
    .ns-icon { animation: glowRipple 3s ease-in-out infinite; }

    /* ── Footer brand hover ── */
    .ft-brand:hover .ft-icon { animation: iconBounce 0.6s var(--ease-spring); }
    .ft-links a { transition: color var(--dur) var(--ease), transform var(--dur) var(--ease), text-shadow var(--dur) var(--ease); }
    .ft-links a:hover { color: var(--gold); transform: translateY(-2px); text-shadow: 0 0 10px rgba(212,168,83,0.3); }

    /* ═══ Responsive ═══ */
    @media (max-width: 1024px) {
        .ext-grid { grid-template-columns: repeat(4, 1fr); }
    }

    @media (max-width: 680px) {
        .main { padding: 16px 14px 0; }
        .nav-inner { padding: 0 16px; height: 56px; }
        .brand-name { font-size: 16px; }
        .brand-mark { width: 34px; height: 34px; }
        .brand-mark i { font-size: 11px; }
        .nav-right .badge:not(.badge-live) { display: none; }
        .film-strip { padding: 0 14px; }

        .ext-grid { grid-template-columns: 1fr 1fr; gap: 8px; }
        .ext-btn { padding: 12px; font-size: 12px; }
        .ext-icon { width: 28px; height: 28px; font-size: 10px; }

        .pinfo { padding: 18px 16px 20px; }
        .pinfo-title { font-size: 20px; }
        .pinfo-top { flex-direction: column; gap: 10px; }
        .size-badge { align-self: flex-start; padding: 6px 14px; font-size: 11px; }
        .cinema-tags { gap: 6px; }
        .ctag { padding: 4px 10px; font-size: 10px; }
        .cta-grid { grid-template-columns: 1fr; gap: 8px; }
        .btn-base { padding: 13px 16px; font-size: 12px; }
        .btn-hero { padding: 14px 20px; font-size: 13px; }

        .s-card { padding: 18px; border-radius: var(--radius-lg); }
        .s-heading { font-size: 9px; margin-bottom: 14px; }
        .stats { grid-template-columns: repeat(3, 1fr); }
        .stat-cell { padding: 12px 8px; }
        .stat-cell .stat-label { font-size: 8px; }
        .stat-cell .stat-val { font-size: 13px; }

        .feat-row { padding: 10px 12px; gap: 12px; }
        .feat-icon { width: 30px; height: 30px; font-size: 11px; }
        .feat-text { font-size: 12px; }

        .stream-info { grid-template-columns: 1fr 1fr; gap: 10px; }
        .info-block { padding: 14px; }
        .info-block-val { font-size: 13px; }

        .grid-layout { gap: 18px; }

        .player-section { border-radius: var(--radius-xl); }
        .now-showing { padding: 10px 14px; }
        .ns-text { font-size: 10px; letter-spacing: 2px; }

        .tg-cta { padding: 13px; font-size: 12px; }

        .ft { padding: 20px 16px 100px; margin-top: 40px; }
        .ft-inner { flex-direction: column; text-align: center; gap: 12px; }
        .ft-brand { font-size: 14px; }

        .mobile-bar { display: flex; animation: slideUpBar 0.5s 0.3s var(--ease-spring) both; }
        .mb-btn { padding: 12px 8px !important; font-size: 11px !important; }
        .toast-bar { bottom: 76px; font-size: 12px; padding: 12px 18px; }

        .curtain-top { height: 120px; }

        .ticker-item { font-size: 10px; gap: 6px; }
        .ticker { gap: 35px; }

        .swipe-hint { display: flex; }

        .modal-card { padding: 30px 24px 28px; }
        .modal-ring { width: 56px; height: 56px; }
        .modal-ring i { font-size: 22px; }
        .modal-card h3 { font-size: 19px; }

        .section-divider { margin: 0; }

        /* ═══ MOBILE PERFORMANCE OPTIMIZATIONS ═══ */

        /* Disable film grain (expensive SVG filter repaint) */
        body::before { display: none !important; }

        /* Simplify vignette to static, no repaint */
        body::after { display: none !important; }

        /* Drastically reduce spotlights — keep 1, hide rest */
        .spot-2, .spot-3, .spot-4 { display: none !important; }
        .spot-1 {
            width: 400px; height: 250px;
            animation: none !important;
            opacity: 0.5;
        }

        /* Remove expensive backdrop-filter on cards */
        .s-card {
            backdrop-filter: none !important;
            -webkit-backdrop-filter: none !important;
            background: rgba(18, 16, 28, 0.85);
        }

        /* Simplify nav blur for mobile */
        .nav {
            backdrop-filter: blur(12px) !important;
            -webkit-backdrop-filter: blur(12px) !important;
        }

        /* Hide heavy player decorations */
        .ambient-glow { display: none !important; }
        .player-ring { display: none !important; }
        .streak-light { display: none !important; }

        /* Kill all infinite CSS animations that cause repaint during scroll */
        .s-card { animation: none !important; }
        .sidebar { opacity: 1 !important; }
        .s-card.visible { animation: none !important; opacity: 1 !important; }
        .feat-row.visible { animation: none !important; opacity: 1 !important; }
        .ext-btn.visible { animation: none !important; opacity: 1 !important; }
        .info-block.visible { animation: none !important; opacity: 1 !important; }
        .player-section { animation: cinemaReveal 1s var(--ease-dramatic) forwards !important; }
        .pinfo-title { animation: none !important; }
        .size-badge { animation: none !important; }
        .ns-icon { animation: none !important; }
        .ctag { animation: none !important; }
        .badge-premium { animation: none !important; }
        .badge-live .pulse-dot { animation: pingPulse 2s cubic-bezier(0,0,0.2,1) infinite !important; }
        .badge-live .pulse-dot::after { animation: none !important; }
        .ns-reel { animation: reelSpin 3s linear infinite !important; }
        .film-strip-inner::before { animation: none !important; box-shadow: 0 0 20px var(--gold-glow); }
        .section-divider::after { animation: none !important; background: var(--gold); opacity: 0.3; left: 20% !important; width: 60% !important; }
        .ft::after { animation: none !important; }

        /* GPU-promote scrolling container */
        .main { will-change: scroll-position; }
        .grid-layout { contain: layout style; }

        /* Simplify card hover transitions */
        .s-card { transition: none !important; }
        .s-card::after { display: none !important; }
        .s-card:hover { transform: none !important; box-shadow: none !important; }

        /* Reduce particle count heavily */
        .particle:nth-child(n+8) { display: none !important; }

        /* Faster scroll progress (no shadow) */
        .scroll-progress {
            box-shadow: none !important;
            transition: none !important;
            will-change: width;
        }
    }

    @media (max-width: 480px) {
        .stream-info { grid-template-columns: 1fr; gap: 8px; }
        .info-block { padding: 12px; }
        .brand-name { font-size: 15px; }
        .stats { grid-template-columns: repeat(3, 1fr); }
        .stat-cell { padding: 10px 6px; }
    }

    @media (max-width: 420px) {
        .cta-grid { grid-template-columns: 1fr; gap: 8px; }
        .btn-hero { grid-column: auto; }
        .ext-grid { grid-template-columns: 1fr 1fr; gap: 6px; }
        .ext-btn { padding: 10px; font-size: 11px; gap: 8px; }
        .stats { grid-template-columns: repeat(3, 1fr); }
        .now-showing { padding: 10px 14px; }
        .pinfo-title { font-size: 18px; }
        .btn-hero { font-size: 12px; padding: 13px 16px; }
        .main { padding: 12px 10px 0; }
        .s-card { padding: 14px; }
    }

    @media (max-width: 360px) {
        .brand-name { font-size: 13px; }
        .pinfo-title { font-size: 16px; }
        .pinfo { padding: 14px 12px 16px; }
        .ctag { font-size: 9px; padding: 3px 8px; }
        .ft-brand { font-size: 13px; }
        .grid-layout { gap: 14px; }
    }

    /* ── Plyr cinematic theme ── */
    .plyr--video { border-radius: 0; }
    .plyr--full-ui input[type=range] { color: var(--gold); }
    .plyr__control--overlaid {
        background: var(--gradient-gold) !important;
        color: var(--bg-void) !important;
        box-shadow: 0 6px 30px var(--gold-glow) !important;
        border: none !important;
    }
    .plyr__control--overlaid svg { fill: var(--bg-void); }
    .plyr__control--overlaid:hover {
        background: var(--gold-light) !important;
        box-shadow: 0 8px 40px var(--gold-glow) !important;
    }
    .plyr__controls {
        background: linear-gradient(transparent, rgba(2,1,4,0.85)) !important;
        position: absolute !important;
        bottom: 0 !important;
        left: 0 !important;
        right: 0 !important;
        z-index: 5 !important;
        padding: 8px 10px !important;
    }
    .plyr__control:hover { background: rgba(212,168,83,0.2) !important; }
    .plyr__time { font-size: 13px !important; font-weight: 600 !important; color: var(--text-primary) !important; }
    .plyr__volume { max-width: 110px !important; }
    .plyr__progress__container { flex: 1 !important; }
    .plyr__menu__container { z-index: 10 !important; }

    /* ── Scroll progress bar ── */
    .scroll-progress {
        position: fixed;
        top: 0;
        left: 0;
        height: 3px;
        width: 0%;
        background: var(--gradient-gold);
        z-index: 200;
        transition: width 0.1s linear;
        box-shadow: 0 0 10px var(--gold-glow), 0 0 20px rgba(212,168,83,0.15);
    }

    /* ── Page loader ── */
    .page-loader {
        position: fixed;
        inset: 0;
        z-index: 9999;
        background: var(--bg-void);
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 24px;
        transition: opacity 0.6s var(--ease), visibility 0.6s;
    }
    .page-loader.hidden { opacity: 0; visibility: hidden; pointer-events: none; }
    .loader-brand {
        font-family: 'Playfair Display', serif;
        font-size: 28px;
        font-weight: 900;
        background: var(--gradient-gold);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        animation: loaderPulse 1.5s ease-in-out infinite;
    }
    .loader-bar {
        width: 120px;
        height: 3px;
        border-radius: 999px;
        background: rgba(212,168,83,0.1);
        overflow: hidden;
    }
    .loader-bar-inner {
        height: 100%;
        background: var(--gradient-gold);
        border-radius: 999px;
        animation: loaderFill 2.8s var(--ease) forwards;
    }
    @keyframes loaderPulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    @keyframes loaderFill {
        0% { width: 0%; }
        100% { width: 100%; }
    }

    /* ── Nav shrink on scroll ── */
    .nav.scrolled {
        background: rgba(6,5,10,0.92);
    }
    .nav.scrolled .nav-inner { height: 56px; }
    .nav.scrolled .brand-mark { width: 34px; height: 34px; }
    .nav.scrolled .brand-name { font-size: 18px; }
    .nav-inner, .brand-mark, .brand-name {
        transition: all 0.3s var(--ease);
    }

    /* ── Smooth hover glow trail on buttons ── */
    .btn-base::after {
        content: '';
        position: absolute;
        inset: 0;
        border-radius: inherit;
        opacity: 0;
        background: radial-gradient(circle at var(--btn-x, 50%) var(--btn-y, 50%), rgba(255,255,255,0.12) 0%, transparent 60%);
        transition: opacity 0.3s var(--ease);
        pointer-events: none;
    }
    .btn-base:hover::after { opacity: 1; }

    /* ── Mobile swipe hint for ext players ── */
    .swipe-hint {
        display: none;
        justify-content: center;
        align-items: center;
        gap: 8px;
        padding: 8px;
        font-size: 10px;
        font-weight: 700;
        color: var(--text-tertiary);
        letter-spacing: 1px;
        text-transform: uppercase;
        animation: swipeHintPulse 2s ease-in-out infinite;
    }
    .swipe-hint i { font-size: 12px; color: var(--gold); }
    @keyframes swipeHintPulse {
        0%, 100% { opacity: 0.5; transform: translateX(0); }
        50% { opacity: 1; transform: translateX(5px); }
    }

    /* ── Touch-active states ── */
    @media (hover: none) {
        .ext-btn:active {
            transform: scale(0.95) !important;
            background: var(--bg-card-hover) !important;
            border-color: var(--glass-border-hover) !important;
            transition-duration: 0.1s;
        }
        .feat-row:active {
            background: rgba(25,22,40,0.6) !important;
            transform: translateX(3px);
        }
        .s-card:active {
            transform: scale(0.99) !important;
            border-color: var(--glass-border-hover) !important;
        }
        .btn-base:active {
            transform: scale(0.96) !important;
            transition-duration: 0.1s;
        }
        .info-block:active {
            background: var(--bg-card-hover) !important;
            border-color: var(--glass-border-hover) !important;
        }
    }

    /* ── Smooth scroll ── */
    html { scroll-behavior: smooth; }

    /* ── Focus-visible for accessibility ── */
    .btn-base:focus-visible, .ext-btn:focus-visible, .mb-btn:focus-visible {
        outline: 2px solid var(--gold);
        outline-offset: 2px;
    }

    /* ── Card glass shimmer on hover ── */
    .s-card > * { position: relative; z-index: 1; }

    /* ── Custom cursor ── */
    .cursor-dot {
        position: fixed;
        width: 8px;
        height: 8px;
        background: var(--gold);
        border-radius: 50%;
        pointer-events: none;
        z-index: 10000;
        mix-blend-mode: difference;
        transition: width 0.2s var(--ease), height 0.2s var(--ease), background 0.2s var(--ease), opacity 0.2s;
        transform: translate(-50%, -50%);
        opacity: 0;
    }
    .cursor-ring {
        position: fixed;
        width: 36px;
        height: 36px;
        border: 1.5px solid rgba(212,168,83,0.4);
        border-radius: 50%;
        pointer-events: none;
        z-index: 10000;
        transition: width 0.3s var(--ease), height 0.3s var(--ease), border-color 0.3s var(--ease), opacity 0.2s;
        transform: translate(-50%, -50%);
        opacity: 0;
    }
    .cursor-dot.visible, .cursor-ring.visible { opacity: 1; }
    .cursor-dot.hovering {
        width: 14px;
        height: 14px;
        background: var(--gold-light);
    }
    .cursor-ring.hovering {
        width: 50px;
        height: 50px;
        border-color: var(--gold);
    }
    .cursor-dot.clicking {
        width: 5px;
        height: 5px;
        background: var(--warm-white);
    }
    .cursor-ring.clicking {
        width: 28px;
        height: 28px;
        border-color: var(--gold-light);
    }

    /* ── Ambient player glow ── */
    .ambient-glow {
        position: absolute;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        width: 110%;
        height: 130%;
        background: radial-gradient(ellipse at center, rgba(212,168,83,0.08) 0%, rgba(198,40,40,0.04) 40%, transparent 70%);
        filter: blur(60px);
        pointer-events: none;
        z-index: -1;
        animation: ambientPulse 5s ease-in-out infinite;
    }
    @keyframes ambientPulse {
        0%, 100% { opacity: 0.6; transform: translate(-50%, -50%) scale(1); }
        50% { opacity: 1; transform: translate(-50%, -50%) scale(1.05); }
    }

    /* ── Brand glitch on hover ── */
    .brand:hover .brand-name {
        animation: glitchText 0.4s ease-out;
    }
    @keyframes glitchText {
        0% { transform: translate(0); }
        20% { transform: translate(-2px, 1px); filter: hue-rotate(20deg); }
        40% { transform: translate(2px, -1px); filter: hue-rotate(-20deg); }
        60% { transform: translate(-1px, -1px); filter: hue-rotate(10deg); }
        80% { transform: translate(1px, 1px); }
        100% { transform: translate(0); filter: none; }
    }

    /* ── Holographic badge effect ── */
    .badge-premium::before {
        content: '';
        position: absolute;
        inset: 0;
        border-radius: inherit;
        background: linear-gradient(105deg, transparent 25%, rgba(255,255,255,0.08) 45%, rgba(212,168,83,0.1) 50%, rgba(255,255,255,0.08) 55%, transparent 75%);
        background-size: 250% 100%;
        animation: holoShift 3s linear infinite;
        pointer-events: none;
    }
    .badge-premium { position: relative; overflow: hidden; }
    @keyframes holoShift {
        0% { background-position: -100% 0; }
        100% { background-position: 200% 0; }
    }

    /* ── 3D tilt on cards ── */
    .s-card {
        transform-style: preserve-3d;
        perspective: 1000px;
    }

    /* ── Player ambient ring pulses ── */
    .player-ring {
        position: absolute;
        inset: -4px;
        border-radius: var(--radius-2xl);
        border: 1px solid transparent;
        pointer-events: none;
        z-index: -1;
    }
    .player-ring:nth-child(1) {
        animation: ringPulse 4s ease-in-out infinite;
        border-color: rgba(212,168,83,0.06);
    }
    .player-ring:nth-child(2) {
        inset: -10px;
        animation: ringPulse 4s 1s ease-in-out infinite;
        border-color: rgba(212,168,83,0.03);
    }
    .player-ring:nth-child(3) {
        inset: -18px;
        animation: ringPulse 4s 2s ease-in-out infinite;
        border-color: rgba(212,168,83,0.015);
    }
    @keyframes ringPulse {
        0%, 100% { opacity: 0.3; transform: scale(1); }
        50% { opacity: 1; transform: scale(1.01); }
    }

    /* ── Network status pill ── */
    .net-status {
        position: fixed;
        top: 80px;
        right: 20px;
        z-index: 150;
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 6px 14px;
        border-radius: 999px;
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.8px;
        text-transform: uppercase;
        background: rgba(6,5,10,0.8);
        backdrop-filter: blur(10px);
        border: 1px solid var(--glass-border);
        opacity: 0;
        transform: translateX(100px);
        transition: all 0.5s var(--ease-spring);
    }
    .net-status.visible {
        opacity: 1;
        transform: translateX(0);
    }
    .net-status.online { color: var(--teal-cinema); border-color: rgba(0,137,123,0.2); }
    .net-status.offline { color: var(--crimson-light); border-color: rgba(198,40,40,0.2); }
    .net-dot {
        width: 6px;
        height: 6px;
        border-radius: 50%;
        animation: livePulse 1.5s ease-in-out infinite;
    }
    .net-status.online .net-dot { background: var(--teal-cinema); box-shadow: 0 0 8px var(--teal-glow); }
    .net-status.offline .net-dot { background: var(--crimson-light); box-shadow: 0 0 8px var(--crimson-glow); }

    /* ── Back to top button ── */
    .back-top {
        position: fixed;
        bottom: 90px;
        right: 20px;
        z-index: 95;
        width: 44px;
        height: 44px;
        border-radius: 50%;
        background: rgba(6,5,10,0.8);
        backdrop-filter: blur(10px);
        border: 1px solid var(--glass-border);
        color: var(--gold);
        font-size: 16px;
        display: grid;
        place-items: center;
        cursor: pointer;
        opacity: 0;
        visibility: hidden;
        transform: translateY(20px) scale(0.8);
        transition: all 0.4s var(--ease-spring);
        -webkit-tap-highlight-color: transparent;
    }
    .back-top.visible {
        opacity: 1;
        visibility: visible;
        transform: translateY(0) scale(1);
    }
    .back-top:hover {
        background: rgba(212,168,83,0.12);
        border-color: rgba(212,168,83,0.3);
        transform: translateY(-3px) scale(1.1);
        box-shadow: 0 8px 25px rgba(212,168,83,0.15);
    }
    .back-top:active { transform: scale(0.95); }

    /* ── Keyboard hints overlay ── */
    .kbd-hints {
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 95;
        display: flex;
        flex-direction: column;
        gap: 6px;
        opacity: 0;
        visibility: hidden;
        transform: translateY(10px);
        transition: all 0.4s var(--ease);
    }
    .kbd-hints.visible {
        opacity: 1;
        visibility: visible;
        transform: translateY(0);
    }
    .kbd-hint {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 5px 10px;
        border-radius: var(--radius-xs);
        background: rgba(6,5,10,0.85);
        backdrop-filter: blur(10px);
        border: 1px solid var(--glass-border);
        font-size: 10px;
        color: var(--text-secondary);
        white-space: nowrap;
    }
    .kbd-key {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-width: 22px;
        height: 20px;
        padding: 0 5px;
        border-radius: 4px;
        background: rgba(212,168,83,0.08);
        border: 1px solid rgba(212,168,83,0.15);
        font-size: 10px;
        font-weight: 800;
        color: var(--gold);
        letter-spacing: 0;
    }

    /* ── Click confetti explosion ── */
    .confetti {
        position: fixed;
        width: 6px;
        height: 6px;
        pointer-events: none;
        z-index: 9998;
    }

    /* ── Cinema time greeting ── */
    .time-greeting {
        position: absolute;
        top: 14px;
        right: 22px;
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
        color: var(--text-tertiary);
        display: flex;
        align-items: center;
        gap: 6px;
        z-index: 2;
    }
    .time-greeting i { font-size: 10px; color: var(--gold); }

    @keyframes slideUpBar {
        from { transform: translateY(100%); }
        to { transform: translateY(0); }
    }

    /* ── Enhanced page loader with cinema reels ── */
    .loader-reel {
        position: absolute;
        font-size: 60px;
        color: rgba(212,168,83,0.04);
        animation: reelSpin 4s linear infinite;
    }
    .loader-reel:nth-child(1) { top: 20%; left: 15%; animation-duration: 6s; }
    .loader-reel:nth-child(2) { bottom: 20%; right: 15%; animation-duration: 8s; animation-direction: reverse; }
    .loader-tagline {
        font-size: 11px;
        font-weight: 600;
        color: var(--text-tertiary);
        letter-spacing: 3px;
        text-transform: uppercase;
        animation: loaderPulse 2s ease-in-out infinite;
        animation-delay: 0.5s;
    }

    /* ── Animated underline on nav brand ── */
    .brand::after {
        content: '';
        position: absolute;
        bottom: -4px;
        left: 54px;
        right: 0;
        height: 1px;
        background: var(--gradient-gold);
        transform: scaleX(0);
        transform-origin: left;
        transition: transform 0.4s var(--ease-dramatic);
    }
    .brand { position: relative; }
    .brand:hover::after { transform: scaleX(1); }

    /* ── Streak light on player ── */
    .streak-light {
        position: absolute;
        top: 0;
        left: -100%;
        width: 50%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(212,168,83,0.03), transparent);
        pointer-events: none;
        z-index: 1;
        animation: streakSweep 8s ease-in-out infinite;
    }
    @keyframes streakSweep {
        0%, 100% { left: -50%; opacity: 0; }
        50% { left: 100%; opacity: 1; }
    }

    /* ── Footer glow line ── */
    .ft::after {
        content: '';
        position: absolute;
        bottom: 0;
        left: 50%;
        transform: translateX(-50%);
        width: 80px;
        height: 2px;
        background: var(--gradient-gold);
        border-radius: 999px;
        box-shadow: 0 0 15px var(--gold-glow);
        animation: glowPulse 3s ease-in-out infinite;
    }
    .ft { position: relative; overflow: hidden; }

    /* ── Ext button number badge ── */
    .ext-btn { position: relative; }
    .ext-num {
        position: absolute;
        top: -4px;
        right: -4px;
        width: 18px;
        height: 18px;
        border-radius: 50%;
        background: var(--gradient-gold);
        font-size: 9px;
        font-weight: 900;
        color: var(--bg-void);
        display: grid;
        place-items: center;
        opacity: 0;
        transform: scale(0);
        transition: all 0.3s var(--ease-spring);
    }
    .ext-btn:hover .ext-num {
        opacity: 1;
        transform: scale(1);
    }

    /* ── Modal enhanced backdrop pattern ── */
    .modal-bg::before {
        content: '';
        position: absolute;
        inset: 0;
        background-image:
            radial-gradient(circle at 20% 30%, rgba(212,168,83,0.03) 0%, transparent 50%),
            radial-gradient(circle at 80% 70%, rgba(198,40,40,0.02) 0%, transparent 50%);
        pointer-events: none;
    }

    /* ── Ticker pause on hover ── */
    .ticker-wrap:hover .ticker {
        animation-play-state: paused;
    }

    /* ── Cinema film sprocket holes on player ── */
    .film-holes {
        position: absolute;
        top: 0;
        bottom: 0;
        width: 16px;
        display: flex;
        flex-direction: column;
        justify-content: space-evenly;
        align-items: center;
        padding: 8px 0;
        pointer-events: none;
        z-index: 2;
        opacity: 0.12;
    }
    .film-holes.left { left: 0; }
    .film-holes.right { right: 0; }
    .film-hole {
        width: 6px;
        height: 10px;
        border-radius: 2px;
        background: var(--gold);
    }

    /* ── Enhanced mobile safe areas + notch ── */
    @supports (padding: env(safe-area-inset-bottom)) {
        .mobile-bar {
            padding-bottom: calc(6px + env(safe-area-inset-bottom));
        }
        body { padding-bottom: env(safe-area-inset-bottom); }
    }

    /* ── Prefers reduced motion ── */
    @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after {
            animation-duration: 0.01ms !important;
            animation-iteration-count: 1 !important;
            transition-duration: 0.01ms !important;
        }
        .particles, .spotlights, .page-loader { display: none !important; }
        .scroll-progress { transition: none !important; }
        .cursor-dot, .cursor-ring { display: none !important; }
    }

    /* ── Hide custom cursor & kbd hints on touch ── */
    @media (hover: none) {
        .cursor-dot, .cursor-ring { display: none !important; }
        .kbd-hints { display: none !important; }
        .back-top { bottom: 75px; right: 14px; width: 40px; height: 40px; font-size: 14px; }
    }

    /* ── Net status responsive ── */
    @media (max-width: 680px) {
        .net-status { top: auto; bottom: 75px; right: 14px; left: 14px; justify-content: center; }
        .time-greeting { display: none; }
        .film-holes { display: none; }
    }

    ::selection { background: rgba(212,168,83,0.3); color: #fff; }
    ::-webkit-scrollbar { width: 5px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: rgba(212,168,83,0.12); border-radius: 999px; }
    ::-webkit-scrollbar-thumb:hover { background: rgba(212,168,83,0.25); }
    </style>
</head>

<body>

<!-- Custom cursor -->
<div class="cursor-dot" id="cursorDot"></div>
<div class="cursor-ring" id="cursorRing"></div>

<!-- Page Loader -->
<div class="page-loader" id="pageLoader">
    <i class="fas fa-compact-disc loader-reel"></i>
    <i class="fas fa-compact-disc loader-reel"></i>
    <div class="loader-brand">AckerStreamX</div>
    <div class="loader-bar"><div class="loader-bar-inner"></div></div>
    <div class="loader-tagline">Preparing Your Cinema</div>
</div>
<script>
(function(){
    var l = document.getElementById('pageLoader');
    if (!l) return;
    setTimeout(function(){
        l.classList.add('hidden');
        setTimeout(function(){ try { l.remove(); } catch(e){} }, 800);
    }, 3000);
})();
</script>

<!-- Scroll Progress -->
<div class="scroll-progress" id="scrollProgress"></div>

<!-- Cinematic spotlights -->
<div class="spotlights">
    <div class="spot spot-1"></div>
    <div class="spot spot-2"></div>
    <div class="spot spot-3"></div>
    <div class="spot spot-4"></div>
</div>

<!-- Curtain top gradient -->
<div class="curtain-top"></div>

<!-- Navigation -->
<nav class="nav">
    <div class="nav-inner">
        <a class="brand" href="#">
            <div class="brand-mark"><i class="fas fa-play"></i></div>
            <span class="brand-name">AckerStreamX</span>
        </a>
        <div class="nav-right">
            <span class="badge badge-live"><span class="pulse-dot"></span> Streaming</span>
            <span class="badge badge-premium"><i class="fas fa-crown"></i> Premium</span>
            <span class="badge"><i class="fas fa-film"></i> HD</span>
        </div>
    </div>
</nav>

<!-- Scrolling Ticker -->
<div class="ticker-wrap">
    <div class="ticker">
        <span class="ticker-item t-gold"><i class="fas fa-star"></i> Premium Streaming</span>
        <span class="ticker-item"><i class="fas fa-bolt"></i> Ultra-Fast Delivery</span>
        <span class="ticker-item"><i class="fas fa-shield-halved"></i> Encrypted Transfer</span>
        <span class="ticker-item t-gold"><i class="fas fa-film"></i> Cinema Quality</span>
        <span class="ticker-item t-gold"><i class="fas fa-crown"></i> No Ads</span>
        <span class="ticker-item"><i class="fas fa-clock"></i> 24/7 Available</span>
        <span class="ticker-item"><i class="fas fa-wand-magic-sparkles"></i> AckerStreamX Powered</span>
        <span class="ticker-item t-gold"><i class="fas fa-star"></i> Premium Streaming</span>
        <span class="ticker-item"><i class="fas fa-bolt"></i> Ultra-Fast Delivery</span>
        <span class="ticker-item"><i class="fas fa-shield-halved"></i> Encrypted Transfer</span>
        <span class="ticker-item t-gold"><i class="fas fa-film"></i> Cinema Quality</span>
        <span class="ticker-item t-gold"><i class="fas fa-crown"></i> No Ads</span>
        <span class="ticker-item"><i class="fas fa-clock"></i> 24/7 Available</span>
    </div>
</div>

<!-- Floating particles -->
<div class="particles" id="particleContainer"></div>

<!-- Film strip divider -->
<div class="film-strip"><div class="film-strip-inner"></div></div>

<!-- Main -->
<div class="main">
    <div class="grid-layout">

        <!-- ── Player ── -->
        <div class="player-section" id="playerSection">
            <div class="ambient-glow"></div>
            <div class="player-ring"></div>
            <div class="player-ring"></div>
            <div class="player-ring"></div>
            <div class="streak-light"></div>
            <div class="now-showing">
                <span class="time-greeting" id="timeGreeting"><i class="fas fa-clock"></i> <span id="greetText"></span></span>
                <div class="ns-icon"><i class="fas fa-play"></i></div>
                <span class="ns-text">Now Showing</span>
                <div class="ns-line"></div>
                <span class="ns-reel"><i class="fas fa-compact-disc"></i></span>
            </div>
            <div class="video-frame">
                <div class="film-holes left" id="filmHoles1"></div>
                <div class="film-holes right" id="filmHoles2"></div>
                <video id="player" playsinline controls>
                    <source src=__FILE_URL_HTML__ type="video/mp4">
                </video>
            </div>
            <div class="pinfo">
                <div class="pinfo-top">
                    <h1 class="pinfo-title">__FILE_NAME_HTML__</h1>
                    <span class="size-badge"><i class="fas fa-database"></i> __FILE_SIZE__</span>
                </div>
                <div class="cinema-tags">
                    <span class="ctag ctag-gold"><i class="fas fa-star"></i> Premiere Quality</span>
                    <span class="ctag ctag-teal"><i class="fas fa-bolt"></i> Direct Stream</span>
                    <span class="ctag ctag-crimson"><i class="fas fa-shield-halved"></i> Secured</span>
                    <span class="ctag ctag-steel"><i class="fas fa-film"></i> Cinematic</span>
                </div>
                <div class="cta-grid">
                    <button class="btn-base btn-gold" onclick="copyLink(event)"><i class="fas fa-link"></i> Copy Link</button>
                    <button class="btn-base btn-hero" onclick="openModal(event)"><i class="fas fa-cloud-arrow-down"></i> Download Now</button>
                </div>
            </div>
        </div>

        <!-- ── External Players (full-width under player) ── -->
        <div class="s-card sidebar">
            <div class="s-heading"><i class="fas fa-clapperboard"></i> External Players <span class="s-heading-line"></span></div>
            <div class="ext-grid">
                <button class="ext-btn" onclick="playOnline()">
                    <span class="ext-icon vlc"><i class="fas fa-play"></i></span>
                    VLC
                    <span class="ext-num">1</span>
                </button>
                <button class="ext-btn" onclick="playOnlineMx()">
                    <span class="ext-icon mx"><i class="fas fa-play"></i></span>
                    MX Player
                    <span class="ext-num">2</span>
                </button>
                <button class="ext-btn" onclick="playOnlinesp()">
                    <span class="ext-icon sp"><i class="fas fa-play"></i></span>
                    SPlayer
                    <span class="ext-num">3</span>
                </button>
                <button class="ext-btn" onclick="playOnlinepi()">
                    <span class="ext-icon pi"><i class="fas fa-play"></i></span>
                    PlayIt
                    <span class="ext-num">4</span>
                </button>
            </div>
            <div class="swipe-hint"><i class="fas fa-hand-point-right"></i> Tap to play externally</div>
        </div>
        <div class="s-card sidebar">
            <div class="s-heading"><i class="fas fa-info-circle"></i> Stream Information <span class="s-heading-line"></span></div>
            <div class="stream-info">
                <div class="info-block">
                    <div class="info-block-label"><i class="fas fa-file"></i> File Name</div>
                    <div class="info-block-val" style="font-size:13px; word-break:break-all;">__FILE_NAME_HTML__</div>
                </div>
                <div class="info-block">
                    <div class="info-block-label"><i class="fas fa-hard-drive"></i> File Size</div>
                    <div class="info-block-val">__FILE_SIZE__</div>
                </div>
                <div class="info-block">
                    <div class="info-block-label"><i class="fas fa-video"></i> Format</div>
                    <div class="info-block-val">MP4 / Video</div>
                </div>
                <div class="info-block">
                    <div class="info-block-label"><i class="fas fa-server"></i> Delivery</div>
                    <div class="info-block-val">Direct Stream</div>
                </div>
            </div>
        </div>

        <div class="section-divider"></div>

        <!-- ── Screening Details (bottom) ── -->
        <div class="s-card sidebar">
            <div class="s-heading"><i class="fas fa-ticket"></i> Screening Details <span class="s-heading-line"></span></div>
            <div class="stats">
                <div class="stat-cell">
                    <div class="stat-label">Status</div>
                    <div class="stat-val c-green"><span class="live-indicator"><span class="live-dot"></span> Live</span></div>
                </div>
                <div class="stat-cell">
                    <div class="stat-label">Size</div>
                    <div class="stat-val c-gold">__FILE_SIZE__</div>
                </div>
                <div class="stat-cell">
                    <div class="stat-label">Quality</div>
                    <div class="stat-val c-ember">Direct</div>
                </div>
                <div class="stat-cell">
                    <div class="stat-label">Speed</div>
                    <div class="stat-val c-steel">Max</div>
                </div>
                <div class="stat-cell">
                    <div class="stat-label">Protocol</div>
                    <div class="stat-val c-crimson">HTTPS</div>
                </div>
            </div>

            <div class="features">
                <div class="feat-row">
                    <div class="feat-icon fi-gold"><i class="fas fa-shield-halved"></i></div>
                    <span class="feat-text">Ad-free cinematic experience</span>
                </div>
                <div class="feat-row">
                    <div class="feat-icon fi-teal"><i class="fas fa-gauge-high"></i></div>
                    <span class="feat-text">Zero-buffering direct stream</span>
                </div>
                <div class="feat-row">
                    <div class="feat-icon fi-ember"><i class="fas fa-user-check"></i></div>
                    <span class="feat-text">No registration required</span>
                </div>
                <div class="feat-row">
                    <div class="feat-icon fi-gold"><i class="fas fa-lock"></i></div>
                    <span class="feat-text">End-to-end encrypted transfer</span>
                </div>

            </div>

            <button class="tg-cta" onclick="openTgBot()">
                <i class="fab fa-telegram"></i> Get Your Own Stream Deck
            </button>
        </div>
    </div>
</div>

<!-- Mobile bottom bar -->
<div class="mobile-bar">
    <button class="mb-btn mb-copy" onclick="copyLink(event)"><i class="fas fa-link"></i> Copy</button>
    <button class="mb-btn mb-dl" onclick="openModal(event)"><i class="fas fa-download"></i> Download</button>
</div>

<!-- Network status -->
<div class="net-status" id="netStatus">
    <span class="net-dot"></span>
    <span id="netText">Online</span>
</div>

<!-- Back to top -->
<button class="back-top" id="backTop" aria-label="Back to top">
    <i class="fas fa-chevron-up"></i>
</button>

<!-- Keyboard hints -->
<div class="kbd-hints" id="kbdHints">
    <div class="kbd-hint"><span class="kbd-key">Space</span> Play / Pause</div>
    <div class="kbd-hint"><span class="kbd-key">C</span> Copy Link</div>
    <div class="kbd-hint"><span class="kbd-key">D</span> Download</div>
    <div class="kbd-hint"><span class="kbd-key">?</span> Toggle Hints</div>
</div>

<!-- Toast -->
<div class="toast-bar" id="toast">
    <span class="toast-dot"></span>
    <span id="toast-msg">Link copied to clipboard</span>
</div>

<!-- Download Modal -->
<div class="modal-bg" id="downloadModal">
    <div class="modal-card">
        <div class="modal-ring"><i class="fas fa-arrow-down"></i></div>
        <h3>Preparing Your Download</h3>
        <p class="modal-sub" id="modalStatus">Initializing...</p>
        <div class="prog-outer">
            <div class="prog-inner" id="progBar"></div>
        </div>
        <div class="modal-pct" id="modalPct">0%</div>
    </div>
</div>

<!-- Footer -->
<footer class="ft">
    <div class="ft-inner">
        <div class="ft-brand">
            <div class="ft-icon"><i class="fas fa-play"></i></div>
            <span class="ft-brand-text">AckerStreamX</span>
        </div>
        <div class="ft-links">
            <a href="https://telegram.dog" target="_blank" rel="noopener">Report Issues</a>
            <a href="https://telegram.dog/sydney_sweeney_robot" target="_blank" rel="noopener">Get Your Bot</a>
        </div>
        <div class="ft-copy">&copy; ACKERSTREAMX&deg;</div>
    </div>
</footer>

<!-- Plyr -->
<script>
(function(){
    var s = document.createElement('script');
    s.src = 'https://cdnjs.cloudflare.com/ajax/libs/plyr/3.7.8/plyr.js';
    s.onload = function(){
        new Plyr('#player', {
            controls: ['play-large','play','progress','current-time','duration','mute','volume','settings','pip','fullscreen'],
            settings: ['quality','speed']
        });
    };
    s.onerror = function(){ console.warn('Plyr CDN failed to load'); };
    document.head.appendChild(s);
})();
</script>

<script>
(function() {
    'use strict';

    const fileName = __FILE_NAME_JS__;
    const currentUrl = window.location.href;
    const finalUrl = currentUrl.replace("/watch/", "/");
    const streamRoot = finalUrl.endsWith('/') ? finalUrl.slice(0, -1) : finalUrl;
    const isMobile = window.matchMedia('(max-width: 680px)').matches;



    /* ── Ripple effect ── */
    function spawnRipple(e, el) {
        const rect = el.getBoundingClientRect();
        const r = document.createElement('span');
        const size = Math.max(rect.width, rect.height);
        r.className = 'ripple';
        r.style.width = r.style.height = size + 'px';
        r.style.left = (e.clientX - rect.left - size / 2) + 'px';
        r.style.top  = (e.clientY - rect.top  - size / 2) + 'px';
        el.appendChild(r);
        r.addEventListener('animationend', () => r.remove());
    }

    /* ── Toast ── */
    let toastTimer;
    window.showToast = function(msg) {
        const t = document.getElementById("toast");
        document.getElementById("toast-msg").textContent = msg;
        t.classList.add("active");
        clearTimeout(toastTimer);
        toastTimer = setTimeout(() => t.classList.remove("active"), 3200);
    };

    /* ── Copy ── */
    window.copyLink = async function(e) {
        if (e && e.currentTarget) spawnRipple(e, e.currentTarget);
        try {
            await navigator.clipboard.writeText(finalUrl);
            showToast("Link copied to clipboard!");
        } catch (err) {
            prompt("Copy this link:", finalUrl);
        }
    };

    /* ── External players ── */
    window.playOnline   = () => { window.location.href = `vlc://${finalUrl}`; };
    window.playOnlineMx = () => { window.location.href = `intent:${finalUrl}#Intent;package=com.mxtech.videoplayer.ad;S.title=${fileName};end`; };
    window.playOnlinesp = () => { window.location.href = `intent:${finalUrl}#Intent;action=com.young.simple.player.playback_online;package=com.young.simple.player;end`; };
    window.playOnlinepi = () => { window.location.href = `playit://playerv2/video?url=${finalUrl}&title=${fileName}`; };

    /* ── Telegram ── */
    window.openTgBot = () => window.open("https://telegram.dog/sydney_sweeney_robot", "_blank");

    /* ── Download modal ── */
    window.openModal = function(e) {
        if (e && e.currentTarget) spawnRipple(e, e.currentTarget);
        const modal  = document.getElementById("downloadModal");
        const bar    = document.getElementById("progBar");
        const status = document.getElementById("modalStatus");
        const pct    = document.getElementById("modalPct");
        modal.classList.add("active");
        bar.style.width = "0%";
        pct.textContent = "0%";

        const steps = [
            { text: "Received download request...",  w: "15%",  p: "15%" },
            { text: "Verifying screening access...", w: "35%",  p: "35%" },
            { text: "Processing your file...",       w: "55%",  p: "55%" },
            { text: "Generating secure link...",     w: "80%",  p: "80%" },
            { text: "Launching download!",           w: "100%", p: "100%" },
        ];

        let i = 0;
        status.textContent = steps[0].text;
        bar.style.width    = steps[0].w;
        pct.textContent    = steps[0].p;

        const timer = setInterval(() => {
            i++;
            if (i < steps.length) {
                status.textContent = steps[i].text;
                bar.style.width    = steps[i].w;
                pct.textContent    = steps[i].p;
            } else {
                clearInterval(timer);
                modal.classList.remove("active");
                bar.style.width = "0%";
                pct.textContent = "0%";
                window.location.href = finalUrl + (finalUrl.includes('?') ? '&' : '?') + 'dl=1';
            }
        }, 850);
    };

    /* ── Disable inspection ── */
    document.addEventListener('contextmenu', e => e.preventDefault());
    document.addEventListener('keydown', function(e) {
        if (e.key === 'F12' || (e.ctrlKey && e.shiftKey && e.key === 'I') || (e.ctrlKey && e.key === 'u') || e.ctrlKey || e.shiftKey || e.altKey) {
            e.preventDefault();
        }
    });

    /* ── Stagger entrance for sidebar cards ── */
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                observer.unobserve(entry.target);

                // Stagger children animations
                const featRows = entry.target.querySelectorAll('.feat-row');
                featRows.forEach((row, i) => {
                    row.style.animationDelay = (i * 0.12) + 's';
                    row.classList.add('visible');
                });
                const extBtns = entry.target.querySelectorAll('.ext-btn');
                extBtns.forEach((btn, i) => {
                    btn.style.animationDelay = (i * 0.1) + 's';
                    btn.classList.add('visible');
                });
                const statCells = entry.target.querySelectorAll('.stat-cell');
                statCells.forEach((cell, i) => {
                    cell.style.animationDelay = (i * 0.12) + 's';
                    cell.classList.add('visible');
                });
                const infoBlocks = entry.target.querySelectorAll('.info-block');
                infoBlocks.forEach((block, i) => {
                    block.style.animationDelay = (i * 0.1) + 's';
                    block.classList.add('visible');
                });
            }
        });
    }, { threshold: 0.08 });
    document.querySelectorAll('.s-card').forEach(c => observer.observe(c));

    /* ── Spotlight follow cursor on cards ── */
    document.querySelectorAll('.s-card').forEach(card => {
        card.addEventListener('mousemove', (e) => {
            const rect = card.getBoundingClientRect();
            const x = ((e.clientX - rect.left) / rect.width * 100).toFixed(1);
            const y = ((e.clientY - rect.top) / rect.height * 100).toFixed(1);
            card.style.setProperty('--mouse-x', x + '%');
            card.style.setProperty('--mouse-y', y + '%');
        });
    });

    /* ── Magnetic tilt on ext buttons ── */
    document.querySelectorAll('.ext-btn').forEach(btn => {
        btn.addEventListener('mousemove', (e) => {
            const rect = btn.getBoundingClientRect();
            const x = (e.clientX - rect.left - rect.width / 2) / rect.width * 8;
            const y = (e.clientY - rect.top - rect.height / 2) / rect.height * 8;
            btn.style.transform = `translateY(-3px) scale(1.02) perspective(500px) rotateX(${-y}deg) rotateY(${x}deg)`;
        });
        btn.addEventListener('mouseleave', () => {
            btn.style.transform = '';
        });
    });

    /* ── Floating particles (fewer on mobile) ── */
    const particleContainer = document.getElementById('particleContainer');
    const particleCount = isMobile ? 6 : 25;
    if (particleContainer) {
        for (let i = 0; i < particleCount; i++) {
            const p = document.createElement('div');
            p.className = 'particle';
            p.style.left = Math.random() * 100 + '%';
            p.style.setProperty('--p-dur', (5 + Math.random() * 12) + 's');
            p.style.setProperty('--p-delay', (Math.random() * 10) + 's');
            const size = 1 + Math.random() * 3;
            p.style.width = p.style.height = size + 'px';
            if (Math.random() > 0.7) p.style.background = 'var(--crimson-light)';
            else if (Math.random() > 0.5) p.style.background = 'var(--ember)';
            particleContainer.appendChild(p);
        }
    }

    /* ── Smooth number counting for stats ── */
    function animateValue(el, start, end, suffix, duration) {
        const range = end - start;
        const startTime = performance.now();
        function step(ts) {
            const elapsed = ts - startTime;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            el.textContent = Math.floor(start + range * eased) + suffix;
            if (progress < 1) requestAnimationFrame(step);
        }
        requestAnimationFrame(step);
    }

    /* ── Parallax spotlights on mouse ── */
    let mouseX = 0, mouseY = 0, raf;
    function onMouse(e) {
        mouseX = (e.clientX / window.innerWidth - 0.5) * 2;
        mouseY = (e.clientY / window.innerHeight - 0.5) * 2;
    }
    function parallaxLoop() {
        const spots = document.querySelectorAll('.spot');
        spots.forEach((s, i) => {
            const strength = (i + 1) * 8;
            const rotateX = mouseY * 2;
            const rotateY = mouseX * 2;
            s.style.transform = `translate(${mouseX * strength}px, ${mouseY * strength}px) rotate(${rotateY * 0.5}deg)`;
        });
        raf = requestAnimationFrame(parallaxLoop);
    }
    if (window.matchMedia('(hover: hover)').matches) {
        document.addEventListener('mousemove', onMouse, { passive: true });
        parallaxLoop();
    }

    /* ── Auto-update "Now Showing" text ── */
    const nsTextEl = document.querySelector('.ns-text');
    if (nsTextEl) {
        const phrases = ['Now Showing', 'Streaming Live', 'On Air', 'Playing Now'];
        let phraseIdx = 0;
        setInterval(() => {
            phraseIdx = (phraseIdx + 1) % phrases.length;
            nsTextEl.style.opacity = '0';
            nsTextEl.style.transform = 'translateY(-5px)';
            setTimeout(() => {
                nsTextEl.textContent = phrases[phraseIdx];
                nsTextEl.style.opacity = '1';
                nsTextEl.style.transform = 'translateY(0)';
            }, 300);
        }, 4000);
        nsTextEl.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
    }

    /* ── Unified scroll handler (single rAF-throttled loop) ── */
    const scrollBar = document.getElementById('scrollProgress');
    const navEl = document.querySelector('.nav');
    const backTopBtn = document.getElementById('backTop');
    const filmStrip = document.querySelector('.film-strip-inner');
    let scrollRafPending = false;
    let prevNavScrolled = false;
    let prevBackTopVisible = false;

    window.addEventListener('scroll', () => {
        if (scrollRafPending) return;
        scrollRafPending = true;
        requestAnimationFrame(() => {
            const sy = window.scrollY;
            const docH = document.documentElement.scrollHeight - window.innerHeight;

            // Scroll progress bar
            if (scrollBar && docH > 0) {
                scrollBar.style.width = (sy / docH * 100) + '%';
            }

            // Nav shrink
            if (navEl) {
                const shouldScroll = sy > 50;
                if (shouldScroll !== prevNavScrolled) {
                    navEl.classList.toggle('scrolled', shouldScroll);
                    prevNavScrolled = shouldScroll;
                }
            }

            // Back to top
            if (backTopBtn) {
                const shouldShow = sy > 400;
                if (shouldShow !== prevBackTopVisible) {
                    backTopBtn.classList.toggle('visible', shouldShow);
                    prevBackTopVisible = shouldShow;
                }
            }

            // Film strip glow (desktop only)
            if (filmStrip && !isMobile) {
                filmStrip.style.opacity = 1 - Math.min(sy / 300, 1) * 0.5;
            }

            scrollRafPending = false;
        });
    }, { passive: true });

    /* ── Button glow trail ── */
    document.querySelectorAll('.btn-base').forEach(btn => {
        btn.addEventListener('mousemove', (e) => {
            const rect = btn.getBoundingClientRect();
            btn.style.setProperty('--btn-x', ((e.clientX - rect.left) / rect.width * 100) + '%');
            btn.style.setProperty('--btn-y', ((e.clientY - rect.top) / rect.height * 100) + '%');
        });
    });

    /* ── Smooth reveal for player info elements ── */
    const pinfo = document.querySelector('.pinfo');
    if (pinfo) {
        const revealEls = pinfo.querySelectorAll('.pinfo-top, .cinema-tags');
        revealEls.forEach((el, i) => {
            el.style.opacity = '0';
            el.style.transform = 'translateY(15px)';
            el.style.transition = `opacity 0.5s ${0.3 + i * 0.15}s var(--ease-dramatic), transform 0.5s ${0.3 + i * 0.15}s var(--ease-dramatic)`;
        });
        // Trigger after player section reveals
        setTimeout(() => {
            revealEls.forEach(el => {
                el.style.opacity = '1';
                el.style.transform = 'translateY(0)';
            });
        }, 600);
    }

    /* ── Cinema tags interactive tap feedback ── */
    document.querySelectorAll('.ctag').forEach(tag => {
        tag.addEventListener('click', function() {
            this.style.transform = 'scale(0.92)';
            this.style.transition = 'transform 0.1s ease';
            setTimeout(() => {
                this.style.transform = '';
                this.style.transition = '';
            }, 200);
        });
    });

    /* ── Double-tap copy link on mobile ── */
    let lastTap = 0;
    const playerSection = document.querySelector('.player-section');
    if (playerSection && 'ontouchstart' in window) {
        playerSection.addEventListener('touchend', (e) => {
            const now = Date.now();
            if (now - lastTap < 350 && now - lastTap > 50) {
                e.preventDefault();
                window.copyLink(e);
            }
            lastTap = now;
        });
    }

    /* ── Typewriter effect for file title on load ── */
    const titleEl = document.querySelector('.pinfo-title');
    if (titleEl) {
        const fullText = titleEl.textContent;
        titleEl.textContent = '';
        titleEl.style.borderRight = '2px solid var(--gold)';
        let charIdx = 0;
        const typeInterval = setInterval(() => {
            if (charIdx < fullText.length) {
                titleEl.textContent += fullText[charIdx];
                charIdx++;
            } else {
                clearInterval(typeInterval);
                setTimeout(() => { titleEl.style.borderRight = 'none'; }, 1500);
            }
        }, 30);
    }

    /* ── Smooth counter animation for stat values ── */
    const statObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const statVal = entry.target.querySelector('.stat-val');
                if (statVal) {
                    statVal.style.transform = 'scale(0.5)';
                    statVal.style.opacity = '0';
                    statVal.style.transition = 'all 0.6s var(--ease-spring)';
                    setTimeout(() => {
                        statVal.style.transform = 'scale(1)';
                        statVal.style.opacity = '1';
                    }, 100);
                }
                statObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.5 });
    document.querySelectorAll('.stat-cell').forEach(cell => statObserver.observe(cell));

    /* ── Auto-animate feature rows bounce in ── */
    const featObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const rows = entry.target.querySelectorAll('.feat-row');
                rows.forEach((row, i) => {
                    row.style.opacity = '0';
                    row.style.transform = 'translateX(-30px)';
                    row.style.transition = `all 0.5s ${i * 0.1}s var(--ease-spring)`;
                    setTimeout(() => {
                        row.style.opacity = '1';
                        row.style.transform = 'translateX(0)';
                    }, 50);
                });
                featObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.2 });
    document.querySelectorAll('.features').forEach(f => featObserver.observe(f));

    /* ── Heading text shimmer on hover ── */
    document.querySelectorAll('.s-heading').forEach(heading => {
        heading.addEventListener('mouseenter', function() {
            this.style.color = 'var(--gold-light)';
            this.style.transition = 'color 0.3s ease';
            this.querySelector('.s-heading-line').style.background = 'linear-gradient(90deg, var(--gold), transparent)';
        });
        heading.addEventListener('mouseleave', function() {
            this.style.color = '';
            this.querySelector('.s-heading-line').style.background = '';
        });
    });

    /* ── Mobile: Vibration feedback on button tap ── */
    if ('vibrate' in navigator) {
        document.querySelectorAll('.btn-base, .ext-btn, .mb-btn, .tg-cta').forEach(btn => {
            btn.addEventListener('touchstart', () => navigator.vibrate(10), { passive: true });
        });
    }

    /* ── Smooth reveal for section dividers ── */
    const dividerObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.style.transform = 'scaleX(1)';
                entry.target.style.opacity = '1';
                dividerObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.5 });
    document.querySelectorAll('.section-divider').forEach(d => {
        d.style.transform = 'scaleX(0)';
        d.style.opacity = '0';
        d.style.transition = 'transform 0.8s var(--ease-dramatic), opacity 0.4s ease';
        dividerObserver.observe(d);
    });

    /* ── Mobile bottom bar appear animation ── */
    const mobileBar = document.querySelector('.mobile-bar');

    /* ── Idle gold dust effect (desktop only) ── */
    if (window.matchMedia('(hover: hover)').matches) {
        document.addEventListener('mousemove', (e) => {
            if (Math.random() > 0.92) {
                const dust = document.createElement('div');
                dust.style.cssText = `
                    position: fixed;
                    left: ${e.clientX}px;
                    top: ${e.clientY}px;
                    width: ${1 + Math.random() * 3}px;
                    height: ${1 + Math.random() * 3}px;
                    background: var(--gold);
                    border-radius: 50%;
                    pointer-events: none;
                    z-index: 9998;
                    opacity: 0.7;
                    transition: all ${0.5 + Math.random() * 0.8}s ease-out;
                `;
                document.body.appendChild(dust);
                requestAnimationFrame(() => {
                    dust.style.opacity = '0';
                    dust.style.transform = `translate(${(Math.random()-0.5)*40}px, ${-20-Math.random()*30}px) scale(0)`;
                });
                setTimeout(() => dust.remove(), 1500);
            }
        }, { passive: true });
    }

    /* ── Custom cursor (desktop only) ── */
    const cursorDot = document.getElementById('cursorDot');
    const cursorRing = document.getElementById('cursorRing');
    if (cursorDot && cursorRing && window.matchMedia('(hover: hover)').matches) {
        let cx = 0, cy = 0, rx = 0, ry = 0;
        document.addEventListener('mousemove', (e) => {
            cx = e.clientX;
            cy = e.clientY;
            cursorDot.style.left = cx + 'px';
            cursorDot.style.top = cy + 'px';
            if (!cursorDot.classList.contains('visible')) {
                cursorDot.classList.add('visible');
                cursorRing.classList.add('visible');
            }
        }, { passive: true });

        // Smooth ring follow
        function ringLoop() {
            rx += (cx - rx) * 0.15;
            ry += (cy - ry) * 0.15;
            cursorRing.style.left = rx + 'px';
            cursorRing.style.top = ry + 'px';
            requestAnimationFrame(ringLoop);
        }
        ringLoop();

        // Hover detection on interactive elements
        const interactives = 'a, button, .ext-btn, .btn-base, .mb-btn, .tg-cta, .ctag, .badge, .feat-row, .info-block, .stat-cell';
        document.querySelectorAll(interactives).forEach(el => {
            el.addEventListener('mouseenter', () => {
                cursorDot.classList.add('hovering');
                cursorRing.classList.add('hovering');
            });
            el.addEventListener('mouseleave', () => {
                cursorDot.classList.remove('hovering');
                cursorRing.classList.remove('hovering');
            });
        });

        // Click effect
        document.addEventListener('mousedown', () => {
            cursorDot.classList.add('clicking');
            cursorRing.classList.add('clicking');
        });
        document.addEventListener('mouseup', () => {
            cursorDot.classList.remove('clicking');
            cursorRing.classList.remove('clicking');
        });

        // Hide when leaving window
        document.addEventListener('mouseleave', () => {
            cursorDot.classList.remove('visible');
            cursorRing.classList.remove('visible');
        });
        document.addEventListener('mouseenter', () => {
            cursorDot.classList.add('visible');
            cursorRing.classList.add('visible');
        });
    }

    /* ── 3D tilt on cards (desktop) ── */
    if (window.matchMedia('(hover: hover)').matches) {
        document.querySelectorAll('.s-card').forEach(card => {
            card.addEventListener('mousemove', (e) => {
                const rect = card.getBoundingClientRect();
                const x = (e.clientX - rect.left) / rect.width;
                const y = (e.clientY - rect.top) / rect.height;
                const tiltX = (y - 0.5) * -5;
                const tiltY = (x - 0.5) * 5;
                card.style.transform = `perspective(1000px) rotateX(${tiltX}deg) rotateY(${tiltY}deg) translateY(-2px)`;
            });
            card.addEventListener('mouseleave', () => {
                card.style.transform = '';
                card.style.transition = 'transform 0.5s var(--ease)';
                setTimeout(() => card.style.transition = '', 500);
            });
        });
    }

    /* ── Film sprocket holes ── */
    ['filmHoles1', 'filmHoles2'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            for (let i = 0; i < 8; i++) {
                const hole = document.createElement('div');
                hole.className = 'film-hole';
                el.appendChild(hole);
            }
        }
    });

    /* ── Time greeting ── */
    const greetEl = document.getElementById('greetText');
    if (greetEl) {
        const hour = new Date().getHours();
        let greeting = 'Good Evening';
        let icon = 'moon';
        if (hour >= 5 && hour < 12) { greeting = 'Good Morning'; icon = 'sun'; }
        else if (hour >= 12 && hour < 17) { greeting = 'Good Afternoon'; icon = 'cloud-sun'; }
        else if (hour >= 17 && hour < 21) { greeting = 'Good Evening'; icon = 'sunset'; }
        greetEl.textContent = greeting;
        const greetIcon = document.querySelector('.time-greeting i');
        if (greetIcon) greetIcon.className = `fas fa-${icon}`;
    }

    /* ── Back to top button click ── */
    if (backTopBtn) {
        backTopBtn.addEventListener('click', () => {
            window.scrollTo({ top: 0, behavior: 'smooth' });
        });
    }

    /* ── Network status indicator ── */
    const netStatus = document.getElementById('netStatus');
    const netText = document.getElementById('netText');
    function updateNetStatus(online) {
        if (!netStatus) return;
        netStatus.classList.remove('online', 'offline');
        netStatus.classList.add(online ? 'online' : 'offline');
        netText.textContent = online ? 'Online' : 'Offline';
        netStatus.classList.add('visible');
        if (online) {
            setTimeout(() => netStatus.classList.remove('visible'), 3000);
        }
    }
    window.addEventListener('online', () => updateNetStatus(true));
    window.addEventListener('offline', () => updateNetStatus(false));

    /* ── Keyboard shortcuts ── */
    let kbdVisible = false;
    document.addEventListener('keydown', (e) => {
        // Don't interfere with existing anti-inspect handler
        if (e.key === 'F12' || e.ctrlKey || e.shiftKey || e.altKey) return;

        switch(e.key.toLowerCase()) {
            case ' ':
                e.preventDefault();
                const vid = document.querySelector('video');
                if (vid) vid.paused ? vid.play() : vid.pause();
                break;
            case 'c':
                window.copyLink(e);
                break;
            case 'd':
                window.openModal(e);
                break;
            case '?':
                kbdVisible = !kbdVisible;
                const hints = document.getElementById('kbdHints');
                if (hints) hints.classList.toggle('visible', kbdVisible);
                break;
            case 'escape':
                const modal = document.getElementById('downloadModal');
                if (modal) modal.classList.remove('active');
                break;
        }
    });

    /* ── Confetti burst on Download Now click ── */
    function spawnConfetti(x, y) {
        const colors = ['#d4a853','#e8c97a','#c62828','#ff6b35','#00897b','#fff8ef'];
        for (let i = 0; i < 30; i++) {
            const c = document.createElement('div');
            c.className = 'confetti';
            const size = 4 + Math.random() * 6;
            const angle = (Math.PI * 2 * i) / 30;
            const velocity = 80 + Math.random() * 120;
            const destX = Math.cos(angle) * velocity;
            const destY = Math.sin(angle) * velocity - 40;
            c.style.cssText = `
                left: ${x}px;
                top: ${y}px;
                width: ${size}px;
                height: ${size * (0.4 + Math.random() * 0.6)}px;
                background: ${colors[Math.floor(Math.random() * colors.length)]};
                border-radius: ${Math.random() > 0.5 ? '50%' : '2px'};
                transition: all ${0.5 + Math.random() * 0.5}s cubic-bezier(0.25, 0.46, 0.45, 0.94);
                opacity: 1;
            `;
            document.body.appendChild(c);
            requestAnimationFrame(() => {
                c.style.transform = `translate(${destX}px, ${destY}px) rotate(${Math.random()*720}deg)`;
                c.style.opacity = '0';
            });
            setTimeout(() => c.remove(), 1200);
        }
    }
    document.querySelectorAll('.btn-hero').forEach(btn => {
        btn.addEventListener('click', (e) => {
            spawnConfetti(e.clientX, e.clientY);
        });
    });

    /* ── Scroll velocity: faster scroll = more particle glow (desktop only) ── */
    if (!isMobile) {
        let prevScrollY2 = window.scrollY;
        window.addEventListener('scroll', () => {
            const velocity = Math.abs(window.scrollY - prevScrollY2);
            prevScrollY2 = window.scrollY;
            const spots = document.querySelector('.spotlights');
            if (spots) {
                spots.style.opacity = 1 + Math.min(velocity / 50, 1) * 0.5;
                spots.style.transition = 'opacity 0.3s ease-out';
            }
        }, { passive: true });
    }

    /* ── Page visibility: pause/resume animations ── */
    document.addEventListener('visibilitychange', () => {
        const particles = document.getElementById('particleContainer');
        if (particles) {
            particles.style.animationPlayState = document.hidden ? 'paused' : 'running';
            particles.querySelectorAll('.particle').forEach(p => {
                p.style.animationPlayState = document.hidden ? 'paused' : 'running';
            });
        }
    });

    /* ── Player section tilt on mousemove (subtle) ── */
    const playerSec = document.getElementById('playerSection');
    if (playerSec && window.matchMedia('(hover: hover)').matches) {
        playerSec.addEventListener('mousemove', (e) => {
            const rect = playerSec.getBoundingClientRect();
            const x = (e.clientX - rect.left) / rect.width - 0.5;
            const y = (e.clientY - rect.top) / rect.height - 0.5;
            playerSec.style.transform = `perspective(1200px) rotateX(${y * -2}deg) rotateY(${x * 2}deg)`;
        });
        playerSec.addEventListener('mouseleave', () => {
            playerSec.style.transform = '';
            playerSec.style.transition = 'transform 0.6s var(--ease)';
            setTimeout(() => playerSec.style.transition = '', 600);
        });
    }

    /* ── Auto-hide mobile bar on scroll down, show on scroll up ── */
    if (isMobile && mobileBar) {
        let lastMobileScroll = 0;
        let mobileBarHidden = false;
        mobileBar.style.transition = 'transform 0.3s var(--ease)';
        window.addEventListener('scroll', () => {
            const st = window.scrollY;
            const shouldHide = st > lastMobileScroll && st > 200;
            if (shouldHide !== mobileBarHidden) {
                mobileBar.style.transform = shouldHide ? 'translateY(100%)' : 'translateY(0)';
                mobileBarHidden = shouldHide;
            }
            lastMobileScroll = st;
        }, { passive: true });
    }

    /* ── Footer year auto-update ── */
    const ftCopy = document.querySelector('.ft-copy');
    if (ftCopy) {
        ftCopy.innerHTML = `&copy; ${new Date().getFullYear()} ACKERSTREAMX&deg;`;
    }

    /* ── Easter egg: Click brand 5 times → gold rain ── */
    let brandClicks = 0;
    const brandEl = document.querySelector('.brand');
    if (brandEl) {
        brandEl.addEventListener('click', (e) => {
            e.preventDefault();
            brandClicks++;
            if (brandClicks >= 5) {
                brandClicks = 0;
                // Gold rain celebration
                for (let i = 0; i < 50; i++) {
                    setTimeout(() => {
                        const rain = document.createElement('div');
                        rain.style.cssText = `
                            position: fixed;
                            left: ${Math.random() * 100}vw;
                            top: -10px;
                            width: ${2 + Math.random() * 4}px;
                            height: ${10 + Math.random() * 20}px;
                            background: linear-gradient(to bottom, var(--gold), transparent);
                            pointer-events: none;
                            z-index: 9999;
                            opacity: ${0.3 + Math.random() * 0.7};
                            transition: transform ${1 + Math.random() * 2}s linear, opacity ${1 + Math.random()}s ease-out;
                        `;
                        document.body.appendChild(rain);
                        requestAnimationFrame(() => {
                            rain.style.transform = `translateY(${window.innerHeight + 30}px)`;
                            rain.style.opacity = '0';
                        });
                        setTimeout(() => rain.remove(), 3500);
                    }, i * 40);
                }
                window.showToast('✨ Golden Cinema Mode Activated!');
            }
        });
    }

    /* ── Smooth parallax for cards on scroll ── */
    if (window.matchMedia('(hover: hover)').matches) {
        const cards = document.querySelectorAll('.s-card');
        window.addEventListener('scroll', () => {
            cards.forEach((card, i) => {
                const rect = card.getBoundingClientRect();
                const visible = rect.top < window.innerHeight && rect.bottom > 0;
                if (visible) {
                    const progress = (window.innerHeight - rect.top) / (window.innerHeight + rect.height);
                    const yOffset = (progress - 0.5) * -8;
                    if (!card.matches(':hover')) {
                        card.style.transform = `translateY(${yOffset}px)`;
                    }
                }
            });
        }, { passive: true });
    }

})();
</script>

</body>
</html>

"""


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
        host_header = request.headers.get("X-Forwarded-Host", request.headers.get("Host", ""))
        proto_header = request.headers.get("X-Forwarded-Proto", request.scheme)

        if host_header:
            clean_host = re.sub(r':\d+$', '', host_header.strip())
            cloud_domains = [".koyeb.app", ".herokuapp.com", ".render.com", ".onrender.com", ".railway.app"]
            if any(cd in clean_host.lower() for cd in cloud_domains) or Var.HAS_SSL:
                proto_header = "https"
            base_url = f"{proto_header}://{clean_host}"
        else:
            base_url = Var.URL.rstrip('/')

        base_url = re.sub(r'(https?://[^/:]+):\d+', r'\1', base_url)
        stream_src = f"{base_url}/{message_id}{name_part}?hash={secure_hash}"

        file_id, _, _ = await _resolve_file_context(message_id, secure_hash, request.remote)
        file_size_text = _human_readable_size(getattr(file_id, "file_size", 0))

        display_name = unquote_plus(raw_name) if raw_name else None
        file_name_for_display = display_name or utils.get_name(file_id)

        page_html = WATCH_PAGE_TEMPLATE
        page_html = page_html.replace("__FILE_NAME_HTML__", html.escape(file_name_for_display))
        page_html = page_html.replace("__FILE_NAME_JS__", json.dumps(file_name_for_display))
        page_html = page_html.replace("__FILE_URL_HTML__", html.escape(stream_src, quote=True))
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
        try:
            # Handle "bytes=START-END" — take only the first range if multiple
            # ranges are given (multi-range requests are rare for streaming).
            ranges_part = range_header.replace("bytes=", "").split(",")[0].strip()
            start_str, end_str = ranges_part.split("-", 1)
            from_bytes = int(start_str) if start_str else 0
            until_bytes = int(end_str) if end_str else file_size - 1
        except (ValueError, AttributeError):
            from_bytes = 0
            until_bytes = file_size - 1
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

    # Build a weak ETag from message_id + requested range so browsers/players
    # can validate cached segments and avoid unnecessary re-downloads.
    etag = f'W/"{message_id}-{from_bytes}-{until_bytes}"'

    headers = {
        "Content-Type": mime_type,
        "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
        "Content-Length": str(req_length),
        "Content-Disposition": f'{disposition}; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        # Allow players/browsers to cache segments for 1 hour; avoids
        # re-fetching the same byte range when seeking back.
        "Cache-Control": "public, max-age=3600",
        "ETag": etag,
        # Keep the TCP connection alive between range requests so the player
        # doesn't incur a new TLS handshake for every 1 MB chunk.
        "Connection": "keep-alive",
        "X-Content-Type-Options": "nosniff",
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
