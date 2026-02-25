# File Stream & Download Bot

Version: 2.1.0 [Stable]
Creator: Acckerman
Powered by: Telegram Bot API | Python | Stream Engine

---

## About
This bot converts Telegram files into streamable links and direct download URLs with minimal wait and no ads.

---

## Features
- Instant streaming in the browser
- Direct, resumable download links
- Cross-platform support
- Unique access links for privacy
- Optimized backend for speed and uptime
- Handles files up to 4 GB (Telegram API limit)

---

## New in v2.1.0 [Stable]
- Enhanced stream page UI
- Faster link generation engine
- Improved security and privacy
- Multi-audio and subtitle support (beta)
- Smart link expiry controls

---

## Credits
Developed and maintained by: [Acckerman](https://github.com/Acckerman2)
"Stream smarter - share faster."

---

## Tech Stack
- Language: Python
- Frameworks: Pyrogram / Telethon
- Frontend: HTML, CSS, JavaScript (Plyr.js + hls.js)
- Hosting: Render / Koyeb / VPS supported

---

### Support the Project
If you like this bot, give it a star on GitHub and share it.

---

## Admin Configuration
Set an admin user (Telegram numeric ID) who can run sensitive commands.

Environment variable:
```
ADMIN=123456789   # replace with your Telegram user id
```

Commands restricted to this admin:
- /ban
- /unban
- /broadcast

If ADMIN is not set or invalid, these commands are inaccessible to everyone. The legacy ALLOWED_USERS list still applies elsewhere, but ADMIN takes precedence for these commands.

Find your numeric Telegram ID via any "Get My ID" bot or by logging updates while developing.

---

## Run Locally (no Docker)
1. Install Python 3.10+.
2. Install dependencies:
```
pip install -r requirements.txt
```
3. Export environment variables (API_ID, API_HASH, BOT_TOKEN, BIN_CHANNEL, etc.).
4. Start the bot:
```
python3 -m WebStreamer
```
