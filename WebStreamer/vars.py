import sys
from os import environ
from dotenv import load_dotenv

load_dotenv()


class Var(object):
    MULTI_CLIENT = True  # Always enabled like filter2
    API_ID = int(environ.get("API_ID","24865057"))
    API_HASH = str(environ.get("API_HASH","a45372ac22649e134c7871ab3125bfb1"))
    BOT_TOKEN = str(environ.get("BOT_TOKEN"))
    SLEEP_THRESHOLD = int(environ.get("SLEEP_THRESHOLD", "60"))  # 1 minute
    WORKERS = int(environ.get("WORKERS", "150"))  # 150 workers like filter2
    STREAM_CHUNK_SIZE = int(environ.get("STREAM_CHUNK_SIZE", str(1024 * 1024)))  # 1MB - Telegram's max limit
    BIN_CHANNEL = int(
        environ.get("BIN_CHANNEL", "-1001788767433")
    )  # you NEED to use a CHANNEL when you're using MULTI_CLIENT
    PORT = int(environ.get("PORT"))
    BIND_ADDRESS = str(environ.get("WEB_SERVER_BIND_ADDRESS", "0.0.0.0"))
    PING_INTERVAL = int(environ.get("PING_INTERVAL", "1200"))  # 20 minutes
    HAS_SSL = str(environ.get("HAS_SSL", "0").lower()) in ("1", "true", "t", "yes", "y")
    NO_PORT = str(environ.get("NO_PORT", "1").lower()) in ("1", "true", "t", "yes", "y")
    HASH_LENGTH = int(environ.get("HASH_LENGTH", 6))
    if not 5 < HASH_LENGTH < 64:
        sys.exit("Hash length should be greater than 5 and less than 64")
    FQDN = str(environ.get("FQDN", BIND_ADDRESS))
    _clean_fqdn = FQDN.split(":")[0] if (NO_PORT or PORT in (80, 443)) and ":" in FQDN else FQDN
    _port_str = "" if (NO_PORT or PORT in (80, 443)) else f":{PORT}"
    URL = f"http{'s' if HAS_SSL else ''}://{_clean_fqdn}{_port_str}/"
    KEEP_ALIVE = str(environ.get("KEEP_ALIVE", "1").lower()) in  ("1", "true", "t", "yes", "y")
    DEBUG = str(environ.get("DEBUG", "1").lower()) in ("1", "true", "t", "yes", "y")
    USE_SESSION_FILE = str(environ.get("USE_SESSION_FILE", "0").lower()) in ("1", "true", "t", "yes", "y")
    ALLOWED_USERS = [x.strip("@ ") for x in str(environ.get("ALLOWED_USERS", "") or "").split(",") if x.strip("@ ")]
    # ADMIN: single Telegram user id who can run sensitive admin commands (/ban, /unban, /broadcast)
    # Set via environment variable ADMIN. Example: ADMIN=123456789
    try:
        ADMIN = int(str(environ.get("ADMIN", "0")).strip())
    except Exception:
        ADMIN = 0
    
    # MongoDB Database Configuration
    DATABASE_URL = str(environ.get("DATABASE_URL", "mongodb+srv://file2stream:acckerman@cluster0.cp947zc.mongodb.net/?appName=Cluster0"))
    DATABASE_NAME = str(environ.get("DATABASE_NAME", "WebStreamer"))
