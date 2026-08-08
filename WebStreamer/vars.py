import sys
import re
from os import environ
from dotenv import load_dotenv

load_dotenv()


def _build_url():
    port = int(environ.get("PORT", "8080") if environ.get("PORT") else "8080")
    bind_addr = str(environ.get("WEB_SERVER_BIND_ADDRESS", "0.0.0.0"))
    has_ssl = str(environ.get("HAS_SSL", "0").lower()) in ("1", "true", "t", "yes", "y")
    no_port = str(environ.get("NO_PORT", "1").lower()) in ("1", "true", "t", "yes", "y")
    fqdn = str(environ.get("FQDN", bind_addr)).strip()
    raw_url = str(environ.get("URL", "")).strip()

    cloud_domains = [".koyeb.app", ".herokuapp.com", ".render.com", ".onrender.com", ".railway.app"]

    if raw_url:
        base = raw_url
    else:
        is_cloud = any(cd in fqdn.lower() for cd in cloud_domains)
        use_ssl = has_ssl or is_cloud
        scheme = "https" if use_ssl else "http"
        base = f"{scheme}://{fqdn}"

    if not base.startswith("http://") and not base.startswith("https://"):
        is_cloud = any(cd in base.lower() for cd in cloud_domains)
        base = ("https://" if (has_ssl or is_cloud) else "http://") + base

    is_cloud_app = any(cd in base.lower() for cd in cloud_domains)
    if is_cloud_app and base.startswith("http://"):
        base = "https://" + base[7:]

    if no_port or is_cloud_app or port in (80, 443):
        base = re.sub(r'(https?://[^/:]+):\d+', r'\1', base)

    if not base.endswith("/"):
        base += "/"

    return base


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
    FQDN = str(environ.get("FQDN", BIND_ADDRESS)).strip()
    URL = _build_url()
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
