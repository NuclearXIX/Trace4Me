# utils/ftp_config.py
import os

# Load FTP credentials from environment variables
FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT", 21))  # default port 21
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")

# Ensure all variables are set
if not all([FTP_HOST, FTP_USER, FTP_PASS]):
    raise Exception("FTP credentials not found. Make sure FTP_HOST, FTP_USER, and FTP_PASS are set as environment variables.")
    
from pathlib import Path
from typing import Optional, Any, Dict

FTP_STORE = "data/ftp_config.json"


def _load() -> dict:
    p = Path(FTP_STORE)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(obj: dict):
    p = Path(FTP_STORE)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def set_ftp_config(
    guild_id: int,
    host: str,
    username: str,
    password: str,
    port: int = 21,
    adm_dir: str = "/",
    interval_sec: int = 10,
    **extras: Any,
):
    """
    Save per-guild FTP settings and (optionally) extra keys such as:
      - nitrado_api_token
      - nitrado_service_id
      - nitrado_log_folder_prefix

    This function MERGES with any existing record to avoid wiping previously
    stored extras when they are not provided in a subsequent call.
    """
    data = _load()
    key = str(guild_id)

    # Start from existing entry (merge behavior)
    current: Dict[str, Any] = dict(data.get(key, {}))

    base = {
        "host": host,
        "port": int(port),
        "username": username,
        "password": password,
        "adm_dir": adm_dir,
        "interval_sec": int(interval_sec),
    }

    # Only persist non-empty extras
    for k, v in (extras or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        base[k] = v

    # Merge and save
    current.update(base)
    data[key] = current
    _save(data)


def get_ftp_config(guild_id: int) -> Optional[dict]:
    return _load().get(str(guild_id))


def clear_ftp_config(guild_id: int):
    data = _load()
    if str(guild_id) in data:
        del data[str(guild_id)]
        _save(data)
