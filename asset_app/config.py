"""Configuration helpers for future route/service modules.

The legacy app still reads environment variables directly for compatibility.
New code should import these helpers instead of duplicating env parsing.
"""

import os
from dataclasses import dataclass
from pathlib import Path


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class AppSettings:
    app_name: str = os.getenv("APP_NAME", "资产智能管控台")
    db_path: str = os.getenv("DB_PATH", "asset_management.db")
    backup_dir: Path = Path(os.getenv("BACKUP_DIR", "export_backups"))
    cookie_secure: bool = env_bool("COOKIE_SECURE", False)
    registration_enabled: bool = env_bool("REGISTRATION_ENABLED", True)
    auto_backup_enabled: bool = env_bool("AUTO_BACKUP_ENABLED", True)
    auto_backup_interval_hours: int = env_int("AUTO_BACKUP_INTERVAL_HOURS", 24)
    auto_backup_retention_count: int = env_int("AUTO_BACKUP_RETENTION_COUNT", 7)
    sync_retry_enabled: bool = env_bool("SYNC_RETRY_ENABLED", True)
    sync_retry_max_attempts: int = env_int("SYNC_RETRY_MAX_ATTEMPTS", 3)
    sync_retry_delay_seconds: int = env_int("SYNC_RETRY_DELAY_SECONDS", 300)


settings = AppSettings()
