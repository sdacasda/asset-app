import concurrent.futures
import csv
import hashlib
import hmac
import io
import ipaddress
import math
import os
import random
import re
import secrets
import shutil
import socket
import sqlite3
import tarfile
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from html import escape as html_escape
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
import urllib3
from bs4 import BeautifulSoup
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, FileResponse
from pydantic import BaseModel


DB_PATH = os.getenv("DB_PATH", "asset_management.db")
APP_NAME = os.getenv("APP_NAME", "资产智能管控台")
SESSION_COOKIE_NAME = "asset_session"
SESSION_DAYS = int(os.getenv("SESSION_DAYS", "7"))
PBKDF2_ITERATIONS = int(os.getenv("PBKDF2_ITERATIONS", "260000"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "2000"))
SCRAPE_WORKERS = int(os.getenv("SCRAPE_WORKERS", "1"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() in ("1", "true", "yes", "on")
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "false").lower() in ("1", "true", "yes", "on")
REGISTRATION_ENABLED = os.getenv("REGISTRATION_ENABLED", "true").lower() in ("1", "true", "yes", "on")
ALLOW_PRIVATE_SOURCE = os.getenv("ALLOW_PRIVATE_SOURCE", "false").lower() in ("1", "true", "yes", "on")
ALLOWED_SOURCE_HOSTS = [x.strip().lower() for x in os.getenv("ALLOWED_SOURCE_HOSTS", "").split(",") if x.strip()]
BACKUP_DIR = Path(os.getenv("BACKUP_DIR", "export_backups"))
MAX_BACKUP_UPLOAD_BYTES = int(os.getenv("MAX_BACKUP_UPLOAD_MB", "50")) * 1024 * 1024
AUTO_BACKUP_ENABLED = os.getenv("AUTO_BACKUP_ENABLED", "true").lower() in ("1", "true", "yes", "on")
AUTO_BACKUP_INTERVAL_HOURS = int(os.getenv("AUTO_BACKUP_INTERVAL_HOURS", "24"))
AUTO_BACKUP_RETENTION_COUNT = int(os.getenv("AUTO_BACKUP_RETENTION_COUNT", "7"))
SYNC_RETRY_ENABLED = os.getenv("SYNC_RETRY_ENABLED", "true").lower() in ("1", "true", "yes", "on")
SYNC_RETRY_MAX_ATTEMPTS = int(os.getenv("SYNC_RETRY_MAX_ATTEMPTS", "3"))
SYNC_RETRY_DELAY_SECONDS = int(os.getenv("SYNC_RETRY_DELAY_SECONDS", "300"))

# 旧网址归属别名。用于把历史资产继续归到原来的“标头｜标签”下面，避免换网址后历史记录变成未知分类。
LEGACY_SOURCE_URL_ALIASES = {
    "https://shengshi888.cc/": "盛世",
    "http://wansheng888.cc/": "万盛",
    "https://nbcz.top/": "嘉盛",
}

LOGIN_LOCK = threading.Lock()
LOGIN_ATTEMPTS: Dict[str, List[float]] = {}
MAX_LOGIN_ATTEMPTS = int(os.getenv("MAX_LOGIN_ATTEMPTS", "8"))
LOGIN_WINDOW_SECONDS = int(os.getenv("LOGIN_WINDOW_SECONDS", "900"))

if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def bj_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=8)))


def now_text() -> str:
    return bj_now().strftime("%Y-%m-%d %H:%M:%S")


APP_LOGS: List[Dict[str, str]] = []
MAX_APP_LOGS = 300

TASK_LOCK = threading.Lock()
SYNC_TASKS: Dict[str, Dict[str, object]] = {}
SCHEDULER_STOP = threading.Event()
SCHEDULER_THREAD = None


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def add_log(message: str, level: str = "info") -> None:
    item = {"time": now_text(), "level": level, "message": str(message)}
    APP_LOGS.append(item)
    if len(APP_LOGS) > MAX_APP_LOGS:
        del APP_LOGS[:-MAX_APP_LOGS]

    print(f"[{item['time']}] [{level}] {message}")

    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute(
            "INSERT INTO app_logs(level, message, created_at) VALUES(?, ?, ?)",
            (level, str(message), item["time"]),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row["name"] for row in rows]


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    cols = table_columns(conn, table)
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def normalize_source_url_for_alias(url: str) -> str:
    return str(url or "").strip().lower().rstrip("/")


def source_group_from_name(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    for sep in ("|", "｜"):
        if sep in raw:
            return raw.split(sep, 1)[0].strip()
    return raw.strip()


def seed_source_url_aliases(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_url_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT NOT NULL UNIQUE,
            source_group TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    for url, group in LEGACY_SOURCE_URL_ALIASES.items():
        conn.execute(
            "INSERT OR IGNORE INTO source_url_aliases(source_url, source_group, created_at) VALUES(?, ?, ?)",
            (normalize_source_url_for_alias(url), group, now_text()),
        )


def remember_source_url_alias(conn: sqlite3.Connection, source_url: str, source_name: str) -> None:
    group = source_group_from_name(source_name)
    url = normalize_source_url_for_alias(source_url)
    if not url or not group:
        return
    conn.execute(
        "INSERT OR IGNORE INTO source_url_aliases(source_url, source_group, created_at) VALUES(?, ?, ?)",
        (url, group, now_text()),
    )


def repair_asset_source_bindings(conn: sqlite3.Connection) -> None:
    # 1) 先按当前数据源的网址 + 关键词精确绑定。
    conn.execute("""
        UPDATE asset_records
        SET source_id = (
            SELECT source_configs.id
            FROM source_configs
            WHERE source_configs.source_url = asset_records.source_url
              AND source_configs.default_keyword = asset_records.keyword
            ORDER BY source_configs.updated_at DESC, source_configs.id DESC
            LIMIT 1
        )
        WHERE source_id IS NULL
          AND EXISTS (
              SELECT 1
              FROM source_configs
              WHERE source_configs.source_url = asset_records.source_url
                AND source_configs.default_keyword = asset_records.keyword
          )
    """)

    # 2) 如果一个网址只对应一个数据源，也可以安全绑定。
    conn.execute("""
        UPDATE asset_records
        SET source_id = (
            SELECT source_configs.id
            FROM source_configs
            WHERE source_configs.source_url = asset_records.source_url
            ORDER BY source_configs.updated_at DESC, source_configs.id DESC
            LIMIT 1
        )
        WHERE source_id IS NULL
          AND (
              SELECT COUNT(1)
              FROM source_configs
              WHERE source_configs.source_url = asset_records.source_url
          ) = 1
    """)

    # 3) 旧网址别名绑定：例如 shengshi888.cc 仍归“盛世”，再按关键词匹配到“盛世｜龙湖街道”。
    conn.execute("""
        UPDATE asset_records
        SET source_id = (
            SELECT sc.id
            FROM source_url_aliases al
            JOIN source_configs sc
              ON (sc.name = al.source_group OR sc.name LIKE al.source_group || '|%' OR sc.name LIKE al.source_group || '｜%')
             AND COALESCE(sc.default_keyword, '') = COALESCE(asset_records.keyword, '')
            WHERE al.source_url = lower(rtrim(asset_records.source_url, '/'))
            ORDER BY sc.updated_at DESC, sc.id DESC
            LIMIT 1
        )
        WHERE source_id IS NULL
          AND EXISTS (
            SELECT 1
            FROM source_url_aliases al
            JOIN source_configs sc
              ON (sc.name = al.source_group OR sc.name LIKE al.source_group || '|%' OR sc.name LIKE al.source_group || '｜%')
             AND COALESCE(sc.default_keyword, '') = COALESCE(asset_records.keyword, '')
            WHERE al.source_url = lower(rtrim(asset_records.source_url, '/'))
          )
    """)




def asset_status_priority_sql(expr: str = "status_type", timer_expr: str = "status_timer") -> str:
    """SQL CASE expression: risk/danger has highest priority, then safe, then unknown."""
    return f"""
        CASE
            WHEN COALESCE({expr}, '') NOT IN ('', 'safe', 'unknown') THEN 3
            WHEN COALESCE({expr}, '') = 'safe' THEN 2
            WHEN COALESCE({timer_expr}, '') LIKE '%已过期%' THEN 2
            WHEN COALESCE({timer_expr}, '') LIKE '%释放%' THEN 2
            WHEN COALESCE({timer_expr}, '') LIKE '%纯净可用%' THEN 2
            ELSE 1
        END
    """


def merge_duplicate_asset_records(conn: sqlite3.Connection) -> None:
    """Merge duplicate active assets under the same source label.

    Rule: same source_id + same address should appear only once, even if it was
    previously scraped from different URLs. If duplicate statuses conflict,
    risk/danger wins over pure/safe; for the same priority, the newest record wins.
    Losers are soft-deleted so they can still be audited/recovered if needed.
    """
    # Make one more binding pass before merging so old URL aliases can be grouped.
    repair_asset_source_bindings(conn)
    rows = conn.execute(f"""
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY source_id, content_text
                    ORDER BY
                        {asset_status_priority_sql('status_type', 'status_timer')} DESC,
                        COALESCE(last_checked, '') DESC,
                        id DESC
                ) AS rn
            FROM asset_records
            WHERE deleted_at IS NULL
              AND source_id IS NOT NULL
              AND COALESCE(content_text, '') != ''
        )
        SELECT id FROM ranked WHERE rn > 1
    """).fetchall()
    loser_ids = [int(r[0]) for r in rows]
    if not loser_ids:
        return
    now = now_text()
    conn.executemany(
        """
        UPDATE asset_records
        SET deleted_at=COALESCE(deleted_at, ?),
            deleted_by=COALESCE(deleted_by, 'system'),
            delete_reason=COALESCE(delete_reason, 'v17 自动合并重复资产：同一标签同一地址仅保留一条，风控优先于纯净')
        WHERE id=?
        """,
        [(now, rid) for rid in loser_ids],
    )
    add_log(f"自动合并重复资产：{len(loser_ids)} 条，同一标签同一地址保留风控优先记录", "info")


def dedupe_record_rows(rows) -> List[Dict[str, object]]:
    """Dedupe API/export rows with the same effective source + address.

    This is a display-level safety net. Database merge runs at startup, but this
    prevents duplicate display if new duplicates are created before the next init.
    """
    def priority(item: Dict[str, object]) -> int:
        stype = str(item.get('status_type') or '')
        timer = str(item.get('status_timer') or '')
        if stype and stype not in ('safe', 'unknown'):
            return 3
        if stype == 'safe' or ('已过期' in timer) or ('释放' in timer) or ('纯净可用' in timer):
            return 2
        return 1

    def score(item: Dict[str, object]):
        return (priority(item), str(item.get('last_checked') or ''), int(item.get('id') or 0))

    best = {}
    order = []
    for row in rows:
        item = dict(row)
        source_key = item.get('effective_source_id') or item.get('source_id') or f"url:{item.get('source_url') or ''}|kw:{item.get('keyword') or ''}"
        key = (source_key, item.get('content_text') or '')
        if key not in best:
            best[key] = item
            order.append(key)
        elif score(item) > score(best[key]):
            best[key] = item
    return [best[k] for k in order]

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_login TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_hash TEXT NOT NULL UNIQUE,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS asset_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT NOT NULL,
            keyword TEXT NOT NULL,
            content_text TEXT NOT NULL,
            status_timer TEXT,
            last_checked TEXT,
            status_type TEXT,
            remaining_hours INTEGER,
            raw_status TEXT,
            UNIQUE(source_url, content_text)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS source_configs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            source_url TEXT NOT NULL,
            default_keyword TEXT,
            default_proxy TEXT,
            request_cookie TEXT,
            schedule_enabled INTEGER NOT NULL DEFAULT 0,
            schedule_interval_minutes INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            last_used_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_url TEXT NOT NULL,
            keyword TEXT NOT NULL,
            used_proxy INTEGER NOT NULL DEFAULT 0,
            used_cookie INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            total_found INTEGER NOT NULL DEFAULT 0,
            inserted_count INTEGER NOT NULL DEFAULT 0,
            updated_count INTEGER NOT NULL DEFAULT 0,
            restored_from_available INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            error_message TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER,
            source_url TEXT NOT NULL,
            keyword TEXT NOT NULL,
            request_cookie TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            attempt INTEGER NOT NULL DEFAULT 1,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            scheduled INTEGER NOT NULL DEFAULT 1,
            next_run_at TEXT NOT NULL,
            task_id TEXT,
            run_id INTEGER,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT
        )
    """)

    ensure_column(conn, "asset_records", "source_id", "INTEGER")
    ensure_column(conn, "asset_records", "status_type", "TEXT")
    ensure_column(conn, "asset_records", "remaining_hours", "INTEGER")
    ensure_column(conn, "asset_records", "raw_status", "TEXT")
    ensure_column(conn, "asset_records", "expire_at", "TEXT")

    ensure_column(conn, "source_configs", "default_keyword", "TEXT")
    ensure_column(conn, "source_configs", "default_proxy", "TEXT")
    ensure_column(conn, "source_configs", "request_cookie", "TEXT")
    ensure_column(conn, "source_configs", "schedule_enabled", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "source_configs", "schedule_interval_minutes", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "source_configs", "enabled", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "source_configs", "sort_order", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "source_configs", "last_used_at", "TEXT")
    ensure_column(conn, "source_configs", "last_scheduled_at", "TEXT")
    ensure_column(conn, "source_configs", "last_success_at", "TEXT")
    ensure_column(conn, "source_configs", "last_failed_at", "TEXT")
    ensure_column(conn, "source_configs", "last_error", "TEXT")
    ensure_column(conn, "source_configs", "created_at", "TEXT")
    ensure_column(conn, "source_configs", "updated_at", "TEXT")
    ensure_column(conn, "sync_runs", "source_id", "INTEGER")
    ensure_column(conn, "sync_runs", "attempt", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "sync_runs", "scheduled", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "asset_records", "deleted_at", "TEXT")
    ensure_column(conn, "asset_records", "deleted_by", "TEXT")
    ensure_column(conn, "asset_records", "delete_reason", "TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # 让资产记录优先绑定到数据源 ID。这样以后数据源网址变更，历史资产仍按“标题/标签”归类。
    seed_source_url_aliases(conn)
    repair_asset_source_bindings(conn)
    merge_duplicate_asset_records(conn)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_source_id ON asset_records(source_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_deleted_at ON asset_records(deleted_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_source_keyword ON asset_records(source_url, keyword)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_last_checked ON asset_records(last_checked)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_status ON asset_records(status_timer)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_status_type ON asset_records(status_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_remaining_hours ON asset_records(remaining_hours)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_jobs_status_next ON sync_jobs(status, next_run_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_jobs_source ON sync_jobs(source_id, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_expire_at ON asset_records(expire_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sessions_token_hash ON sessions(token_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_source_configs_enabled ON source_configs(enabled, sort_order, updated_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_app_logs_created_at ON app_logs(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sync_runs_started_at ON sync_runs(started_at)")

    cur.execute("DELETE FROM sessions WHERE expires_at < ?", (now_text(),))
    reset_stale_sync_jobs(conn)
    conn.commit()
    conn.close()

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def make_password_hash(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), PBKDF2_ITERATIONS).hex()
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${salt}${digest}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algo, iterations, salt, digest = stored_hash.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), int(iterations)).hex()
        return hmac.compare_digest(candidate, digest)
    except Exception:
        return False


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def create_session(user_id: int) -> str:
    token = secrets.token_urlsafe(48)
    expires_at = (bj_now() + timedelta(days=SESSION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

    conn = get_conn()
    conn.execute(
        "INSERT INTO sessions(token_hash, user_id, created_at, expires_at) VALUES(?, ?, ?, ?)",
        (hash_token(token), user_id, now_text(), expires_at),
    )
    conn.commit()
    conn.close()
    return token


def set_auth_cookie(response: RedirectResponse, token: str) -> None:
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=SESSION_DAYS * 24 * 3600,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="lax",
        path="/",
    )


def get_current_user(request: Request) -> Optional[Dict[str, object]]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None

    conn = get_conn()
    row = conn.execute(
        """
        SELECT users.id, users.username
        FROM sessions
        JOIN users ON users.id = sessions.user_id
        WHERE sessions.token_hash = ? AND sessions.expires_at >= ?
        """,
        (hash_token(token), now_text()),
    ).fetchone()
    conn.close()

    if not row:
        return None

    return {"id": row["id"], "username": row["username"]}


def require_user(request: Request) -> Dict[str, object]:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="请先登录")
    return user


def validate_target_url(url: str) -> str:
    url = (url or "").strip()
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError("网址需以 http:// 或 https:// 开头")

    if not parsed.hostname:
        raise ValueError("网址格式不正确")

    if parsed.username or parsed.password:
        raise ValueError("数据源网址中不允许包含用户名或密码")

    host = parsed.hostname.lower().rstrip(".")

    if ALLOWED_SOURCE_HOSTS and not any(host == x or host.endswith("." + x) for x in ALLOWED_SOURCE_HOSTS):
        raise ValueError("该数据源域名不在允许访问范围内")

    if ALLOW_PRIVATE_SOURCE:
        return url

    try:
        addr_infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError("数据源域名解析失败")

    for info in addr_infos:
        ip = ipaddress.ip_address(info[4][0].split("%", 1)[0])
        if not ip.is_global:
            raise ValueError("禁止访问内网、回环、链路本地或非公网地址")

    return url


def clean_cookie(cookie: str) -> str:
    cookie = (cookie or "").strip()
    cookie = cookie.replace("\r", "").replace("\n", "").strip()
    if cookie.lower().startswith("cookie:"):
        cookie = cookie.split(":", 1)[1].strip()
    return cookie


def build_common_headers(source_url: str, request_cookie: str = "") -> Dict[str, str]:
    root = source_url.rstrip("/")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "max-age=0",
        "Origin": root,
        "Referer": root + "/",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
    }

    cookie = clean_cookie(request_cookie)
    if cookie:
        headers["Cookie"] = cookie

    return headers


def explain_request_exception(exc: Exception) -> str:
    text = str(exc)
    lower = text.lower()
    name = exc.__class__.__name__

    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return f"连接超时：{text}"
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return f"读取超时：{text}"
    if isinstance(exc, requests.exceptions.SSLError):
        return f"SSL 证书错误：{text}"
    if "connection refused" in lower:
        return f"连接被拒绝：{text}"
    if "name or service not known" in lower or "temporary failure in name resolution" in lower:
        return f"DNS 解析失败：{text}"
    if "host unreachable" in lower:
        return f"目标主机不可达：{text}"
    if "timed out" in lower or "timeout" in lower:
        return f"连接超时：{text}"
    if "connection reset" in lower:
        return f"连接被对方重置：{text}"
    if "remote end closed connection" in lower:
        return f"远端服务器主动关闭连接：{text}"

    return f"{name}: {text}"


def is_available_status(status_text: str) -> bool:
    status_text = status_text or ""
    return ("已过期" in status_text) or ("释放" in status_text) or ("纯净可用" in status_text) or ("空闲" in status_text) or ("可用" in status_text)


def normalize_status_fields(status_text: str) -> Tuple[str, Optional[int], str]:
    raw = status_text or "未知状态"
    text = raw.strip()

    if is_available_status(text):
        return "safe", -1, raw

    if "未知状态" in text or not text:
        return "unknown", None, raw

    minutes = parse_countdown_minutes(text)

    if minutes is not None:
        hours = max(1, math.ceil(minutes / 60))
        return "countdown", hours, raw

    return "countdown", None, raw


def parse_countdown_minutes(status_text: str) -> Optional[int]:
    text = status_text or ""

    if not text:
        return None

    if is_available_status(text):
        return None

    if "未知状态" in text:
        return None

    day_match = re.search(r"(\d+)\s*天", text)
    hour_match = re.search(r"(\d+)\s*(?:小时|时)", text)
    minute_match = re.search(r"(\d+)\s*(?:分钟|分)", text)

    if not day_match and not hour_match and not minute_match:
        return None

    total = 0

    if day_match:
        total += int(day_match.group(1)) * 24 * 60

    if hour_match:
        total += int(hour_match.group(1)) * 60

    if minute_match:
        total += int(minute_match.group(1))

    return total


def parse_db_time(value: str) -> Optional[datetime]:
    if not value:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:19], fmt).replace(tzinfo=timezone(timedelta(hours=8)))
        except Exception:
            pass

    return None


def calc_expire_at_text(status_text: str, checked_at: str) -> Optional[str]:
    minutes = parse_countdown_minutes(status_text)

    if minutes is None:
        return None

    base = parse_db_time(checked_at)

    if not base:
        return None

    return (base + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")


def auto_purify_expired_countdowns(reason: str = "auto") -> Dict[str, int]:
    try:
        conn = get_conn()

        rows = conn.execute("""
            SELECT id, status_timer, last_checked
            FROM asset_records
            WHERE last_checked IS NOT NULL
              AND COALESCE(status_type, '') != 'safe'
              AND COALESCE(status_timer, '') NOT LIKE '%已过期%'
              AND COALESCE(status_timer, '') NOT LIKE '%释放%'
              AND COALESCE(status_timer, '') NOT LIKE '%纯净可用%'
        """).fetchall()

        now_dt = bj_now()
        converted = 0
        filled_expire_at = 0

        for row in rows:
            rid = int(row["id"])
            status_timer = row["status_timer"] or ""
            last_checked = row["last_checked"] or ""

            if is_available_status(status_timer) or "未知状态" in status_timer:
                continue

            expire_at = calc_expire_at_text(status_timer, last_checked)

            if expire_at:
                conn.execute(
                    """
                    UPDATE asset_records
                    SET expire_at=?,
                        raw_status=COALESCE(raw_status, ?)
                    WHERE id=?
                    """,
                    (expire_at, status_timer, rid),
                )
                filled_expire_at += 1

            expire_dt = parse_db_time(expire_at)

            if expire_dt and expire_dt <= now_dt:
                conn.execute(
                    """
                    UPDATE asset_records
                    SET status_timer=?,
                        status_type=?,
                        remaining_hours=?,
                        raw_status=?,
                        expire_at=?,
                        last_checked=?
                    WHERE id=?
                    """,
                    (
                        "已过期(纯净可用)",
                        "safe",
                        -1,
                        status_timer,
                        None,
                        now_text(),
                        rid,
                    ),
                )
                converted += 1

        conn.commit()
        conn.close()

        if converted:
            add_log(f"状态自动校准完成：{converted} 条倒计时结束资产已转为纯净可用，触发来源={reason}", "info")

        return {
            "converted": converted,
            "filled_expire_at": filled_expire_at,
        }

    except Exception as exc:
        add_log(f"状态自动校准失败：{exc}", "error")
        return {
            "converted": 0,
            "filled_expire_at": 0,
        }


def fetch_page_with_retry(source_url: str, keyword: str, page: int, request_cookie: str = "") -> Optional[str]:
    source_url = validate_target_url(source_url)
    headers = build_common_headers(source_url, request_cookie)

    payload = {
        "page": str(page),
        "search_keyword": keyword,
    }

    for attempt in range(5):
        try:
            resp = requests.post(
                source_url,
                headers=headers,
                data=payload,
                timeout=REQUEST_TIMEOUT,
                verify=VERIFY_SSL,
                allow_redirects=False,
            )

            if resp.status_code in (301, 302, 303, 307, 308):
                add_log(f"页面 {page} 返回跳转，已拦截。Location={resp.headers.get('Location', '')}", "warn")
                return None

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "").strip()
                sleep_time = min(int(retry_after), 60) if retry_after.isdigit() else 2 * (2 ** attempt) + random.uniform(0.5, 1.5)
                add_log(f"页面 {page} 被限流，等待 {sleep_time:.1f} 秒后重试", "warn")
                time.sleep(sleep_time)
                continue

            if resp.status_code in (403, 456):
                add_log(f"节点在第 {page} 页连接受限，HTTP {resp.status_code}", "warn")
                return None

            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text

        except Exception as exc:
            message = explain_request_exception(exc)
            add_log(f"页面 {page} 第 {attempt + 1} 次抓取失败：{message}", "error")
            if attempt == 4:
                return None
            time.sleep(3)

    return None


def parse_structural_data(html_text: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []

    if not html_text:
        return results

    if "/_guard/auto.js" in html_text and len(html_text.strip()) < 200:
        add_log("当前页面只返回防护脚本 /_guard/auto.js，未获得真实搜索结果。请为该数据源填写浏览器 Cookie。", "warn")
        return results

    soup = BeautifulSoup(html_text, "html.parser")
    status_keywords = ["过期", "到期", "剩余", "释放", "可用", "空闲", "正常", "有效期", "有效至", "即将释放", "倒计时"]

    for node in soup.find_all(string=re.compile(r"省.*市")):
        clean_text = node.strip()

        if len(clean_text) < 10 or "无需添加" in clean_text:
            continue

        status_str = "未知状态"
        parent = node.parent

        for _ in range(6):
            if not parent:
                break

            parent_text = parent.get_text(separator="|", strip=True)

            if any(k in parent_text for k in status_keywords):
                for part in parent_text.split("|"):
                    part = part.strip()

                    if any(k in part for k in status_keywords):
                        if is_available_status(part):
                            status_str = "已过期(纯净可用)"
                        else:
                            status_str = part
                        break

                if status_str != "未知状态":
                    break

            parent = parent.parent

        results.append({"content": clean_text, "status": status_str})

    return results


def parse_text_fallback(html_text: str) -> List[Dict[str, str]]:
    text = BeautifulSoup(html_text or "", "html.parser").get_text("\n", strip=True)
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    results: List[Dict[str, str]] = []
    status_keywords = ["过期", "到期", "剩余", "释放", "可用", "空闲", "正常", "有效期", "有效至", "即将释放", "倒计时"]

    for i, line in enumerate(lines):
        if "省" in line and "市" in line and len(line) >= 10 and "无需添加" not in line:
            status = "未知状态"
            nearby = lines[i:i + 8]

            for part in nearby:
                if any(k in part for k in status_keywords):
                    if is_available_status(part):
                        status = "已过期(纯净可用)"
                    else:
                        status = part
                    break

            results.append({"content": line, "status": status})

    return results


def reset_stale_sync_jobs(conn: sqlite3.Connection) -> None:
    """Move interrupted DB-backed jobs back to the queue after container restart."""
    try:
        now = now_text()
        conn.execute(
            """
            UPDATE sync_jobs
            SET status='queued',
                next_run_at=?,
                updated_at=?,
                last_error=COALESCE(last_error, '容器重启后恢复队列')
            WHERE status IN ('running', 'claimed')
            """,
            (now, now),
        )
    except Exception:
        pass


def create_sync_job(
    source_url: str,
    keyword: str,
    request_cookie: str = "",
    source_id: Optional[int] = None,
    attempt: int = 1,
    max_attempts: int = SYNC_RETRY_MAX_ATTEMPTS,
    scheduled: bool = True,
    next_run_at: Optional[str] = None,
    last_error: str = "",
) -> int:
    conn = get_conn()
    now = now_text()
    cur = conn.execute(
        """
        INSERT INTO sync_jobs(
            source_id, source_url, keyword, request_cookie, status,
            attempt, max_attempts, scheduled, next_run_at,
            last_error, created_at, updated_at
        ) VALUES(?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            source_url,
            keyword,
            clean_cookie(request_cookie),
            int(attempt or 1),
            int(max_attempts or SYNC_RETRY_MAX_ATTEMPTS),
            1 if scheduled else 0,
            next_run_at or now,
            last_error[:500] if last_error else "",
            now,
            now,
        ),
    )
    conn.commit()
    job_id = int(cur.lastrowid)
    conn.close()
    return job_id


def update_sync_job(job_id: Optional[int], **fields) -> None:
    if not job_id:
        return
    allowed = {"status", "task_id", "run_id", "last_error", "started_at", "finished_at", "next_run_at", "attempt"}
    payload = {k: v for k, v in fields.items() if k in allowed}
    if not payload:
        return
    payload["updated_at"] = now_text()
    sets = ", ".join([f"{k}=?" for k in payload.keys()])
    vals = list(payload.values()) + [int(job_id)]
    conn = get_conn()
    conn.execute(f"UPDATE sync_jobs SET {sets} WHERE id=?", vals)
    conn.commit()
    conn.close()


def dispatch_due_sync_jobs(limit: int = 3) -> int:
    """Start queued DB-backed jobs that are due. Keeps retries across restarts."""
    conn = get_conn()
    now = now_text()
    rows = conn.execute(
        """
        SELECT * FROM sync_jobs
        WHERE status='queued' AND next_run_at <= ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (now, int(limit)),
    ).fetchall()
    started = 0
    for row in rows:
        if has_running_task_for_source(row["source_url"], row["keyword"]):
            continue
        conn.execute(
            "UPDATE sync_jobs SET status='claimed', updated_at=? WHERE id=? AND status='queued'",
            (now, int(row["id"])),
        )
        conn.commit()
        payload = ScrapeRequest(
            target_url=row["source_url"],
            keyword=row["keyword"],
            request_cookie=row["request_cookie"] or "",
            source_id=row["source_id"],
            attempt=int(row["attempt"] or 1),
            job_id=int(row["id"]),
        )
        task_id = start_sync_task(payload, scheduled=bool(row["scheduled"]), job_id=int(row["id"]))
        add_log(f"持久化同步任务已启动：job={row['id']}，task={task_id}，source={row['source_url']}，keyword={row['keyword']}", "info")
        started += 1
    conn.close()
    return started


def parse_by_source(source_url: str, html_text: str) -> List[Dict[str, str]]:
    parsed = urlparse(source_url)
    host = (parsed.hostname or "").lower()

    results = parse_structural_data(html_text)

    if results:
        return results

    if "chuangshi88.cc" in host:
        return parse_text_fallback(html_text)

    return parse_text_fallback(html_text)


def create_sync_run(source_url: str, keyword: str, request_cookie: str, source_id: Optional[int] = None, attempt: int = 1, scheduled: bool = False) -> int:
    conn = get_conn()
    cur = conn.execute(
        """
        INSERT INTO sync_runs(source_id, source_url, keyword, used_proxy, used_cookie, status, started_at, attempt, scheduled)
        VALUES(?, ?, ?, 0, ?, ?, ?, ?, ?)
        """,
        (source_id, source_url, keyword, 1 if request_cookie else 0, "running", now_text(), int(attempt or 1), 1 if scheduled else 0),
    )
    conn.commit()
    run_id = int(cur.lastrowid)
    conn.close()
    return run_id


def finish_sync_run(
    run_id: int,
    status: str,
    total_found: int = 0,
    inserted_count: int = 0,
    updated_count: int = 0,
    restored_from_available: int = 0,
    error_message: str = "",
) -> None:
    try:
        conn = get_conn()
        conn.execute(
            """
            UPDATE sync_runs
            SET status=?,
                total_found=?,
                inserted_count=?,
                updated_count=?,
                restored_from_available=?,
                finished_at=?,
                error_message=?
            WHERE id=?
            """,
            (
                status,
                int(total_found or 0),
                int(inserted_count or 0),
                int(updated_count or 0),
                int(restored_from_available or 0),
                now_text(),
                error_message or "",
                run_id,
            ),
        )
        row = conn.execute("SELECT source_id FROM sync_runs WHERE id=?", (run_id,)).fetchone()
        if row and row["source_id"]:
            sid = int(row["source_id"])
            now = now_text()
            if status == "success":
                conn.execute(
                    "UPDATE source_configs SET last_success_at=?, last_error='', updated_at=? WHERE id=?",
                    (now, now, sid),
                )
            elif status in ("failed", "empty", "cancelled"):
                conn.execute(
                    "UPDATE source_configs SET last_failed_at=?, last_error=?, updated_at=? WHERE id=?",
                    (now, (error_message or status)[:500], now, sid),
                )
        conn.commit()
        conn.close()
    except Exception as exc:
        add_log(f"同步历史写入失败：{exc}", "error")


class ScrapeRequest(BaseModel):
    target_url: str
    keyword: str
    request_cookie: str = ""
    source_id: Optional[int] = None
    attempt: int = 1
    job_id: Optional[int] = None


class SourceConfigPayload(BaseModel):
    name: str
    source_url: str
    default_keyword: str = ""
    request_cookie: str = ""
    schedule_enabled: bool = False
    schedule_interval_minutes: int = 0
    enabled: bool = True
    sort_order: int = 0


class BulkDeleteRequest(BaseModel):
    ids: List[int]


class BackupMergeRequest(BaseModel):
    filename: str


def update_task(task_id: str, **kwargs) -> None:
    with TASK_LOCK:
        task = SYNC_TASKS.get(task_id)
        if not task:
            return
        task.update(kwargs)
        task["updated_at"] = now_text()


def get_task(task_id: str) -> Optional[Dict[str, object]]:
    with TASK_LOCK:
        task = SYNC_TASKS.get(task_id)
        return dict(task) if task else None


def task_cancel_requested(task_id: str) -> bool:
    with TASK_LOCK:
        task = SYNC_TASKS.get(task_id)
        return bool(task and task.get("cancel_requested"))


def process_batch_sync_for_task(task_id: str, payload: ScrapeRequest) -> Tuple[bool, List[Dict[str, str]], str]:
    try:
        source_url = validate_target_url(payload.target_url)
        request_cookie = clean_cookie(payload.request_cookie)
    except ValueError as exc:
        return False, [], str(exc)

    keyword = (payload.keyword or "").strip()

    if not keyword:
        return False, [], "检索关键词不能为空"

    update_task(task_id, status="running", phase="抓取第 1 页", current_page=1, total_pages=1, progress=3)

    first_page_html = fetch_page_with_retry(source_url, keyword, 1, request_cookie)

    if task_cancel_requested(task_id):
        return False, [], "任务已取消"

    if not first_page_html:
        return False, [], "同步失败：数据源未响应、被限流、Cookie 失效或网络连接受限。"

    if "/_guard/auto.js" in first_page_html and len(first_page_html.strip()) < 200:
        return False, [], "该数据源返回了防护脚本，未返回真实页面。请在数据源的“请求 Cookie”里填写浏览器 Network 中复制的 Cookie。"

    all_records = parse_by_source(source_url, first_page_html)
    first_soup = BeautifulSoup(first_page_html, "html.parser")
    clean_text = first_soup.get_text(separator="").replace(" ", "").replace("\xa0", "").replace("\n", "")

    match = re.search(r"共.*?(\d+).*?条", clean_text)

    if match:
        total_pages = math.ceil(int(match.group(1)) / 10)
    else:
        total_pages = 150

    total_pages = max(1, min(total_pages, MAX_PAGES))

    update_task(
        task_id,
        phase=f"已解析总页数 {total_pages}",
        current_page=1,
        total_pages=total_pages,
        total_found=len(all_records),
        progress=8,
    )

    for page in range(2, total_pages + 1):
        if task_cancel_requested(task_id):
            return False, [], "任务已取消"

        progress = 8 + int((page - 1) / max(total_pages, 1) * 72)

        update_task(
            task_id,
            phase=f"抓取第 {page} / {total_pages} 页",
            current_page=page,
            total_pages=total_pages,
            total_found=len(all_records),
            progress=progress,
        )

        html = fetch_page_with_retry(source_url, keyword, page, request_cookie)

        if html:
            all_records.extend(parse_by_source(source_url, html))

    unique_results: List[Dict[str, str]] = []
    seen = set()

    for item in all_records:
        content = item.get("content", "").strip()

        if content and content not in seen:
            seen.add(content)
            unique_results.append({
                "content": content,
                "status": item.get("status", "未知状态"),
            })

    update_task(task_id, phase="抓取完成，准备写入数据库", total_found=len(unique_results), progress=85)

    return True, unique_results, "同步完成"


def write_sync_results(task_id: str, run_id: int, payload: ScrapeRequest, new_data: List[Dict[str, str]]) -> Dict[str, int]:
    target_url = validate_target_url(payload.target_url)
    keyword = payload.keyword.strip()
    current_time = now_text()
    source_id = int(payload.source_id) if payload.source_id else None

    inserted_count = 0
    updated_count = 0
    restored_from_available = 0

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS current_scraped")
    cur.execute("CREATE TEMP TABLE current_scraped(content_text TEXT PRIMARY KEY)")

    for idx, item in enumerate(new_data, start=1):
        if task_cancel_requested(task_id):
            conn.rollback()
            conn.close()
            raise RuntimeError("任务已取消")

        content = item["content"]
        status_timer = item["status"]
        status_type, remaining_hours, raw_status = normalize_status_fields(status_timer)

        cur.execute("INSERT OR IGNORE INTO current_scraped(content_text) VALUES(?)", (content,))

        old = None
        if source_id:
            old = cur.execute(
                """
                SELECT id, status_timer
                FROM asset_records
                WHERE source_id=? AND content_text=?
                  AND deleted_at IS NULL
                ORDER BY last_checked DESC, id DESC
                LIMIT 1
                """,
                (source_id, content),
            ).fetchone()

        if old is None:
            old = cur.execute(
                """
                SELECT id, status_timer
                FROM asset_records
                WHERE source_url=? AND content_text=?
                  AND deleted_at IS NULL
                ORDER BY last_checked DESC, id DESC
                LIMIT 1
                """,
                (target_url, content),
            ).fetchone()

        if old is None:
            inserted_count += 1
            cur.execute(
                """
                INSERT INTO asset_records(
                    source_id, source_url, keyword, content_text, status_timer, last_checked,
                    status_type, remaining_hours, raw_status, deleted_at, deleted_by, delete_reason
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                ON CONFLICT(source_url, content_text) DO UPDATE SET
                    source_id=excluded.source_id,
                    keyword=excluded.keyword,
                    status_timer=excluded.status_timer,
                    last_checked=excluded.last_checked,
                    status_type=excluded.status_type,
                    remaining_hours=excluded.remaining_hours,
                    raw_status=excluded.raw_status,
                    deleted_at=NULL,
                    deleted_by=NULL,
                    delete_reason=NULL
                """,
                (
                    source_id,
                    target_url,
                    keyword,
                    content,
                    status_timer,
                    current_time,
                    status_type,
                    remaining_hours,
                    raw_status,
                ),
            )
        else:
            updated_count += 1
            old_status = old["status_timer"] or ""
            if is_available_status(old_status) and not is_available_status(status_timer):
                restored_from_available += 1
            cur.execute(
                """
                UPDATE asset_records
                SET source_id = COALESCE(?, source_id),
                    source_url = ?,
                    keyword = ?,
                    status_timer = ?,
                    last_checked = ?,
                    status_type = ?,
                    remaining_hours = ?,
                    raw_status = ?
                WHERE id = ?
                """,
                (
                    source_id,
                    target_url,
                    keyword,
                    status_timer,
                    current_time,
                    status_type,
                    remaining_hours,
                    raw_status,
                    old["id"],
                ),
            )

        if idx % 50 == 0:
            update_task(
                task_id,
                phase=f"写入数据库 {idx} / {len(new_data)}",
                progress=85 + int(idx / max(len(new_data), 1) * 12),
            )

    if source_id:
        cur.execute(
            """
            UPDATE asset_records
            SET status_timer = ?,
                last_checked = ?,
                status_type = ?,
                remaining_hours = ?,
                raw_status = ?
            WHERE keyword = ?
              AND source_id = ?
              AND deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM current_scraped c WHERE c.content_text = asset_records.content_text
              )
              AND status_timer NOT LIKE ?
              AND status_timer NOT LIKE ?
            """,
            ("已过期(纯净可用)", current_time, "safe", -1, "已过期(纯净可用)", keyword, source_id, "%已过期%", "%释放%"),
        )
    else:
        cur.execute(
            """
            UPDATE asset_records
            SET status_timer = ?,
                last_checked = ?,
                status_type = ?,
                remaining_hours = ?,
                raw_status = ?
            WHERE keyword = ?
              AND source_url = ?
              AND deleted_at IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM current_scraped c WHERE c.content_text = asset_records.content_text
              )
              AND status_timer NOT LIKE ?
              AND status_timer NOT LIKE ?
            """,
            ("已过期(纯净可用)", current_time, "safe", -1, "已过期(纯净可用)", keyword, target_url, "%已过期%", "%释放%"),
        )

    if source_id:
        cur.execute(
            "UPDATE source_configs SET last_used_at=?, updated_at=? WHERE id=?",
            (current_time, current_time, source_id),
        )

    conn.commit()
    conn.close()

    return {
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "restored_from_available": restored_from_available,
    }


def schedule_retry_task(original_task_id: str, payload: ScrapeRequest, reason: str, scheduled: bool) -> None:
    if not (scheduled and SYNC_RETRY_ENABLED):
        return
    attempt = int(getattr(payload, "attempt", 1) or 1)
    if attempt >= SYNC_RETRY_MAX_ATTEMPTS:
        add_log(f"定时同步重试已达上限：task={original_task_id}，attempt={attempt}，原因={reason}", "error")
        return

    try:
        next_dt = bj_now() + timedelta(seconds=max(5, SYNC_RETRY_DELAY_SECONDS))
        job_id = create_sync_job(
            source_url=payload.target_url,
            keyword=payload.keyword,
            request_cookie=payload.request_cookie,
            source_id=payload.source_id,
            attempt=attempt + 1,
            max_attempts=SYNC_RETRY_MAX_ATTEMPTS,
            scheduled=True,
            next_run_at=next_dt.strftime("%Y-%m-%d %H:%M:%S"),
            last_error=reason,
        )
        add_log(f"已写入持久化重试任务：原任务={original_task_id}，job={job_id}，第 {attempt + 1}/{SYNC_RETRY_MAX_ATTEMPTS} 次，原因={reason}", "warn")
    except Exception as exc:
        add_log(f"创建同步重试失败：{exc}", "error")

def sync_task_worker(task_id: str, payload: ScrapeRequest, scheduled: bool = False) -> None:
    request_cookie = clean_cookie(payload.request_cookie)
    job_id = int(getattr(payload, "job_id", 0) or 0) or None
    run_id = create_sync_run(payload.target_url, payload.keyword, request_cookie, payload.source_id, int(getattr(payload, "attempt", 1) or 1), scheduled)
    update_sync_job(job_id, status="running", task_id=task_id, run_id=run_id, started_at=now_text())

    update_task(
        task_id,
        status="running",
        phase="任务开始",
        progress=1,
        run_id=run_id,
        scheduled=scheduled,
        started_at=now_text(),
    )

    add_log(
        f"{'定时' if scheduled else '手动'}后台同步开始：task={task_id}，source={payload.target_url}，keyword={payload.keyword}，Cookie={'有' if request_cookie else '无'}",
        "info",
    )

    try:
        success, new_data, message = process_batch_sync_for_task(task_id, payload)

        if task_cancel_requested(task_id) or message == "任务已取消":
            finish_sync_run(run_id, "cancelled", error_message="用户取消任务")
            update_sync_job(job_id, status="cancelled", last_error="用户取消任务", finished_at=now_text())
            update_task(task_id, status="cancelled", phase="任务已取消", progress=100, message="任务已取消", finished_at=now_text())
            add_log(f"后台同步已取消：task={task_id}", "warn")
            return

        if not success:
            finish_sync_run(run_id, "failed", error_message=message)
            update_sync_job(job_id, status="failed", last_error=message, finished_at=now_text())
            update_task(task_id, status="failed", phase="任务失败", progress=100, message=message, finished_at=now_text())
            add_log(f"后台同步失败：task={task_id}，{message}", "error")
            schedule_retry_task(task_id, payload, message, scheduled)
            return

        if len(new_data) == 0:
            warning_message = (
                "同步完成，但未解析到任何资产记录。"
                "可能原因：关键词无结果、Cookie 已失效、页面被防护拦截、接口返回空内容，或解析规则不适配。"
                "本次不会把旧数据批量标记为纯净可用。"
            )
            finish_sync_run(run_id, "empty", total_found=0, error_message=warning_message)
            update_sync_job(job_id, status="empty", last_error=warning_message, finished_at=now_text())
            update_task(task_id, status="empty", phase="空结果", progress=100, total_found=0, message=warning_message, finished_at=now_text())
            add_log(warning_message, "warn")
            schedule_retry_task(task_id, payload, warning_message, scheduled)
            return

        update_task(task_id, phase="写入数据库", progress=86)

        counts = write_sync_results(task_id, run_id, payload, new_data)

        finish_sync_run(
            run_id,
            "success",
            total_found=len(new_data),
            inserted_count=counts["inserted_count"],
            updated_count=counts["updated_count"],
            restored_from_available=counts["restored_from_available"],
        )
        update_sync_job(job_id, status="success", last_error="", finished_at=now_text())

        restored_note = f"，其中 {counts['restored_from_available']} 条从纯净可用恢复为倒计时状态" if counts["restored_from_available"] else ""
        message = f"同步完成！解析 {len(new_data)} 条，新增 {counts['inserted_count']} 条，更新 {counts['updated_count']} 条{restored_note}。"

        update_task(
            task_id,
            status="success",
            phase="完成",
            progress=100,
            total_found=len(new_data),
            message=message,
            inserted_count=counts["inserted_count"],
            updated_count=counts["updated_count"],
            restored_from_available=counts["restored_from_available"],
            finished_at=now_text(),
        )

        add_log(f"后台同步完成：task={task_id}，{message}", "info")

    except Exception as exc:
        err = str(exc)
        finish_sync_run(run_id, "failed", error_message=err)
        update_sync_job(job_id, status="failed", last_error=err, finished_at=now_text())
        update_task(task_id, status="failed", phase="异常失败", progress=100, message=err, finished_at=now_text())
        add_log(f"后台同步异常：task={task_id}，{err}", "error")
        schedule_retry_task(task_id, payload, err, scheduled)


def task_source_name(source_id: Optional[int]) -> str:
    if not source_id:
        return ""
    try:
        conn = get_conn()
        row = conn.execute("SELECT name FROM source_configs WHERE id=?", (int(source_id),)).fetchone()
        conn.close()
        return row["name"] if row else ""
    except Exception:
        return ""


def start_sync_task(payload: ScrapeRequest, scheduled: bool = False, job_id: Optional[int] = None) -> str:
    task_id = uuid.uuid4().hex[:12]

    with TASK_LOCK:
        SYNC_TASKS[task_id] = {
            "task_id": task_id,
            "status": "queued",
            "phase": "等待执行",
            "progress": 0,
            "current_page": 0,
            "total_pages": 0,
            "total_found": 0,
            "message": "",
            "cancel_requested": False,
            "created_at": now_text(),
            "updated_at": now_text(),
            "finished_at": None,
            "source_url": payload.target_url,
            "keyword": payload.keyword,
            "source_name": task_source_name(payload.source_id),
            "scheduled": scheduled,
            "source_id": payload.source_id,
            "attempt": int(getattr(payload, "attempt", 1) or 1),
            "job_id": job_id or getattr(payload, "job_id", None),
        }

    th = threading.Thread(target=sync_task_worker, args=(task_id, payload, scheduled), daemon=True)
    th.start()
    return task_id


def has_running_task_for_source(source_url: str, keyword: str) -> bool:
    with TASK_LOCK:
        for task in SYNC_TASKS.values():
            if task.get("status") in ("queued", "running"):
                if task.get("source_url") == source_url and task.get("keyword") == keyword:
                    return True
    return False


def scheduler_loop() -> None:
    add_log("定时同步调度器已启动", "info")

    while not SCHEDULER_STOP.is_set():
        try:
            auto_purify_expired_countdowns("scheduler")
            maybe_auto_backup()
            dispatch_due_sync_jobs(limit=3)
            conn = get_conn()
            rows = conn.execute(
                """
                SELECT id, name, source_url, default_keyword, request_cookie,
                       schedule_enabled, schedule_interval_minutes, last_used_at,
                       last_scheduled_at, last_success_at, last_failed_at, last_error
                FROM source_configs
                WHERE enabled=1
                  AND schedule_enabled=1
                  AND schedule_interval_minutes >= 5
                ORDER BY id ASC
                """
            ).fetchall()
            conn.close()

            now_dt = bj_now()

            for row in rows:
                source_url = row["source_url"]
                keyword = row["default_keyword"] or ""
                interval = int(row["schedule_interval_minutes"] or 0)

                if not keyword or interval < 5:
                    continue

                last_mark = row["last_scheduled_at"] or row["last_used_at"]
                due = False

                if not last_mark:
                    due = True
                else:
                    try:
                        last_dt = datetime.strptime(last_mark, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone(timedelta(hours=8)))
                        due = (now_dt - last_dt).total_seconds() >= interval * 60
                    except Exception:
                        due = True

                if not due:
                    continue

                if has_running_task_for_source(source_url, keyword):
                    continue

                throttle_conn = get_conn()
                throttle_conn.execute(
                    "UPDATE source_configs SET last_scheduled_at=?, updated_at=? WHERE id=?",
                    (now_text(), now_text(), int(row["id"])),
                )
                throttle_conn.commit()
                throttle_conn.close()

                job_id = create_sync_job(
                    source_url=source_url,
                    keyword=keyword,
                    request_cookie=row["request_cookie"] or "",
                    source_id=int(row["id"]),
                    scheduled=True,
                    next_run_at=now_text(),
                )
                payload = ScrapeRequest(
                    target_url=source_url,
                    keyword=keyword,
                    request_cookie=row["request_cookie"] or "",
                    source_id=int(row["id"]),
                    job_id=job_id,
                )

                task_id = start_sync_task(payload, scheduled=True, job_id=job_id)
                add_log(f"定时同步已创建：job={job_id}，task={task_id}，source={source_url}，keyword={keyword}", "info")

        except Exception as exc:
            add_log(f"定时同步调度器异常：{exc}", "error")

        SCHEDULER_STOP.wait(60)

    add_log("定时同步调度器已停止", "warn")


def create_backup_file() -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = bj_now().strftime("%Y-%m-%d_%H%M%S")
    out_file = BACKUP_DIR / f"asset_console_backup_{ts}.tar.gz"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        for name in ["app.py", "Dockerfile", "requirements.txt", "docker-compose.yml"]:
            src = Path(name)
            if src.exists():
                shutil.copy2(src, tmp_path / name)

        src_conn = sqlite3.connect(DB_PATH)
        dst_conn = sqlite3.connect(tmp_path / "asset_management.db")
        src_conn.backup(dst_conn)
        dst_conn.close()
        src_conn.close()

        with tarfile.open(out_file, "w:gz") as tar:
            for item in tmp_path.iterdir():
                tar.add(item, arcname=item.name)

    add_log(f"已创建备份：{out_file.name}", "info")
    return out_file


def cleanup_old_backups(retention: int = AUTO_BACKUP_RETENTION_COUNT) -> int:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(BACKUP_DIR.glob("*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True)
    removed = 0
    for path in files[max(0, retention):]:
        try:
            path.unlink()
            removed += 1
        except Exception:
            pass
    if removed:
        add_log(f"自动备份清理完成：删除旧备份 {removed} 个", "warn")
    return removed


def maybe_auto_backup() -> None:
    if not AUTO_BACKUP_ENABLED:
        return
    try:
        conn = get_conn()
        row = conn.execute("SELECT value FROM app_meta WHERE key='last_auto_backup_at'").fetchone()
        last = row["value"] if row else ""
        due = True
        if last:
            last_dt = parse_db_time(last)
            due = not last_dt or (bj_now() - last_dt).total_seconds() >= AUTO_BACKUP_INTERVAL_HOURS * 3600
        if not due:
            conn.close()
            return
        now = now_text()
        conn.execute("INSERT OR REPLACE INTO app_meta(key, value) VALUES(?, ?)", ("last_auto_backup_at", now))
        conn.commit()
        conn.close()
        create_backup_file()
        cleanup_old_backups()
        add_log("自动备份完成", "info")
    except Exception as exc:
        add_log(f"自动备份失败：{exc}", "error")


def find_db_in_tar(tar_path: Path) -> bytes:
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if Path(member.name).name == "asset_management.db" and member.isfile():
                f = tar.extractfile(member)
                if not f:
                    break
                return f.read()

    raise ValueError("备份包中未找到 asset_management.db")


def merge_backup_db_bytes(db_bytes: bytes) -> Dict[str, int]:
    with tempfile.TemporaryDirectory() as tmp:
        backup_db = Path(tmp) / "backup.db"
        backup_db.write_bytes(db_bytes)

        bconn = sqlite3.connect(str(backup_db))
        bconn.row_factory = sqlite3.Row
        bcur = bconn.cursor()

        conn = get_conn()
        cur = conn.cursor()

        tables = [x[0] for x in bcur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

        merged_assets = 0
        merged_sources = 0
        merged_logs = 0
        merged_runs = 0

        if "asset_records" in tables:
            rows = bcur.execute("""
                SELECT source_url, keyword, content_text, status_timer, last_checked,
                       status_type, remaining_hours, raw_status
                FROM asset_records
            """).fetchall()

            for row in rows:
                cur.execute("""
                    INSERT OR IGNORE INTO asset_records(
                        source_url, keyword, content_text, status_timer, last_checked,
                        status_type, remaining_hours, raw_status
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["source_url"],
                    row["keyword"],
                    row["content_text"],
                    row["status_timer"],
                    row["last_checked"],
                    row["status_type"] if "status_type" in row.keys() else None,
                    row["remaining_hours"] if "remaining_hours" in row.keys() else None,
                    row["raw_status"] if "raw_status" in row.keys() else row["status_timer"],
                ))
                merged_assets += cur.rowcount

        if "source_configs" in tables:
            bcols = table_columns(bconn, "source_configs")
            rows = bcur.execute("SELECT * FROM source_configs").fetchall()

            for row in rows:
                name = row["name"] if "name" in bcols else ""
                if not name:
                    continue

                cur.execute("""
                    INSERT OR IGNORE INTO source_configs(
                        name, source_url, default_keyword, default_proxy, request_cookie,
                        schedule_enabled, schedule_interval_minutes, enabled,
                        sort_order, last_used_at, created_at, updated_at
                    )
                    VALUES(?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    name,
                    row["source_url"] if "source_url" in bcols else "",
                    row["default_keyword"] if "default_keyword" in bcols else "",
                    row["request_cookie"] if "request_cookie" in bcols else "",
                    row["schedule_enabled"] if "schedule_enabled" in bcols else 0,
                    row["schedule_interval_minutes"] if "schedule_interval_minutes" in bcols else 0,
                    row["enabled"] if "enabled" in bcols else 1,
                    row["sort_order"] if "sort_order" in bcols else 0,
                    row["last_used_at"] if "last_used_at" in bcols else None,
                    row["created_at"] if "created_at" in bcols else now_text(),
                    row["updated_at"] if "updated_at" in bcols else now_text(),
                ))
                merged_sources += cur.rowcount

        if "app_logs" in tables:
            rows = bcur.execute("SELECT level, message, created_at FROM app_logs ORDER BY id ASC LIMIT 5000").fetchall()
            for row in rows:
                cur.execute(
                    "INSERT INTO app_logs(level, message, created_at) VALUES(?, ?, ?)",
                    (row["level"], row["message"], row["created_at"]),
                )
                merged_logs += 1

        if "sync_runs" in tables:
            rows = bcur.execute("""
                SELECT source_url, keyword, used_proxy, used_cookie, status, total_found,
                       inserted_count, updated_count, restored_from_available,
                       started_at, finished_at, error_message
                FROM sync_runs ORDER BY id ASC LIMIT 1000
            """).fetchall()

            for row in rows:
                cur.execute("""
                    INSERT INTO sync_runs(
                        source_url, keyword, used_proxy, used_cookie, status, total_found,
                        inserted_count, updated_count, restored_from_available,
                        started_at, finished_at, error_message
                    )
                    VALUES(?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["source_url"],
                    row["keyword"],
                    row["used_cookie"],
                    row["status"],
                    row["total_found"],
                    row["inserted_count"],
                    row["updated_count"],
                    row["restored_from_available"],
                    row["started_at"],
                    row["finished_at"],
                    row["error_message"],
                ))
                merged_runs += 1

        conn.commit()
        conn.close()
        bconn.close()

        return {
            "assets": merged_assets,
            "sources": merged_sources,
            "logs": merged_logs,
            "sync_runs": merged_runs,
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    global SCHEDULER_THREAD
    init_db()
    add_log("系统启动完成", "info")
    SCHEDULER_STOP.clear()
    SCHEDULER_THREAD = threading.Thread(target=scheduler_loop, daemon=True)
    SCHEDULER_THREAD.start()
    yield
    SCHEDULER_STOP.set()
    add_log("系统停止", "warn")


app = FastAPI(title=APP_NAME, lifespan=lifespan)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    return response


@app.get("/health")
def health():
    return {"ok": True, "service": "asset-console", "time": now_text()}



def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def is_login_rate_limited(request: Request) -> bool:
    key = get_client_ip(request)
    now = time.time()
    cutoff = now - LOGIN_WINDOW_SECONDS

    with LOGIN_LOCK:
        attempts = [ts for ts in LOGIN_ATTEMPTS.get(key, []) if ts >= cutoff]
        LOGIN_ATTEMPTS[key] = attempts
        return len(attempts) >= MAX_LOGIN_ATTEMPTS


def record_login_failure(request: Request) -> None:
    key = get_client_ip(request)
    now = time.time()
    cutoff = now - LOGIN_WINDOW_SECONDS

    with LOGIN_LOCK:
        attempts = [ts for ts in LOGIN_ATTEMPTS.get(key, []) if ts >= cutoff]
        attempts.append(now)
        LOGIN_ATTEMPTS[key] = attempts


def clear_login_failures(request: Request) -> None:
    key = get_client_ip(request)

    with LOGIN_LOCK:
        LOGIN_ATTEMPTS.pop(key, None)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if get_current_user(request):
        return RedirectResponse("/", status_code=303)
    return HTMLResponse(render_auth_page("login"))


@app.post("/login")
def login_action(request: Request, username: str = Form(...), password: str = Form(...)):
    if is_login_rate_limited(request):
        return HTMLResponse(render_auth_page("login", "登录失败次数过多，请稍后再试"), status_code=429)

    username = username.strip()

    conn = get_conn()
    row = conn.execute(
        "SELECT id, username, password_hash FROM users WHERE username = ?",
        (username,),
    ).fetchone()

    if not row or not verify_password(password, row["password_hash"]):
        conn.close()
        record_login_failure(request)
        return HTMLResponse(render_auth_page("login", "用户名或密码不正确"), status_code=400)

    conn.execute("UPDATE users SET last_login = ? WHERE id = ?", (now_text(), row["id"]))
    conn.commit()
    conn.close()

    clear_login_failures(request)
    token = create_session(int(row["id"]))
    response = RedirectResponse("/", status_code=303)
    set_auth_cookie(response, token)
    add_log(f"用户登录：{username}", "info")
    return response


@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    if not REGISTRATION_ENABLED:
        return HTMLResponse(render_auth_page("register", "当前服务器已关闭新用户注册"), status_code=403)

    if get_current_user(request):
        return RedirectResponse("/", status_code=303)

    return HTMLResponse(render_auth_page("register"))


@app.post("/register")
def register_action(username: str = Form(...), password: str = Form(...), confirm_password: str = Form(...)):
    if not REGISTRATION_ENABLED:
        return HTMLResponse(render_auth_page("register", "当前服务器已关闭新用户注册"), status_code=403)

    username = username.strip()

    if not re.match(r"^[a-zA-Z0-9_\-\u4e00-\u9fa5]{3,32}$", username):
        return HTMLResponse(render_auth_page("register", "用户名需为 3-32 位，可包含中文、英文、数字、下划线或短横线"), status_code=400)

    if len(password) < 8:
        return HTMLResponse(render_auth_page("register", "密码至少需要 8 位"), status_code=400)

    if password != confirm_password:
        return HTMLResponse(render_auth_page("register", "两次输入的密码不一致"), status_code=400)

    conn = get_conn()

    try:
        cur = conn.execute(
            "INSERT INTO users(username, password_hash, created_at) VALUES(?, ?, ?)",
            (username, make_password_hash(password), now_text()),
        )
        conn.commit()
        user_id = int(cur.lastrowid)
    except sqlite3.IntegrityError:
        conn.close()
        return HTMLResponse(render_auth_page("register", "该用户名已存在"), status_code=400)

    conn.close()

    token = create_session(user_id)
    response = RedirectResponse("/", status_code=303)
    set_auth_cookie(response, token)
    add_log(f"新用户注册：{username}", "info")
    return response


@app.post("/logout")
def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE_NAME)

    if token:
        conn = get_conn()
        conn.execute("DELETE FROM sessions WHERE token_hash = ?", (hash_token(token),))
        conn.commit()
        conn.close()

    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return response


@app.get("/api/stats")
def api_stats(user: Dict[str, object] = Depends(require_user)):
    auto_purify_expired_countdowns("stats")
    conn = get_conn()

    def count(table: str) -> int:
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception:
            return 0

    active_assets = conn.execute("SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NULL").fetchone()[0]
    trash_assets = conn.execute("SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NOT NULL").fetchone()[0]
    safe = conn.execute("SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NULL AND (status_type='safe' OR status_timer LIKE '%已过期%' OR status_timer LIKE '%释放%')").fetchone()[0]
    danger = conn.execute("SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NULL AND COALESCE(status_type, '') NOT IN ('safe', 'unknown') AND status_timer NOT LIKE '%已过期%' AND status_timer NOT LIKE '%释放%'").fetchone()[0]
    unknown = conn.execute("SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NULL AND (status_type='unknown' OR status_timer LIKE '%未知状态%')").fetchone()[0]

    data = {
        "assets": active_assets,
        "trash_assets": trash_assets,
        "sources": count("source_configs"),
        "users": count("users"),
        "logs": count("app_logs"),
        "sync_runs": count("sync_runs"),
        "safe": safe,
        "danger": danger,
        "unknown": unknown,
        "db_path": str(Path(DB_PATH).resolve()),
        "db_size": Path(DB_PATH).stat().st_size if Path(DB_PATH).exists() else 0,
        "time": now_text(),
    }

    conn.close()
    return data


@app.get("/api/logs")
def api_logs(user: Dict[str, object] = Depends(require_user)):
    try:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT created_at AS time, level, message
            FROM app_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (MAX_APP_LOGS,),
        ).fetchall()
        conn.close()
        data = [dict(row) for row in rows]
        data.reverse()
        return {"data": data}
    except Exception:
        return {"data": APP_LOGS[-MAX_APP_LOGS:]}


@app.get("/api/sync_runs")
def api_sync_runs(user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT r.id, r.source_id, COALESCE(s.name, r.source_url) AS source_name, r.source_url, r.keyword, r.used_proxy, r.used_cookie, r.status,
               r.total_found, r.inserted_count, r.updated_count, r.restored_from_available,
               r.started_at, r.finished_at, r.error_message
        FROM sync_runs r
        LEFT JOIN source_configs s ON s.id = r.source_id
        ORDER BY r.id DESC
        LIMIT 100
        """
    ).fetchall()
    data = dedupe_record_rows(rows)
    conn.close()
    return {"data": data}


@app.get("/api/sources")
def list_sources(user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    rows = conn.execute("""
        SELECT s.id, s.name, s.source_url, s.default_keyword, s.request_cookie,
               s.schedule_enabled, s.schedule_interval_minutes,
               s.enabled, s.sort_order, s.last_used_at, s.last_scheduled_at,
               s.last_success_at, s.last_failed_at, s.last_error,
               s.created_at, s.updated_at,
               (SELECT COUNT(1) FROM asset_records a WHERE a.source_id=s.id AND a.deleted_at IS NULL) AS asset_count,
               (SELECT COUNT(1) FROM sync_jobs j WHERE j.source_id=s.id AND j.status IN ('queued','claimed','running')) AS active_job_count,
               (SELECT COUNT(1) FROM sync_jobs j WHERE j.source_id=s.id AND j.status='failed') AS failed_job_count
        FROM source_configs s
        ORDER BY s.enabled DESC, s.sort_order ASC, s.updated_at DESC, s.id DESC
    """).fetchall()
    conn.close()

    data = []
    for row in rows:
        item = dict(row)
        if not item.get("enabled"):
            item["health_status"] = "disabled"
            item["health_label"] = "已停用"
            item["health_detail"] = "该数据源已停用"
        elif item.get("active_job_count"):
            item["health_status"] = "syncing"
            item["health_label"] = "同步中"
            item["health_detail"] = f"当前有 {item.get('active_job_count') or 0} 个同步任务"
        elif item.get("last_error"):
            item["health_status"] = "error"
            item["health_label"] = "需检查"
            item["health_detail"] = item.get("last_error") or "最近同步失败"
        elif item.get("last_success_at"):
            item["health_status"] = "ok"
            item["health_label"] = "正常"
            item["health_detail"] = f"上次成功：{item.get('last_success_at')}"
        elif item.get("schedule_enabled"):
            item["health_status"] = "pending"
            item["health_label"] = "待同步"
            item["health_detail"] = "已开启定时，但还没有成功同步记录"
        else:
            item["health_status"] = "idle"
            item["health_label"] = "未定时"
            item["health_detail"] = "未开启定时同步"
        data.append(item)
    return {"data": data}


def validate_source_payload(payload: SourceConfigPayload) -> Tuple[str, str, str, str, int, int, int, int]:
    name = payload.name.strip()

    if not name or len(name) > 60:
        raise ValueError("数据源名称不能为空，且不能超过 60 个字符")

    source_url = validate_target_url(payload.source_url)
    default_keyword = (payload.default_keyword or "").strip()
    request_cookie = clean_cookie(payload.request_cookie)
    schedule_enabled = 1 if payload.schedule_enabled else 0
    schedule_interval_minutes = max(0, int(payload.schedule_interval_minutes or 0))
    enabled = 1 if payload.enabled else 0
    sort_order = int(payload.sort_order or 0)

    if schedule_enabled and schedule_interval_minutes < 5:
        raise ValueError("定时同步间隔不能小于 5 分钟")

    if schedule_enabled and not default_keyword:
        raise ValueError("开启定时同步时，默认检索关键词不能为空")

    return name, source_url, default_keyword, request_cookie, schedule_enabled, schedule_interval_minutes, enabled, sort_order


@app.post("/api/sources")
def create_source(payload: SourceConfigPayload, user: Dict[str, object] = Depends(require_user)):
    try:
        name, source_url, default_keyword, request_cookie, schedule_enabled, schedule_interval_minutes, enabled, sort_order = validate_source_payload(payload)
    except ValueError as exc:
        return JSONResponse({"success": False, "message": str(exc)}, status_code=400)

    conn = get_conn()

    try:
        cur = conn.execute("""
            INSERT INTO source_configs(
                name, source_url, default_keyword, default_proxy, request_cookie,
                schedule_enabled, schedule_interval_minutes, enabled,
                sort_order, created_at, updated_at
            )
            VALUES(?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?)
        """, (name, source_url, default_keyword, request_cookie, schedule_enabled, schedule_interval_minutes, enabled, sort_order, now_text(), now_text()))
        new_id = int(cur.lastrowid)
        conn.execute(
            """
            UPDATE asset_records
            SET source_id=?
            WHERE source_id IS NULL
              AND source_url=?
              AND (keyword=? OR ?='')
            """,
            (new_id, source_url, default_keyword, default_keyword),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return JSONResponse({"success": False, "message": "该数据源名称已存在，请换一个名称或点击更新"}, status_code=400)

    conn.close()
    add_log(f"数据源已保存：{name}，Cookie={'有' if request_cookie else '无'}，定时={'开' if schedule_enabled else '关'}", "info")
    return {"success": True, "message": "数据源已保存", "id": new_id}


@app.put("/api/sources/{source_id}")
def update_source(source_id: int, payload: SourceConfigPayload, user: Dict[str, object] = Depends(require_user)):
    try:
        name, source_url, default_keyword, request_cookie, schedule_enabled, schedule_interval_minutes, enabled, sort_order = validate_source_payload(payload)
    except ValueError as exc:
        return JSONResponse({"success": False, "message": str(exc)}, status_code=400)

    conn = get_conn()
    old_source = conn.execute("SELECT name, source_url FROM source_configs WHERE id=?", (source_id,)).fetchone()

    try:
        if old_source:
            remember_source_url_alias(conn, old_source["source_url"], old_source["name"])
        cur = conn.execute("""
            UPDATE source_configs
            SET name=?,
                source_url=?,
                default_keyword=?,
                default_proxy='',
                request_cookie=?,
                schedule_enabled=?,
                schedule_interval_minutes=?,
                enabled=?,
                sort_order=?,
                updated_at=?
            WHERE id=?
        """, (name, source_url, default_keyword, request_cookie, schedule_enabled, schedule_interval_minutes, enabled, sort_order, now_text(), source_id))
        conn.execute(
            """
            UPDATE asset_records
            SET source_id=?
            WHERE (source_id IS NULL OR source_id=?)
              AND source_url=?
              AND (keyword=? OR ?='')
            """,
            (source_id, source_id, source_url, default_keyword, default_keyword),
        )
        # 如果本次修改了网址，旧网址会被记录为别名；旧资产仍然归到原数据源标头下。
        remember_source_url_alias(conn, source_url, name)
        repair_asset_source_bindings(conn)
        conn.commit()
        changed = cur.rowcount
    except sqlite3.IntegrityError:
        conn.close()
        return JSONResponse({"success": False, "message": "该数据源名称已存在"}, status_code=400)

    conn.close()

    if not changed:
        return JSONResponse({"success": False, "message": "数据源不存在"}, status_code=404)

    add_log(f"数据源已更新：{name}，Cookie={'有' if request_cookie else '无'}，定时={'开' if schedule_enabled else '关'}", "info")
    return {"success": True, "message": "数据源已更新"}


@app.delete("/api/sources/{source_id}")
def delete_source(source_id: int, delete_assets: bool = False, user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    row = conn.execute("SELECT name FROM source_configs WHERE id=?", (source_id,)).fetchone()
    if not row:
        conn.close()
        return JSONResponse({"success": False, "message": "数据源不存在"}, status_code=404)

    asset_count = conn.execute("SELECT COUNT(1) AS c FROM asset_records WHERE source_id=? AND deleted_at IS NULL", (source_id,)).fetchone()["c"]

    if delete_assets:
        conn.execute("UPDATE asset_records SET deleted_at=?, deleted_by=?, delete_reason=? WHERE source_id=? AND deleted_at IS NULL", (now_text(), str(user.get("username", "")), "删除数据源时选择同时删除", source_id))
        asset_msg = f"，并将 {asset_count} 条资产移入回收站"
    else:
        conn.execute("UPDATE asset_records SET source_id=NULL WHERE source_id=?", (source_id,))
        asset_msg = f"，保留 {asset_count} 条资产"

    cur = conn.execute("DELETE FROM source_configs WHERE id=?", (source_id,))
    conn.commit()
    changed = cur.rowcount
    conn.close()

    if not changed:
        return JSONResponse({"success": False, "message": "数据源不存在"}, status_code=404)

    add_log(f"数据源已删除：{row['name']}{asset_msg}", "warn")
    return {"success": True, "message": f"数据源已删除{asset_msg}"}


@app.post("/api/sources/{source_id}/test")
def test_source(source_id: int, user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    row = conn.execute("SELECT id, name, source_url, default_keyword, request_cookie FROM source_configs WHERE id=?", (source_id,)).fetchone()
    conn.close()
    if not row:
        return JSONResponse({"success": False, "message": "数据源不存在"}, status_code=404)
    keyword = row["default_keyword"] or ""
    if not keyword:
        return JSONResponse({"success": False, "message": "测试失败：默认关键词为空"}, status_code=400)
    try:
        started = time.time()
        html = fetch_page_with_retry(row["source_url"], keyword, 1, row["request_cookie"] or "")
        elapsed = round(time.time() - started, 2)
        if not html:
            msg = "测试失败：数据源未响应、Cookie 失效、网络受限或被目标站拦截"
            add_log(f"数据源测试失败：{row['name']}，{msg}", "error")
            return {"success": False, "message": msg, "elapsed": elapsed, "parsed": 0}
        if "/_guard/auto.js" in html and len(html.strip()) < 200:
            msg = "测试失败：目标站返回防护脚本，请更新 Cookie"
            add_log(f"数据源测试失败：{row['name']}，{msg}", "warn")
            return {"success": False, "message": msg, "elapsed": elapsed, "parsed": 0}
        parsed = parse_by_source(row["source_url"], html)
        msg = f"测试成功：响应 {len(html)} 字符，首页解析 {len(parsed)} 条，用时 {elapsed} 秒"
        add_log(f"数据源测试成功：{row['name']}，首页解析 {len(parsed)} 条", "info")
        return {"success": True, "message": msg, "elapsed": elapsed, "parsed": len(parsed)}
    except Exception as exc:
        msg = explain_request_exception(exc)
        add_log(f"数据源测试异常：{row['name']}，{msg}", "error")
        return {"success": False, "message": msg}



@app.post("/api/sync_tasks")
def create_sync_task(payload: ScrapeRequest, user: Dict[str, object] = Depends(require_user)):
    try:
        validate_target_url(payload.target_url)
        clean_cookie(payload.request_cookie)
    except ValueError as exc:
        return JSONResponse({"success": False, "message": str(exc)}, status_code=400)

    if has_running_task_for_source(payload.target_url, payload.keyword):
        return JSONResponse({"success": False, "message": "同一数据源和关键词已有任务正在执行"}, status_code=409)

    task_id = start_sync_task(payload, scheduled=False)
    return {"success": True, "message": "后台同步任务已创建", "task_id": task_id}


@app.post("/api/sync_tasks/all_enabled")
def create_all_enabled_sync_tasks(user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT id, name, source_url, default_keyword, request_cookie
        FROM source_configs
        WHERE enabled=1 AND COALESCE(default_keyword, '') <> ''
        ORDER BY sort_order ASC, id ASC
        """
    ).fetchall()
    conn.close()

    created = []
    skipped = []
    for row in rows:
        if has_running_task_for_source(row["source_url"], row["default_keyword"] or ""):
            skipped.append(row["name"])
            continue
        payload = ScrapeRequest(
            target_url=row["source_url"],
            keyword=row["default_keyword"] or "",
            request_cookie=row["request_cookie"] or "",
            source_id=int(row["id"]),
        )
        created.append({"source": row["name"], "task_id": start_sync_task(payload, scheduled=False)})

    add_log(f"批量同步已创建：{len(created)} 个任务，跳过 {len(skipped)} 个", "info")
    return {"success": True, "message": f"已创建 {len(created)} 个同步任务，跳过 {len(skipped)} 个正在运行的数据源", "created": created, "skipped": skipped}


@app.post("/api/search_and_scrape")
def search_and_scrape(payload: ScrapeRequest, user: Dict[str, object] = Depends(require_user)):
    return create_sync_task(payload, user)


@app.get("/api/sync_tasks/{task_id}")
def read_sync_task(task_id: str, user: Dict[str, object] = Depends(require_user)):
    task = get_task(task_id)
    if not task:
        return JSONResponse({"success": False, "message": "任务不存在"}, status_code=404)
    return {"success": True, "task": task}


@app.post("/api/sync_tasks/{task_id}/cancel")
def cancel_sync_task(task_id: str, user: Dict[str, object] = Depends(require_user)):
    with TASK_LOCK:
        task = SYNC_TASKS.get(task_id)
        if not task:
            return JSONResponse({"success": False, "message": "任务不存在"}, status_code=404)
        if task.get("status") not in ("queued", "running"):
            return {"success": False, "message": "任务已结束，不能取消"}
        task["cancel_requested"] = True
        task["phase"] = "正在取消"
        task["updated_at"] = now_text()

    add_log(f"用户请求取消任务：task={task_id}", "warn")
    return {"success": True, "message": "已请求取消任务"}


@app.get("/api/get_records")
def get_records(user: Dict[str, object] = Depends(require_user)):
    auto_purify_expired_countdowns("get_records")
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            a.id,
            a.source_id,
            COALESCE(
                a.source_id,
                (SELECT sc.id FROM source_configs sc WHERE sc.source_url=a.source_url AND sc.default_keyword=a.keyword ORDER BY sc.updated_at DESC, sc.id DESC LIMIT 1),
                CASE WHEN (SELECT COUNT(1) FROM source_configs sc2 WHERE sc2.source_url=a.source_url)=1
                     THEN (SELECT sc3.id FROM source_configs sc3 WHERE sc3.source_url=a.source_url ORDER BY sc3.updated_at DESC, sc3.id DESC LIMIT 1)
                     ELSE NULL END
            ) AS effective_source_id,
            COALESCE(
                (SELECT sc.name FROM source_configs sc WHERE sc.id=a.source_id LIMIT 1),
                (SELECT sc.name FROM source_configs sc WHERE sc.source_url=a.source_url AND sc.default_keyword=a.keyword ORDER BY sc.updated_at DESC, sc.id DESC LIMIT 1),
                CASE WHEN (SELECT COUNT(1) FROM source_configs sc2 WHERE sc2.source_url=a.source_url)=1
                     THEN (SELECT sc3.name FROM source_configs sc3 WHERE sc3.source_url=a.source_url ORDER BY sc3.updated_at DESC, sc3.id DESC LIMIT 1)
                     ELSE NULL END
            ) AS source_name,
            a.content_text,
            a.status_timer,
            a.last_checked,
            a.source_url,
            a.keyword,
            a.status_type,
            a.remaining_hours,
            a.raw_status,
            a.expire_at
        FROM asset_records a
        WHERE a.deleted_at IS NULL
        ORDER BY a.last_checked DESC, a.id DESC
    """).fetchall()
    conn.close()
    return {"data": [dict(row) for row in rows]}


def classify_record_status(status_text: str, status_type: str = "") -> str:
    if status_type:
        if status_type == "safe":
            return "safe"
        if status_type == "unknown":
            return "unknown"
        return "danger"

    status_text = status_text or ""

    if is_available_status(status_text):
        return "safe"
    if "未知状态" in status_text:
        return "unknown"
    return "danger"


@app.get("/api/export_records")
def export_records(
    source_url: str = "all",
    source_id: str = "all",
    status_filter: str = "all",
    q: str = "",
    user: Dict[str, object] = Depends(require_user),
):
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            a.id,
            a.source_id,
            COALESCE(
                a.source_id,
                (SELECT sc.id FROM source_configs sc WHERE sc.source_url=a.source_url AND sc.default_keyword=a.keyword ORDER BY sc.updated_at DESC, sc.id DESC LIMIT 1),
                CASE WHEN (SELECT COUNT(1) FROM source_configs sc2 WHERE sc2.source_url=a.source_url)=1
                     THEN (SELECT sc3.id FROM source_configs sc3 WHERE sc3.source_url=a.source_url ORDER BY sc3.updated_at DESC, sc3.id DESC LIMIT 1)
                     ELSE NULL END
            ) AS effective_source_id,
            COALESCE(
                (SELECT sc.name FROM source_configs sc WHERE sc.id=a.source_id LIMIT 1),
                (SELECT sc.name FROM source_configs sc WHERE sc.source_url=a.source_url AND sc.default_keyword=a.keyword ORDER BY sc.updated_at DESC, sc.id DESC LIMIT 1),
                CASE WHEN (SELECT COUNT(1) FROM source_configs sc2 WHERE sc2.source_url=a.source_url)=1
                     THEN (SELECT sc3.name FROM source_configs sc3 WHERE sc3.source_url=a.source_url ORDER BY sc3.updated_at DESC, sc3.id DESC LIMIT 1)
                     ELSE NULL END
            ) AS source_name,
            a.content_text,
            a.status_timer,
            a.last_checked,
            a.source_url,
            a.keyword,
            a.status_type
        FROM asset_records a
        WHERE a.deleted_at IS NULL
        ORDER BY a.last_checked DESC, a.id DESC
    """).fetchall()
    conn.close()

    q = (q or "").strip().lower()
    data = []

    for item in dedupe_record_rows(rows):
        status_class = classify_record_status(item.get("status_timer") or "", item.get("status_type") or "")

        if source_id != "all":
            try:
                if int(item.get("effective_source_id") or 0) != int(source_id):
                    continue
            except Exception:
                continue
        elif source_url != "all" and item.get("source_url") != source_url:
            continue

        if status_filter != "all" and status_class != status_filter:
            continue
        if q:
            haystack = f"{item.get('content_text') or ''} {item.get('keyword') or ''} {item.get('source_name') or ''}".lower()
            if q not in haystack:
                continue

        data.append(item)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "地址内容", "状态", "最后更新", "数据源名称", "数据源网址", "关键词"])

    for item in data:
        writer.writerow([
            item.get("id", ""),
            item.get("content_text", ""),
            item.get("status_timer", ""),
            item.get("last_checked", ""),
            item.get("source_name", ""),
            item.get("source_url", ""),
            item.get("keyword", ""),
        ])

    content = "\ufeff" + output.getvalue()
    filename = f"asset_records_{bj_now().strftime('%Y%m%d_%H%M%S')}.csv"

    add_log(f"导出 CSV：{len(data)} 条", "info")

    return Response(
        content=content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/records/bulk_delete")
def bulk_delete_records(payload: BulkDeleteRequest, user: Dict[str, object] = Depends(require_user)):
    ids = []
    seen = set()

    for raw_id in payload.ids or []:
        try:
            rid = int(raw_id)
        except Exception:
            continue
        if rid > 0 and rid not in seen:
            seen.add(rid)
            ids.append(rid)

    if not ids:
        return JSONResponse({"success": False, "message": "没有可删除的记录"}, status_code=400)

    if len(ids) > 5000:
        return JSONResponse({"success": False, "message": "单次最多删除 5000 条，请缩小筛选范围"}, status_code=400)

    placeholders = ",".join(["?"] * len(ids))
    conn = get_conn()
    before = conn.execute(f"SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NULL AND id IN ({placeholders})", ids).fetchone()[0]
    conn.execute(f"UPDATE asset_records SET deleted_at=?, deleted_by=?, delete_reason=? WHERE deleted_at IS NULL AND id IN ({placeholders})", [now_text(), str(user.get("username", "")), "手动删除"] + ids)
    conn.commit()
    conn.close()

    add_log(f"资产记录已移入回收站：{before} 条", "warn")
    return {"success": True, "message": f"已将 {before} 条资产移入回收站，可在系统维护中恢复", "deleted_count": before}



@app.get("/api/records/trash")
def list_deleted_records(user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, content_text, status_timer, source_url, keyword, deleted_at, deleted_by, delete_reason
        FROM asset_records
        WHERE deleted_at IS NOT NULL
        ORDER BY deleted_at DESC, id DESC
        LIMIT 200
    """).fetchall()
    conn.close()
    return {"data": [dict(row) for row in rows]}


@app.post("/api/records/restore")
def restore_deleted_records(payload: BulkDeleteRequest, user: Dict[str, object] = Depends(require_user)):
    ids = []
    seen = set()
    for raw_id in payload.ids or []:
        try:
            rid = int(raw_id)
        except Exception:
            continue
        if rid > 0 and rid not in seen:
            seen.add(rid)
            ids.append(rid)
    if not ids:
        return JSONResponse({"success": False, "message": "没有可恢复的记录"}, status_code=400)
    placeholders = ",".join(["?"] * len(ids))
    conn = get_conn()
    count = conn.execute(f"SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NOT NULL AND id IN ({placeholders})", ids).fetchone()[0]
    conn.execute(f"UPDATE asset_records SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE id IN ({placeholders})", ids)
    conn.commit()
    conn.close()
    add_log(f"从回收站恢复资产记录：{count} 条", "info")
    return {"success": True, "message": f"已恢复 {count} 条资产记录", "restored_count": count}


@app.delete("/api/records/trash")
def purge_deleted_records(user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) FROM asset_records WHERE deleted_at IS NOT NULL").fetchone()[0]
    conn.execute("DELETE FROM asset_records WHERE deleted_at IS NOT NULL")
    conn.commit()
    conn.close()
    add_log(f"已永久清空回收站：{count} 条", "warn")
    return {"success": True, "message": f"已永久删除 {count} 条回收站资产", "deleted_count": count}



@app.post("/api/maintenance/calibrate_status")
def calibrate_status(user: Dict[str, object] = Depends(require_user)):
    result = auto_purify_expired_countdowns("manual")

    return {
        "success": True,
        "message": f"状态校准完成：{result['converted']} 条倒计时结束资产已转为纯净可用，补全到期时间 {result['filled_expire_at']} 条。",
        "result": result,
    }


@app.post("/api/maintenance/cleanup")
def cleanup_maintenance(user: Dict[str, object] = Depends(require_user)):
    conn = get_conn()

    old_logs = conn.execute("SELECT COUNT(*) FROM app_logs").fetchone()[0]
    old_runs = conn.execute("SELECT COUNT(*) FROM sync_runs").fetchone()[0]

    conn.execute("""
        DELETE FROM app_logs
        WHERE id NOT IN (
            SELECT id FROM app_logs ORDER BY id DESC LIMIT 3000
        )
    """)

    conn.execute("""
        DELETE FROM sync_runs
        WHERE id NOT IN (
            SELECT id FROM sync_runs ORDER BY id DESC LIMIT 500
        )
    """)

    conn.commit()

    new_logs = conn.execute("SELECT COUNT(*) FROM app_logs").fetchone()[0]
    new_runs = conn.execute("SELECT COUNT(*) FROM sync_runs").fetchone()[0]

    conn.close()

    deleted_logs = max(0, old_logs - new_logs)
    deleted_runs = max(0, old_runs - new_runs)

    add_log(f"维护清理完成：删除旧日志 {deleted_logs} 条，删除旧同步历史 {deleted_runs} 条", "warn")

    return {
        "success": True,
        "message": f"清理完成：删除旧日志 {deleted_logs} 条，删除旧同步历史 {deleted_runs} 条",
        "deleted_logs": deleted_logs,
        "deleted_runs": deleted_runs,
    }


@app.get("/api/backups")
def list_backups(user: Dict[str, object] = Depends(require_user)):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    items = []

    for path in sorted(BACKUP_DIR.glob("*.tar.gz"), key=lambda x: x.stat().st_mtime, reverse=True):
        items.append({
            "filename": path.name,
            "size": path.stat().st_size,
            "modified_at": datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })

    return {"data": items}


@app.post("/api/backups/create")
def api_create_backup(user: Dict[str, object] = Depends(require_user)):
    path = create_backup_file()
    return {"success": True, "message": "备份已创建", "filename": path.name}


@app.post("/api/backups/merge_local")
def api_merge_local_backup(payload: BackupMergeRequest, user: Dict[str, object] = Depends(require_user)):
    filename = Path(payload.filename).name
    backup_path = BACKUP_DIR / filename

    if not backup_path.exists():
        return JSONResponse({"success": False, "message": "备份文件不存在"}, status_code=404)

    try:
        db_bytes = find_db_in_tar(backup_path)
        result = merge_backup_db_bytes(db_bytes)
    except Exception as exc:
        return JSONResponse({"success": False, "message": f"合并失败：{exc}"}, status_code=400)

    add_log(f"已合并本地备份：{filename}，资产新增 {result['assets']} 条，数据源新增 {result['sources']} 个", "info")
    return {"success": True, "message": "备份合并完成", "result": result}


@app.post("/api/backups/merge_upload")
async def api_merge_upload_backup(file: UploadFile = File(...), user: Dict[str, object] = Depends(require_user)):
    if not file.filename.endswith(".tar.gz"):
        return JSONResponse({"success": False, "message": "仅支持 .tar.gz 备份包"}, status_code=400)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_file = Path(tmp) / Path(file.filename).name
        content = await file.read()
        if len(content) > MAX_BACKUP_UPLOAD_BYTES:
            return JSONResponse(
                {"success": False, "message": f"备份包不能超过 {MAX_BACKUP_UPLOAD_BYTES // 1024 // 1024} MB"},
                status_code=413,
            )
        tmp_file.write_bytes(content)

        try:
            db_bytes = find_db_in_tar(tmp_file)
            result = merge_backup_db_bytes(db_bytes)
        except Exception as exc:
            return JSONResponse({"success": False, "message": f"合并失败：{exc}"}, status_code=400)

    add_log(f"已合并上传备份：{file.filename}，资产新增 {result['assets']} 条，数据源新增 {result['sources']} 个", "info")
    return {"success": True, "message": "上传备份合并完成", "result": result}



@app.get("/api/backups/download/{filename}")
def api_download_backup(filename: str, user: Dict[str, object] = Depends(require_user)):
    safe_name = Path(filename).name
    backup_path = BACKUP_DIR / safe_name

    if not backup_path.exists() or not backup_path.is_file():
        return JSONResponse({"success": False, "message": "备份文件不存在"}, status_code=404)

    add_log(f"下载备份：{safe_name}", "info")
    return FileResponse(
        path=str(backup_path),
        filename=safe_name,
        media_type="application/gzip",
    )


@app.delete("/api/backups/{filename}")
def api_delete_backup(filename: str, user: Dict[str, object] = Depends(require_user)):
    safe_name = Path(filename).name
    backup_path = BACKUP_DIR / safe_name

    if not backup_path.exists() or not backup_path.is_file():
        return JSONResponse({"success": False, "message": "备份文件不存在"}, status_code=404)

    backup_path.unlink()
    add_log(f"删除本地备份：{safe_name}", "warn")
    return {"success": True, "message": "备份已删除"}


@app.get("/", response_class=HTMLResponse)
def read_root(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    return HTMLResponse(render_dashboard_page(str(user["username"])))


def render_auth_page(mode: str, error: str = "") -> str:
    is_login = mode == "login"
    title = "登录" if is_login else "注册"
    subtitle = "登录后进入资产智能管控台" if is_login else "创建账号后即可使用资产智能管控台"
    action = "/login" if is_login else "/register"
    button = "登录控制台" if is_login else "创建账号"
    switch_url = "/register" if is_login else "/login"
    switch_text = "还没有账号？立即注册" if is_login else "已有账号？返回登录"

    confirm = ""
    if not is_login:
        confirm = '<label>确认密码</label><input name="confirm_password" type="password" placeholder="再次输入密码" required minlength="8" autocomplete="new-password">'

    error_html = ""
    if error:
        error_html = f'<div class="error">{html_escape(error)}</div>'

    html = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;color:#172033;background:#eef3f8}
.card{width:min(430px,100%);background:#fff;border:1px solid #e5e7eb;border-radius:16px;box-shadow:0 18px 45px rgba(15,23,42,.12);overflow:hidden}
.head{padding:32px 32px 18px;border-bottom:1px solid #eef2f7}
.mark{width:48px;height:48px;border-radius:12px;display:grid;place-items:center;background:#1677ff;color:white;font-size:24px;margin-bottom:16px}
h1{margin:0;font-size:26px}
p{margin:10px 0 0;color:#667085;line-height:1.7}
form{padding:24px 32px 32px}
label{display:block;margin:14px 0 8px;font-size:13px;font-weight:700;color:#334155}
input{width:100%;height:46px;padding:0 14px;border:1px solid #cbd5e1;border-radius:10px;font-size:15px;outline:none}
input:focus{border-color:#1677ff;box-shadow:0 0 0 3px rgba(22,119,255,.12)}
button{width:100%;height:48px;margin-top:22px;border:0;border-radius:10px;background:#1677ff;color:white;font-weight:800;cursor:pointer}
.switch{text-align:center;margin-top:18px}
.switch a{color:#1677ff;font-weight:800;text-decoration:none}
.error{margin:18px 32px 0;padding:12px 14px;border-radius:10px;background:#fff1f0;border:1px solid #ffccc7;color:#a8071a}
.hint{margin-top:14px;padding:12px;border-radius:10px;background:#f8fafc;color:#64748b;font-size:13px;line-height:1.7}


/* v4 mobile asset-first layout */
.mobile-header{display:none}
.mobile-menu-btn{border:0;background:#1677ff;color:#fff;border-radius:12px;height:42px;min-width:42px;padding:0 12px;font-weight:900;font-size:18px;cursor:pointer}
.mobile-overlay{display:none}
.mobile-only{display:none}

@media(max-width:768px){
  .app{display:block}
  .mobile-header{display:flex;align-items:center;justify-content:space-between;gap:10px;background:#fff;border-bottom:1px solid var(--line);padding:10px 12px;position:sticky;top:0;z-index:30}
  .mobile-header .mh-title{font-weight:900;font-size:17px;color:#101828;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .mobile-header .mh-sub{font-size:12px;color:#667085;margin-top:2px}
  .mobile-only{display:block}
  .mobile-overlay{position:fixed;inset:0;background:rgba(15,23,42,.45);z-index:40;display:none}
  body.drawer-open .mobile-overlay{display:block}
  .sidebar{position:fixed;left:0;top:0;bottom:0;width:min(82vw,320px);height:100vh;z-index:50;transform:translateX(-105%);transition:transform .22s ease;overflow:auto;border-radius:0 18px 18px 0;box-shadow:16px 0 38px rgba(15,23,42,.22)}
  body.drawer-open .sidebar{transform:translateX(0)}
  .brand{padding:10px 4px 18px}
  .nav{display:grid;grid-template-columns:1fr;gap:8px;overflow:visible;padding:0;margin:0}
  .nav button{width:100%;height:46px;text-align:left;padding:0 14px;border-radius:12px;min-width:0}
  .user{margin-top:18px}
  .topbar{display:none}
  .content{padding:12px}
  .stats{display:none}
  #statusMsg{display:none}
  .panel-head{margin-bottom:10px}
  #tab-assets .panel-head h2{font-size:17px}
  #tab-assets .panel-head .actions{grid-template-columns:1fr 1fr;width:100%;margin-top:8px}
  .asset-tools{grid-template-columns:1fr;gap:8px;margin-bottom:10px}
  .asset-tools input{display:none}
  .asset-tools button{display:none}
  #sourceFilter{height:48px;font-weight:800;background:#fff;border-radius:14px}
  .source-summary{grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px;background:#fff}
  .source-summary strong{grid-column:1/-1;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .source-summary .mini{font-size:12px;padding:7px 6px}
  .status-tabs{grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;margin-bottom:10px}
  .status-tabs button{height:36px;border-radius:12px;font-size:12px;padding:0 4px}
  .status-tabs button span{display:block;font-size:11px;font-weight:900;margin-top:1px}
  .advanced{display:none}
  #addressList{gap:8px}
  .record{padding:10px 11px;border-radius:13px;box-shadow:0 4px 14px rgba(15,23,42,.04)}
  .record-select{align-items:center;gap:8px}
  .record-select input{width:17px;min-height:17px;margin-top:0}
  .record-head{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:8px;width:100%}
  .addr{font-size:15px;line-height:1.45;font-weight:900;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;word-break:break-all}
  .badge{font-size:12px;padding:5px 8px;border-radius:999px;white-space:nowrap}
  .record .meta{display:none}
  .record .mobile-meta{display:flex;gap:6px;flex-wrap:wrap;margin-top:7px;color:#667085;font-size:11px;line-height:1.4}
  .record .mobile-meta span{background:#f8fafc;border:1px solid #eef2f7;border-radius:999px;padding:3px 7px;max-width:100%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .pagination{position:sticky;bottom:0;background:rgba(245,247,251,.97);padding:9px 0;margin-top:10px}
  .pagination .btn{min-width:72px;height:38px}
  #pageInfo{font-size:12px;white-space:nowrap;color:#667085}
}

@media(max-width:380px){
  .status-tabs{grid-template-columns:repeat(2,minmax(0,1fr))}
  #tab-assets .panel-head .actions{grid-template-columns:1fr}
  .source-summary{grid-template-columns:1fr 1fr}
}


/* v17 health monitor + v17 PC table polish merged */
.source-badges{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.source-health-meta .health-detail{flex-basis:100%;font-weight:700}
@media (min-width:641px){
  #addressList.asset-table{display:flex!important;flex-direction:column!important;gap:8px!important;margin-top:14px!important}
  #addressList.asset-table .record{display:block!important;padding:0!important;border:0!important;background:transparent!important;box-shadow:none!important}
  #addressList.asset-table .record-select{display:block!important;width:100%!important}
  #addressList.asset-table .record-body{display:block!important;background:#fff!important;border:1px solid #e6edf6!important;border-radius:14px!important;padding:11px 13px!important;box-shadow:0 3px 12px rgba(15,23,42,.035)!important}
  #addressList.asset-table .record-body:hover{border-color:#cbd5e1!important;box-shadow:0 8px 22px rgba(15,23,42,.06)!important}
  #addressList.asset-table .record-head{display:grid!important;grid-template-columns:26px minmax(0,1fr) 72px 70px!important;gap:10px!important;align-items:center!important;width:100%!important;background:transparent!important;border:0!important;padding:0!important}
  #addressList.asset-table .record-check{display:block!important;width:18px!important;height:18px!important;margin:0!important;justify-self:center!important;accent-color:#1677ff!important}
  #addressList.asset-table .addr{font-size:15px!important;line-height:1.45!important;font-weight:800!important;color:#101828!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
  #addressList.asset-table .copy-btn{height:30px!important;min-width:60px!important;padding:0 10px!important;border-radius:999px!important;justify-self:end!important}
  #addressList.asset-table .badge{height:30px!important;min-width:56px!important;padding:0 12px!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;border-radius:999px!important;font-size:13px!important;font-weight:900!important;line-height:1!important;margin:0!important;justify-self:end!important}
  #addressList.asset-table .meta{display:flex!important;align-items:center!important;gap:8px 16px!important;margin:8px 0 0 36px!important;padding:0!important;font-size:12px!important;color:#667085!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
  #addressList.asset-table .mobile-meta{display:none!important}
  .advanced{margin-top:8px!important}
  .source-summary{margin-top:12px!important}
}
@media (max-width:640px){
  #addressList.asset-table .record-check{display:none!important}
}

</style>
</head>
<body>
<section class="card">
<div class="head">
<div class="mark">资</div>
<h1>__TITLE__</h1>
<p>__SUBTITLE__</p>
</div>
__ERROR__
<form method="post" action="__ACTION__">
<label>用户名</label>
<input name="username" type="text" placeholder="请输入用户名" required minlength="3" maxlength="32" autocomplete="username">
<label>密码</label>
<input name="password" type="password" placeholder="请输入密码" required minlength="8" autocomplete="current-password">
__CONFIRM__
<button type="submit">__BUTTON__</button>
<div class="switch"><a href="__SWITCH_URL__">__SWITCH_TEXT__</a></div>

</form>
</section>
</body>
</html>
"""

    return (
        html.replace("__TITLE__", html_escape(title))
        .replace("__SUBTITLE__", html_escape(subtitle))
        .replace("__ERROR__", error_html)
        .replace("__ACTION__", action)
        .replace("__CONFIRM__", confirm)
        .replace("__BUTTON__", html_escape(button))
        .replace("__SWITCH_URL__", switch_url)
        .replace("__SWITCH_TEXT__", html_escape(switch_text))
    )


def render_dashboard_page(username: str) -> str:
    html = r"""
<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>资产智能管控台</title>
<style>
*{box-sizing:border-box}
:root{
--bg:#f5f7fb;--card:#fff;--text:#1f2937;--muted:#667085;--line:#e5e7eb;
--blue:#1677ff;--green:#16a34a;--red:#dc2626;--orange:#d97706;
--shadow:0 10px 26px rgba(15,23,42,.06)
}
body{margin:0;min-height:100vh;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;color:var(--text);background:var(--bg)}
.app{display:grid;grid-template-columns:220px 1fr;min-height:100vh}
.sidebar{background:#101828;color:#e5e7eb;padding:18px;position:sticky;top:0;height:100vh}
.brand{display:flex;align-items:center;gap:10px;padding:6px 4px 18px}
.logo{width:38px;height:38px;border-radius:10px;background:#1677ff;display:grid;place-items:center;color:#fff;font-weight:900}
.brand b{display:block;color:#fff;font-size:15px}
.brand span{display:block;color:#98a2b3;font-size:12px;margin-top:2px}
.nav{display:grid;gap:8px}
.nav button{height:42px;border:0;border-radius:10px;background:transparent;color:#d0d5dd;text-align:left;padding:0 12px;font-weight:700;cursor:pointer}
.nav button.active,.nav button:hover{background:#1d2939;color:white}
.user{position:absolute;left:18px;right:18px;bottom:18px;background:#1d2939;border-radius:12px;padding:12px}
.user-name{font-weight:800;color:white;word-break:break-all}
.user button{width:100%;height:38px;border:0;border-radius:9px;background:#344054;color:white;margin-top:10px;cursor:pointer}
.main{min-width:0}
.topbar{height:64px;background:white;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 22px;position:sticky;top:0;z-index:5}
.topbar h1{font-size:18px;margin:0}
.topbar .desc{font-size:12px;color:var(--muted);margin-top:3px}
.content{padding:20px}
.tab{display:none}
.tab.active{display:block}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:14px}
.card,.panel{background:var(--card);border:1px solid var(--line);border-radius:14px;box-shadow:var(--shadow)}
.stat{padding:16px}
.stat span{color:var(--muted);font-size:13px;font-weight:700}
.stat strong{display:block;font-size:26px;margin-top:8px}
.panel{padding:16px;margin-bottom:14px}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}
.panel-head h2{margin:0;font-size:16px}
.actions{display:flex;gap:8px;flex-wrap:wrap}
.btn{height:40px;border:1px solid var(--line);background:white;border-radius:10px;padding:0 14px;font-weight:700;cursor:pointer}
.btn.primary{background:#1677ff;border-color:#1677ff;color:white}
.btn.danger{background:#fff1f0;border-color:#ffccc7;color:#a8071a}
.btn.warn{background:#fff7ed;border-color:#fed7aa;color:#9a3412}
.btn:disabled{opacity:.55;cursor:not-allowed}
.notice{padding:12px;border-radius:10px;background:#f8fafc;color:#667085;line-height:1.7;font-size:13px;white-space:pre-wrap}
.notice.ok{background:#f0fdf4;color:#166534;border:1px solid #bbf7d0}
.notice.bad{background:#fff1f0;color:#a8071a;border:1px solid #ffccc7}
.notice.warn{background:#fffbeb;color:#92400e;border:1px solid #fde68a}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.full{grid-column:1/-1}
label{display:block;font-size:13px;font-weight:700;color:#344054;margin-bottom:7px}
input,select,textarea{width:100%;min-height:40px;border:1px solid #d0d5dd;border-radius:10px;background:white;padding:0 12px;outline:none;font-family:inherit}
textarea{min-height:86px;padding:10px 12px;resize:vertical;line-height:1.5;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
input:focus,select:focus,textarea:focus{border-color:#1677ff;box-shadow:0 0 0 3px rgba(22,119,255,.1)}
.quick-grid{display:grid;grid-template-columns:1.4fr 1fr auto;gap:10px}
.source-layout{display:grid;grid-template-columns:360px 1fr;gap:14px}
.source-list,.history-list,.backup-list{display:grid;gap:10px}
.item{border:1px solid var(--line);border-radius:12px;padding:12px;background:white}
.item:hover{border-color:#bcd7ff}
.item-head{display:flex;justify-content:space-between;gap:10px}
.item-title{font-weight:900;word-break:break-all}
.item-meta{display:flex;flex-wrap:wrap;gap:8px 12px;color:var(--muted);font-size:12px;margin-top:8px}
.badge{display:inline-flex;align-items:center;padding:5px 9px;border-radius:999px;font-size:12px;font-weight:800;white-space:nowrap}
.safe{background:#dcfce7;color:#166534}
.danger{background:#fee2e2;color:#991b1b}
.unknown{background:#fef3c7;color:#92400e}
.running{background:#dbeafe;color:#1d4ed8}
.asset-tools{display:grid;grid-template-columns:1.2fr 1fr auto;gap:10px;margin-bottom:12px}
.status-tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.status-tabs button{border:1px solid var(--line);background:white;border-radius:999px;height:34px;padding:0 14px;font-weight:800;cursor:pointer}
.status-tabs button.active{border-color:#1677ff;background:#eaf3ff;color:#0958d9}
.advanced{margin-bottom:12px}
.advanced summary{cursor:pointer;color:#1677ff;font-weight:800;font-size:13px;margin-bottom:10px}
.advanced-box{display:grid;grid-template-columns:1fr 1fr auto auto;gap:10px}
.list{list-style:none;margin:0;padding:0;display:grid;gap:10px}
.record{padding:14px;border:1px solid var(--line);border-radius:12px;background:#fff}
.record-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}
.addr{font-weight:800;line-height:1.6;word-break:break-all}
.meta{margin-top:8px;color:#667085;font-size:12px;display:flex;flex-wrap:wrap;gap:8px 12px}
.pagination{display:flex;justify-content:center;align-items:center;gap:10px;margin-top:14px;flex-wrap:wrap}
.page-jump{display:flex;align-items:center;gap:6px}
.page-jump input{width:76px;height:40px;border:1px solid #d8e0ea;border-radius:12px;padding:0 10px;font-weight:800;text-align:center}
.page-jump .btn{min-width:64px}
.empty{padding:28px;border:1px dashed #cbd5e1;border-radius:12px;text-align:center;color:#667085;background:#fff}
.progress-wrap{height:10px;background:#e5e7eb;border-radius:999px;overflow:hidden;margin-top:10px}
.progress-bar{height:100%;width:0;background:#1677ff;transition:width .25s ease}
.logbox{max-height:300px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;line-height:1.7}
details.clean{border:1px solid var(--line);border-radius:12px;padding:12px;background:#fff}
details.clean summary{cursor:pointer;font-weight:900;color:#344054}
.source-summary{display:grid;grid-template-columns:1.4fr repeat(4,auto);gap:10px;align-items:center;margin-bottom:12px;padding:12px;border:1px solid var(--line);border-radius:12px;background:#f8fafc;color:#344054}
.source-summary strong{font-size:14px;color:#101828}
.source-summary .mini{display:inline-flex;gap:6px;align-items:center;border:1px solid var(--line);background:white;border-radius:999px;padding:6px 10px;font-size:12px;font-weight:800;white-space:nowrap}
.record-select{display:flex;align-items:flex-start;gap:10px;width:100%}
.record-select input{width:18px;min-height:18px;margin-top:5px;flex:0 0 auto}
.record-body{min-width:0;flex:1}
.record .source-label{font-weight:800;color:#475467}
@media(max-width:1100px){
.app{grid-template-columns:1fr}
.sidebar{position:relative;height:auto}
.nav{grid-template-columns:repeat(3,1fr)}
.nav button{text-align:center}
.user{position:static;margin-top:12px}
.source-layout{grid-template-columns:1fr}
.stats{grid-template-columns:repeat(2,1fr)}
}
@media(max-width:700px){
.content{padding:12px}
.topbar{height:auto;display:block;padding:14px}
.sidebar{padding:12px}
.nav{grid-template-columns:repeat(2,1fr)}
.stats{gap:10px}
.stat{padding:13px}
.stat strong{font-size:22px}
.panel{padding:13px;border-radius:12px}
.panel-head{display:block}
.actions{display:grid;grid-template-columns:1fr;gap:8px;margin-top:10px}
.btn{width:100%}
.quick-grid,.asset-tools,.advanced-box,.form-grid{grid-template-columns:1fr}
.record-head,.item-head{display:block}
.badge{margin-top:8px}
.meta,.item-meta{display:grid}
}
/* v3 mobile responsive hotfix: prevent horizontal overflow on all phones */
html,body{width:100%;max-width:100%;overflow-x:hidden}
.app,.main,.content,.panel,.card,.topbar,.sidebar{min-width:0;max-width:100%}
input,select,textarea,button{max-width:100%;min-width:0}
*{min-width:0}

@media(max-width:768px){
  body{background:#f5f7fb}
  .app{display:block;width:100%;overflow-x:hidden}
  .sidebar{position:relative;height:auto;width:100%;padding:14px 14px 16px;border-radius:0;overflow:hidden}
  .brand{padding:0 0 12px;align-items:center}
  .logo{width:44px;height:44px;border-radius:12px;flex:0 0 auto}
  .brand b{font-size:17px;line-height:1.3}
  .brand span{font-size:13px}
  .nav{display:flex;gap:8px;overflow-x:auto;padding:4px 0 8px;margin:0 -2px;scroll-snap-type:x proximity;-webkit-overflow-scrolling:touch}
  .nav::-webkit-scrollbar{display:none}
  .nav button{flex:0 0 auto;width:auto;min-width:96px;height:40px;text-align:center;padding:0 14px;border-radius:12px;white-space:nowrap;scroll-snap-align:start}
  .user{position:static;margin-top:10px;padding:10px;border-radius:12px}
  .user-name{font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .user form{margin:8px 0 0}
  .user button{height:38px;margin-top:0}

  .topbar{position:relative;height:auto;display:block;padding:16px 16px 12px;border-bottom:1px solid var(--line)}
  .topbar h1{font-size:22px;line-height:1.25;margin-bottom:6px}
  .topbar .desc,#dbInfo{display:block;font-size:12px;line-height:1.6;white-space:normal;word-break:break-word}
  .content{padding:12px;width:100%;overflow:hidden}

  .stats{grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-bottom:12px}
  .stat{padding:14px 16px;border-radius:14px}
  .stat span{font-size:13px;line-height:1.4}
  .stat strong{font-size:24px;line-height:1.2;margin-top:8px}

  .panel{padding:14px;border-radius:14px;margin-bottom:12px;overflow:hidden}
  .panel-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:12px}
  .panel-head h2{font-size:18px;line-height:1.3}
  .actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;width:100%;margin-top:4px}
  .btn{height:42px;width:100%;padding:0 10px;border-radius:12px;font-size:14px;white-space:nowrap}

  .quick-grid,.asset-tools,.advanced-box,.form-grid{display:grid;grid-template-columns:1fr;gap:10px;width:100%}
  .source-layout{display:block;width:100%}
  .source-list,.history-list,.backup-list,.list{gap:10px;width:100%}
  .item,.record{padding:12px;border-radius:14px;width:100%;overflow:hidden}
  .item-head,.record-head{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;flex-wrap:wrap}
  .item-title,.addr{font-size:15px;line-height:1.55;word-break:break-all;overflow-wrap:anywhere}
  .item-meta,.meta{display:flex;flex-wrap:wrap;gap:6px 10px;font-size:12px;line-height:1.55;word-break:break-word;overflow-wrap:anywhere}
  .badge{margin-top:0;flex:0 0 auto}

  .source-summary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;padding:10px;border-radius:14px;overflow:hidden}
  .source-summary strong{grid-column:1/-1;font-size:14px;line-height:1.5;word-break:break-word;overflow-wrap:anywhere}
  .source-summary .mini{justify-content:center;width:100%;padding:7px 8px;white-space:normal;text-align:center;border-radius:12px}

  .status-tabs{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;width:100%}
  .status-tabs button{height:38px;width:100%;padding:0 8px;white-space:nowrap}

  select,input{height:44px;font-size:16px;border-radius:12px}
  textarea{font-size:14px;border-radius:12px}
  .pagination{position:sticky;bottom:0;z-index:4;background:rgba(245,247,251,.96);backdrop-filter:blur(8px);padding:10px 0;margin:10px -4px 0;gap:8px}
  .pagination .btn{width:auto;min-width:82px}
  .empty{padding:20px 12px;border-radius:14px}
}

@media(max-width:390px){
  .content{padding:10px}
  .sidebar{padding:12px}
  .stats{grid-template-columns:1fr}
  .actions{grid-template-columns:1fr}
  .source-summary{grid-template-columns:1fr}
  .status-tabs{grid-template-columns:1fr 1fr}
  .nav button{min-width:88px;padding:0 12px}
}



/* v5/v6 mobile asset-only cleanup: real drawer + minimal asset list */
@media(max-width:768px){
  html,body{width:100%;max-width:100%;overflow-x:hidden;background:#f5f7fb}
  .mobile-header{display:flex!important;position:sticky;top:0;z-index:60;height:54px;align-items:center;gap:10px;background:#fff;border-bottom:1px solid var(--line);padding:8px 12px;box-shadow:0 2px 10px rgba(15,23,42,.04)}
  .mobile-menu-btn{display:inline-grid;place-items:center;width:38px;height:38px;min-width:38px;padding:0;border-radius:12px;border:0;background:#1677ff;color:#fff;font-size:18px;font-weight:900}
  .mobile-header .mh-title{font-size:17px;line-height:1.1;font-weight:900;color:#101828}
  .mobile-header .mh-sub{display:none}
  .mobile-overlay{position:fixed!important;inset:0;background:rgba(15,23,42,.48);z-index:70;display:none!important}
  body.drawer-open .mobile-overlay{display:block!important}
  .app{display:block!important;width:100%!important;min-height:auto!important;overflow-x:hidden!important}
  .sidebar{position:fixed!important;top:0!important;left:0!important;bottom:0!important;width:min(82vw,300px)!important;height:100dvh!important;z-index:80!important;transform:translateX(-105%)!important;transition:transform .22s ease!important;border-radius:0 18px 18px 0!important;box-shadow:16px 0 38px rgba(15,23,42,.28)!important;padding:14px!important;overflow:auto!important;background:#101828!important}
  body.drawer-open .sidebar{transform:translateX(0)!important}
  .sidebar .mobile-only{display:block!important;width:100%;height:42px;margin:0 0 14px!important;border-radius:14px;background:#fff;color:#101828}
  .brand{padding:0 0 14px!important}
  .nav{display:grid!important;grid-template-columns:1fr!important;gap:8px!important;overflow:visible!important;margin:0!important;padding:0!important}
  .nav button{width:100%!important;min-width:0!important;height:44px!important;text-align:left!important;padding:0 14px!important;border-radius:12px!important}
  .user{position:static!important;margin-top:16px!important}
  .main{width:100%!important;min-width:0!important}
  .topbar,.stats,#statusMsg{display:none!important}
  .content{padding:10px!important;width:100%!important;overflow:hidden!important}
  .panel{box-shadow:none!important;border:0!important;background:transparent!important;padding:0!important;margin:0!important;border-radius:0!important}
  #tab-assets .panel-head{display:none!important}
  .asset-tools{display:block!important;margin:0 0 10px!important;width:100%!important}
  .asset-tools input,.asset-tools button{display:none!important}
  #sourceFilter{display:block!important;width:100%!important;height:48px!important;border-radius:14px!important;border:1px solid #d0d5dd!important;background:#fff!important;padding:0 12px!important;font-size:15px!important;font-weight:800!important;color:#101828!important;box-shadow:0 4px 14px rgba(15,23,42,.04)!important}
  #sourceSummary{display:none!important}
  .status-tabs{display:grid!important;grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:8px!important;margin:0 0 10px!important}
  .status-tabs button{height:38px!important;border-radius:999px!important;background:#fff!important;border:1px solid #e5e7eb!important;font-size:14px!important;font-weight:900!important;padding:0 8px!important}
  .status-tabs button.active{border-color:#1677ff!important;background:#eaf3ff!important;color:#0958d9!important}
  .status-tabs button span{display:inline!important;margin-left:4px!important}
  .advanced{display:none!important}
  #addressList{display:grid!important;gap:8px!important;margin:0!important;padding:0!important}
  .record{list-style:none!important;background:#fff!important;border:1px solid #e5e7eb!important;border-radius:14px!important;padding:10px 12px!important;box-shadow:0 4px 14px rgba(15,23,42,.04)!important}
  .record-select{display:block!important}
  .record-check{display:none!important}
  .record-body{width:100%!important}
  .record-head{display:grid!important;grid-template-columns:minmax(0,1fr) auto!important;align-items:center!important;gap:8px!important;width:100%!important}
  .addr{font-size:15px!important;line-height:1.45!important;font-weight:900!important;color:#101828!important;word-break:break-all!important;overflow-wrap:anywhere!important;display:-webkit-box!important;-webkit-line-clamp:2!important;-webkit-box-orient:vertical!important;overflow:hidden!important}
  .badge{font-size:13px!important;line-height:1!important;padding:6px 9px!important;border-radius:999px!important;white-space:nowrap!important;font-weight:900!important}
  .record .meta,.record .mobile-meta{display:none!important}
  .pagination{position:sticky!important;bottom:0!important;z-index:20!important;background:rgba(245,247,251,.96)!important;backdrop-filter:blur(8px)!important;display:grid!important;grid-template-columns:78px 1fr 78px!important;align-items:center!important;gap:8px!important;margin:8px -10px 0!important;padding:9px 10px!important;border-top:1px solid #e5e7eb!important}
  .pagination .btn{height:38px!important;width:100%!important;min-width:0!important;border-radius:12px!important}
  #pageInfo{text-align:center!important;font-size:12px!important;color:#667085!important;white-space:nowrap!important}
}

@media(max-width:360px){
  .status-tabs{grid-template-columns:1fr 1fr!important}
  .addr{font-size:14px!important}
}




/* v6 desktop restore + mobile asset copy polish */
.copy-btn{border:1px solid #d0d5dd;background:#fff;color:#1677ff;border-radius:999px;height:30px;padding:0 10px;font-weight:900;cursor:pointer;white-space:nowrap;box-shadow:0 2px 8px rgba(15,23,42,.04)}
.copy-btn:hover{border-color:#1677ff;background:#eef6ff}
.addr-row{display:flex;align-items:flex-start;gap:8px;min-width:0;flex:1}
@media(min-width:769px){
  .mobile-header,.mobile-overlay,.mobile-only{display:none!important}
  .app{display:grid!important;grid-template-columns:220px minmax(0,1fr)!important;min-height:100vh!important}
  .sidebar{position:sticky!important;top:0!important;height:100vh!important;width:auto!important;transform:none!important;border-radius:0!important;box-shadow:none!important;overflow:auto!important;background:#101828!important;padding:18px!important}
  .main{display:block!important;min-width:0!important;width:100%!important;background:var(--bg)!important}
  .content{padding:20px!important;display:block!important}
  .topbar{display:flex!important}
  .stats{display:grid!important}
  .panel{display:block!important;background:var(--card)!important;border:1px solid var(--line)!important;border-radius:14px!important;padding:16px!important;box-shadow:var(--shadow)!important;margin-bottom:14px!important}
  #tab-assets .panel-head{display:flex!important}
  .asset-tools{display:grid!important;grid-template-columns:1.2fr 1fr auto!important;gap:10px!important;margin-bottom:12px!important}
  .asset-tools input,.asset-tools button{display:block!important}
  .status-tabs{display:flex!important;gap:8px!important;flex-wrap:wrap!important}
  .advanced{display:block!important}
  .record .meta{display:flex!important}
  .record-check{display:block!important}
}
@media(max-width:768px){
  body{background:#f1f5f9!important}
  .mobile-header{border-radius:0 0 16px 16px!important}
  .content{padding:8px!important}
  .asset-tools{margin-top:2px!important}
  #sourceFilter{height:44px!important;border-radius:12px!important;font-size:14px!important;box-shadow:0 2px 8px rgba(15,23,42,.04)!important}
  .status-tabs{grid-template-columns:repeat(4,minmax(0,1fr))!important;gap:6px!important;margin-bottom:8px!important}
  .status-tabs button{height:34px!important;font-size:12px!important;padding:0 4px!important}
  .record{padding:9px 10px!important;border-radius:12px!important;margin:0!important}
  .record-head{grid-template-columns:minmax(0,1fr) auto auto!important;gap:6px!important}
  .addr{font-size:14px!important;line-height:1.4!important;-webkit-line-clamp:2!important}
  .badge{font-size:12px!important;padding:5px 8px!important}
  .copy-btn{height:28px!important;padding:0 9px!important;font-size:12px!important;background:#f8fafc!important}
  .pagination{margin:8px -8px 0!important;padding:8px!important}
}
@media(max-width:390px){
  .status-tabs{grid-template-columns:repeat(2,minmax(0,1fr))!important}
}


/* v7 layout polish: true phone-only mobile mode, restored desktop UI */
@media (min-width:641px){
  .mobile-header,.mobile-overlay,.mobile-only{display:none!important}
  body{background:#f5f7fb!important;overflow-x:hidden!important}
  .app{display:grid!important;grid-template-columns:248px minmax(0,1fr)!important;min-height:100vh!important;width:100%!important;overflow:visible!important}
  .sidebar{position:sticky!important;top:0!important;height:100vh!important;width:248px!important;transform:none!important;border-radius:0!important;box-shadow:none!important;overflow:auto!important;background:#101828!important;padding:20px 18px!important}
  .brand{padding:4px 4px 22px!important}
  .brand b{font-size:16px!important}
  .nav{display:grid!important;grid-template-columns:1fr!important;gap:8px!important;overflow:visible!important;margin:0!important;padding:0!important}
  .nav button{width:100%!important;height:44px!important;min-width:0!important;border-radius:12px!important;text-align:left!important;padding:0 14px!important}
  .user{position:absolute!important;left:18px!important;right:18px!important;bottom:18px!important;margin:0!important;background:#1d2939!important}
  .main{min-width:0!important;width:100%!important;background:#f5f7fb!important;display:block!important}
  .topbar{display:flex!important;height:68px!important;background:#fff!important;border-bottom:1px solid #e5e7eb!important;padding:0 26px!important}
  .stats{display:grid!important;grid-template-columns:repeat(4,minmax(0,1fr))!important;gap:14px!important;margin-bottom:16px!important}
  .content{padding:22px 26px!important;display:block!important;max-width:100%!important;overflow:visible!important}
  .panel{display:block!important;background:#fff!important;border:1px solid #e5e7eb!important;border-radius:18px!important;padding:18px!important;box-shadow:0 12px 28px rgba(15,23,42,.05)!important;margin-bottom:16px!important}
  #tab-assets .panel-head{display:flex!important;margin-bottom:16px!important}
  .asset-tools{display:grid!important;grid-template-columns:minmax(260px,1fr) minmax(360px,.85fr) 76px!important;gap:12px!important;align-items:center!important;margin-bottom:14px!important}
  .asset-tools input,.asset-tools select{height:46px!important;border-radius:14px!important;font-size:15px!important}
  .asset-tools button{display:block!important;height:46px!important;border-radius:14px!important}
  .source-summary{display:flex!important;align-items:center!important;gap:10px!important;flex-wrap:wrap!important;margin-bottom:14px!important;padding:12px 14px!important;border:1px solid #e8eef6!important;border-radius:16px!important;background:#f8fafc!important}
  .source-summary strong{margin-right:auto!important;font-size:15px!important;color:#101828!important;min-width:220px!important}
  .source-summary .mini{border-radius:999px!important;background:#fff!important;border:1px solid #e5e7eb!important;padding:7px 12px!important;font-size:13px!important}
  .status-tabs{display:flex!important;gap:10px!important;flex-wrap:wrap!important;margin:0 0 12px!important}
  .status-tabs button{height:38px!important;border-radius:999px!important;padding:0 18px!important;font-size:14px!important;background:#fff!important;border:1px solid #e5e7eb!important}
  .status-tabs button.active{background:#eef6ff!important;border-color:#1677ff!important;color:#0958d9!important;box-shadow:0 0 0 3px rgba(22,119,255,.08)!important}
  .advanced{display:block!important;margin-bottom:12px!important}
  .advanced-box{display:grid!important;grid-template-columns:220px 140px auto auto!important;gap:10px!important;align-items:center!important;background:#f8fafc!important;border:1px solid #edf2f7!important;border-radius:14px!important;padding:12px!important;margin-top:10px!important}
  .list{display:grid!important;gap:10px!important;margin:0!important;padding:0!important}
  .record{background:#fff!important;border:1px solid #e7edf5!important;border-radius:16px!important;padding:14px 16px!important;box-shadow:0 6px 18px rgba(15,23,42,.035)!important;transition:box-shadow .16s ease,transform .16s ease,border-color .16s ease!important}
  .record:hover{border-color:#cbd5e1!important;box-shadow:0 12px 28px rgba(15,23,42,.07)!important;transform:translateY(-1px)!important}
  .record-select{display:grid!important;grid-template-columns:26px minmax(0,1fr)!important;gap:12px!important;align-items:start!important;width:100%!important}
  .record-check{display:block!important;width:18px!important;height:18px!important;margin-top:4px!important}
  .record-body{min-width:0!important;width:100%!important}
  .record-head{display:grid!important;grid-template-columns:minmax(0,1fr) auto auto!important;gap:10px!important;align-items:start!important;width:100%!important}
  .addr{font-size:16px!important;line-height:1.55!important;font-weight:900!important;color:#0f172a!important;word-break:break-word!important;overflow-wrap:anywhere!important}
  .copy-btn{height:32px!important;padding:0 13px!important;border-radius:999px!important;background:#f8fafc!important;color:#1677ff!important;border:1px solid #d7e3f3!important;justify-self:end!important}
  .badge{height:30px!important;display:inline-flex!important;align-items:center!important;border-radius:999px!important;padding:0 12px!important;font-weight:900!important;white-space:nowrap!important}
  .meta{display:flex!important;flex-wrap:wrap!important;gap:8px 16px!important;margin-top:8px!important;color:#667085!important;font-size:12px!important;line-height:1.6!important}
  .mobile-meta{display:none!important}
  .pagination{display:flex!important;justify-content:center!important;align-items:center!important;gap:14px!important;margin-top:16px!important;padding-top:12px!important;border-top:1px solid #eef2f7!important;position:static!important;background:transparent!important}
  .pagination .btn{width:auto!important;min-width:92px!important;height:40px!important;border-radius:12px!important}
}
@media (max-width:640px){
  html,body{width:100%!important;max-width:100%!important;overflow-x:hidden!important;background:#eef2f7!important}
  .mobile-header{display:flex!important;position:sticky!important;top:0!important;z-index:60!important;height:52px!important;align-items:center!important;gap:10px!important;background:#ffffff!important;border-bottom:1px solid #e5e7eb!important;padding:8px 10px!important;box-shadow:0 2px 10px rgba(15,23,42,.04)!important}
  .mobile-menu-btn{width:38px!important;height:38px!important;border-radius:12px!important;background:#1677ff!important;color:#fff!important;border:0!important;font-weight:900!important}
  .mobile-header .mh-title{font-size:16px!important;font-weight:900!important;color:#101828!important}
  .mobile-header .mh-sub{display:none!important}
  .app{display:block!important;width:100%!important;min-height:auto!important;overflow:hidden!important}
  .sidebar{position:fixed!important;top:0!important;left:0!important;bottom:0!important;width:min(78vw,286px)!important;height:100dvh!important;z-index:80!important;transform:translateX(-105%)!important;transition:transform .2s ease!important;border-radius:0 18px 18px 0!important;background:#101828!important;box-shadow:16px 0 40px rgba(15,23,42,.25)!important;padding:14px!important;overflow:auto!important}
  body.drawer-open .sidebar{transform:translateX(0)!important}
  .mobile-overlay{position:fixed!important;inset:0!important;background:rgba(15,23,42,.48)!important;z-index:70!important;display:none!important}
  body.drawer-open .mobile-overlay{display:block!important}
  .sidebar .mobile-only{display:block!important;width:100%!important;height:42px!important;margin:0 0 14px!important;border-radius:14px!important;background:#fff!important;color:#101828!important}
  .topbar,.stats,#statusMsg,#tab-assets .panel-head,#sourceSummary,.advanced{display:none!important}
  .main{width:100%!important;min-width:0!important;background:#eef2f7!important}
  .content{padding:8px!important;width:100%!important;overflow:hidden!important}
  .panel{border:0!important;background:transparent!important;padding:0!important;margin:0!important;box-shadow:none!important;border-radius:0!important}
  .asset-tools{display:block!important;margin:0 0 8px!important;width:100%!important}
  .asset-tools input,.asset-tools button{display:none!important}
  #sourceFilter{display:block!important;width:100%!important;height:44px!important;border-radius:12px!important;border:1px solid #d8e0eb!important;background:#fff!important;padding:0 10px!important;font-size:14px!important;font-weight:800!important;color:#101828!important;box-shadow:0 2px 9px rgba(15,23,42,.04)!important}
  .status-tabs{display:grid!important;grid-template-columns:repeat(4,minmax(0,1fr))!important;gap:5px!important;margin:0 0 8px!important}
  .status-tabs button{height:32px!important;border-radius:999px!important;background:#fff!important;border:1px solid #e2e8f0!important;font-size:12px!important;font-weight:900!important;padding:0 3px!important;white-space:nowrap!important}
  .status-tabs button.active{border-color:#1677ff!important;background:#eaf3ff!important;color:#0958d9!important}
  .status-tabs button span{display:inline!important;margin-left:2px!important}
  #addressList{display:grid!important;gap:7px!important;margin:0!important;padding:0!important}
  .record{background:#fff!important;border:1px solid #e5e7eb!important;border-radius:12px!important;padding:9px 10px!important;box-shadow:0 3px 10px rgba(15,23,42,.035)!important}
  .record-select{display:block!important;width:100%!important}
  .record-check{display:none!important}
  .record-body{width:100%!important;min-width:0!important}
  .record-head{display:grid!important;grid-template-columns:minmax(0,1fr) auto auto!important;gap:6px!important;align-items:center!important;width:100%!important}
  .addr{font-size:14px!important;line-height:1.4!important;font-weight:900!important;color:#101828!important;word-break:break-word!important;overflow-wrap:anywhere!important;display:-webkit-box!important;-webkit-line-clamp:2!important;-webkit-box-orient:vertical!important;overflow:hidden!important}
  .copy-btn{height:28px!important;padding:0 8px!important;font-size:12px!important;border-radius:999px!important;background:#f8fafc!important;color:#1677ff!important;border:1px solid #d7e3f3!important}
  .badge{height:26px!important;font-size:12px!important;display:inline-flex!important;align-items:center!important;padding:0 8px!important;border-radius:999px!important;white-space:nowrap!important;font-weight:900!important}
  .record .meta,.record .mobile-meta{display:none!important}
  .pagination{position:sticky!important;bottom:0!important;z-index:20!important;background:rgba(238,242,247,.96)!important;backdrop-filter:blur(8px)!important;display:grid!important;grid-template-columns:78px 1fr 78px!important;align-items:center!important;gap:8px!important;margin:8px -8px 0!important;padding:8px!important;border-top:1px solid #e2e8f0!important}
  .pagination .btn{height:38px!important;width:100%!important;min-width:0!important;border-radius:12px!important;background:#fff!important}
  #pageInfo{text-align:center!important;font-size:12px!important;color:#667085!important;white-space:nowrap!important}
}
@media (max-width:380px){
  .status-tabs{grid-template-columns:repeat(2,minmax(0,1fr))!important}
}

/* v11 stability and comfort polish */
.copy-btn.copied{background:#dcfce7!important;border-color:#86efac!important;color:#166534!important}
.danger-text{color:#b42318!important;font-weight:800!important}
@media (min-width:641px){
  .asset-table{display:table!important;width:100%!important;border-collapse:separate!important;border-spacing:0 8px!important}
  .asset-table .record{display:table-row!important;background:transparent!important;box-shadow:none!important;border:0!important;padding:0!important}
  .asset-table .record-select{display:contents!important}
  .asset-table .record-body{display:contents!important}
  .asset-table .record-head{display:grid!important;grid-template-columns:minmax(420px,1fr) 86px 86px!important;align-items:center!important;gap:10px!important;background:#fff!important;border:1px solid var(--line)!important;border-radius:12px!important;padding:10px 12px!important}
  .quick-grid{grid-template-columns:minmax(260px,1fr) minmax(220px,1fr) auto auto!important}
  #addressList{display:grid!important;gap:8px!important}
  .record{border-radius:14px!important;padding:12px 14px!important}
  .record-head{grid-template-columns:minmax(420px,1fr) auto auto!important;align-items:center!important}
  .addr{font-size:15px!important;line-height:1.45!important}
  .meta{margin-top:6px!important;font-size:12px!important;gap:6px 14px!important}
  .source-list .item-meta{display:flex!important;flex-wrap:wrap!important;gap:8px 14px!important}
}
@media (max-width:640px){
  .quick-grid{grid-template-columns:1fr!important}
}

/* v13 PC asset badge and row polish */
@media (min-width:641px){
  #addressList{gap:9px!important}
  .record{padding:0!important;border:0!important;background:transparent!important;box-shadow:none!important}
  .record-select{display:grid!important;grid-template-columns:24px minmax(0,1fr)!important;gap:10px!important;align-items:center!important}
  .record-body{display:grid!important;grid-template-columns:minmax(0,1fr)!important;background:#fff!important;border:1px solid #e7edf5!important;border-radius:14px!important;padding:12px 14px!important;box-shadow:0 4px 14px rgba(15,23,42,.03)!important}
  .record-head{display:grid!important;grid-template-columns:minmax(0,1fr) 74px 70px!important;gap:10px!important;align-items:center!important;width:100%!important;background:transparent!important;border:0!important;padding:0!important}
  .addr{font-size:15px!important;line-height:1.5!important;white-space:normal!important;overflow-wrap:anywhere!important}
  .copy-btn{height:30px!important;min-width:62px!important;padding:0 12px!important;justify-self:end!important}
  .badge{height:28px!important;min-width:56px!important;padding:0 12px!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;border-radius:999px!important;font-size:13px!important;line-height:1!important;margin:0!important;justify-self:end!important}
  .badge.safe{background:#dcfce7!important;color:#166534!important;border:1px solid #bbf7d0!important}
  .badge.danger{background:#fee2e2!important;color:#991b1b!important;border:1px solid #fecaca!important}
  .badge.unknown{background:#fef3c7!important;color:#92400e!important;border:1px solid #fde68a!important}
  .meta{padding-left:0!important;display:flex!important;align-items:center!important;gap:8px 16px!important;margin-top:8px!important}
}

/* v17 health monitor + v17 PC table polish merged */
.source-badges{display:flex;align-items:center;gap:8px;flex-wrap:wrap;justify-content:flex-end}
.source-health-meta .health-detail{flex-basis:100%;font-weight:700}
@media (min-width:641px){
  #addressList.asset-table{display:flex!important;flex-direction:column!important;gap:8px!important;margin-top:14px!important}
  #addressList.asset-table .record{display:block!important;padding:0!important;border:0!important;background:transparent!important;box-shadow:none!important}
  #addressList.asset-table .record-select{display:block!important;width:100%!important}
  #addressList.asset-table .record-body{display:block!important;background:#fff!important;border:1px solid #e6edf6!important;border-radius:14px!important;padding:11px 13px!important;box-shadow:0 3px 12px rgba(15,23,42,.035)!important}
  #addressList.asset-table .record-body:hover{border-color:#cbd5e1!important;box-shadow:0 8px 22px rgba(15,23,42,.06)!important}
  #addressList.asset-table .record-head{display:grid!important;grid-template-columns:26px minmax(0,1fr) 72px 70px!important;gap:10px!important;align-items:center!important;width:100%!important;background:transparent!important;border:0!important;padding:0!important}
  #addressList.asset-table .record-check{display:block!important;width:18px!important;height:18px!important;margin:0!important;justify-self:center!important;accent-color:#1677ff!important}
  #addressList.asset-table .addr{font-size:15px!important;line-height:1.45!important;font-weight:800!important;color:#101828!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
  #addressList.asset-table .copy-btn{height:30px!important;min-width:60px!important;padding:0 10px!important;border-radius:999px!important;justify-self:end!important}
  #addressList.asset-table .badge{height:30px!important;min-width:56px!important;padding:0 12px!important;display:inline-flex!important;align-items:center!important;justify-content:center!important;border-radius:999px!important;font-size:13px!important;font-weight:900!important;line-height:1!important;margin:0!important;justify-self:end!important}
  #addressList.asset-table .meta{display:flex!important;align-items:center!important;gap:8px 16px!important;margin:8px 0 0 36px!important;padding:0!important;font-size:12px!important;color:#667085!important;white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important}
  #addressList.asset-table .mobile-meta{display:none!important}
  .advanced{margin-top:8px!important}
  .source-summary{margin-top:12px!important}
}
@media (max-width:640px){
  #addressList.asset-table .record-check{display:none!important}
}


/* v17 bulk sync progress panel */
.bulk-task-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px;color:#1f2a44}
.bulk-task-head span{color:#667085;font-weight:700;font-size:13px}
.bulk-task-list{display:grid;gap:8px;margin-top:12px;max-height:360px;overflow:auto}
.bulk-task-row{display:grid;grid-template-columns:minmax(0,1fr) 180px;gap:12px;align-items:center;background:#fff;border:1px solid #e6edf6;border-radius:12px;padding:10px 12px}
.bulk-task-main strong{display:block;color:#101828;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bulk-task-main span{display:block;color:#667085;font-size:12px;margin-top:3px}
.bulk-task-msg{color:#b54708;font-size:12px;margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.bulk-task-mini{height:8px;background:#eef2f7;border-radius:999px;overflow:hidden}
.bulk-task-mini div{height:100%;background:#1677ff;border-radius:999px;transition:width .25s ease}
@media(max-width:640px){.bulk-task-head{display:block}.bulk-task-head span{display:block;margin-top:4px}.bulk-task-row{grid-template-columns:1fr}.bulk-task-list{max-height:52vh}.bulk-task-mini{height:7px}}


/* v18 mobile bottom pagination polish */
@media (max-width:640px){
  .pagination{
    position:sticky!important;
    bottom:0!important;
    z-index:24!important;
    display:grid!important;
    grid-template-columns:minmax(84px,1fr) auto minmax(84px,1fr)!important;
    grid-template-areas:
      "prev info next"
      "jump jump jump"!important;
    gap:8px!important;
    align-items:center!important;
    margin:8px -8px 0!important;
    padding:8px 8px calc(8px + env(safe-area-inset-bottom,0px))!important;
    border-top:1px solid #e2e8f0!important;
    background:rgba(238,242,247,.97)!important;
    backdrop-filter:blur(10px)!important;
  }
  #prevBtn{grid-area:prev!important}
  #nextBtn{grid-area:next!important}
  #pageInfo{
    grid-area:info!important;
    text-align:center!important;
    font-size:13px!important;
    font-weight:800!important;
    color:#475467!important;
    white-space:nowrap!important;
    padding:0 4px!important;
  }
  .pagination .btn{
    height:40px!important;
    width:100%!important;
    min-width:0!important;
    border-radius:14px!important;
    background:#fff!important;
    border:1px solid #e2e8f0!important;
    color:#101828!important;
    font-size:14px!important;
    font-weight:900!important;
    box-shadow:0 4px 12px rgba(15,23,42,.04)!important;
  }
  .pagination .btn:not(:disabled):active{transform:scale(.98)!important}
  .pagination .btn:disabled{
    opacity:.45!important;
    color:#98a2b3!important;
    background:#f8fafc!important;
    box-shadow:none!important;
  }
  .page-jump{
    grid-area:jump!important;
    display:grid!important;
    grid-template-columns:minmax(0,1fr) 84px!important;
    gap:8px!important;
    align-items:center!important;
  }
  .page-jump input{
    width:100%!important;
    height:40px!important;
    border:1px solid #d8e0ea!important;
    border-radius:14px!important;
    background:#fff!important;
    padding:0 12px!important;
    text-align:center!important;
    font-size:14px!important;
    font-weight:800!important;
    outline:none!important;
  }
  .page-jump input:focus{border-color:#1677ff!important;box-shadow:0 0 0 3px rgba(22,119,255,.12)!important}
  .pagination.one-page{
    grid-template-columns:1fr!important;
    grid-template-areas:"info"!important;
    padding:8px 8px calc(8px + env(safe-area-inset-bottom,0px))!important;
  }
  .pagination.one-page #prevBtn,
  .pagination.one-page #nextBtn,
  .pagination.one-page .page-jump{display:none!important}
  .pagination.one-page #pageInfo{
    height:36px!important;
    display:flex!important;
    align-items:center!important;
    justify-content:center!important;
    background:#fff!important;
    border:1px solid #e2e8f0!important;
    border-radius:14px!important;
  }
}
@media (max-width:380px){
  .pagination{grid-template-columns:minmax(76px,1fr) auto minmax(76px,1fr)!important;gap:6px!important}
  .pagination .btn,.page-jump input{height:38px!important;font-size:13px!important}
  #pageInfo{font-size:12px!important}
}
</style>
</head>
<body>
<div class="mobile-header"><button class="mobile-menu-btn" onclick="openDrawer()" aria-label="打开菜单">☰</button><div><div class="mh-title" id="mobilePageTitle">资产看板</div><div class="mh-sub" id="mobileSubTitle">只显示地址和状态，更多功能在菜单里</div></div></div>
<div class="mobile-overlay" onclick="closeDrawer()"></div>
<div class="app">


<aside class="sidebar">
<button class="mobile-only btn" style="margin-bottom:12px" onclick="closeDrawer()">关闭菜单</button>
<div class="brand">
<div class="logo">资</div>
<div><b>资产智能管控台</b><span>管理后台</span></div>
</div>
<div class="nav">
<button data-tab="home" onclick="showTab('home')">快速同步</button>
<button class="active" data-tab="assets" onclick="showTab('assets')">资产看板</button>
<button data-tab="sources" onclick="showTab('sources')">数据源</button>
<button data-tab="backup" onclick="showTab('backup')">备份恢复</button>
<button data-tab="system" onclick="showTab('system')">系统维护</button>
</div>
<div class="user">
<div class="user-name">__USERNAME__</div>
<form method="post" action="/logout"><button type="submit">退出登录</button></form>
</div>
</aside>

<main class="main">
<div class="topbar">
<div>
<h1 id="pageTitle">资产看板</h1>
<div class="desc" id="clockText">本地时间 --</div>
</div>
<div class="desc" id="dbInfo">数据库加载中...</div>
</div>

<div class="content">
<section class="stats">
<div class="card stat"><span>资产总数</span><strong id="totalCount">0</strong></div>
<div class="card stat"><span>纯净可用</span><strong id="safeCount">0</strong></div>
<div class="card stat"><span>风控中</span><strong id="dangerCount">0</strong></div>
<div class="card stat"><span>数据源</span><strong id="sourceCount">0</strong></div>
</section>

<div id="statusMsg" class="notice">已就绪。</div>

<section id="tab-home" class="tab">
<div class="panel">
<div class="panel-head"><h2>常用操作：快速同步</h2></div>
<div class="quick-grid">
<select id="quickSourceSelect" onchange="onQuickSourceChange()">
<option value="">请选择数据源</option>
</select>
<input id="quickKeyword" placeholder="关键词会自动带出，也可手动修改">
<button class="btn primary" onclick="startQuickSync()">开始同步</button>
<button class="btn" onclick="syncAllEnabled()">同步全部启用</button>
</div>
</div>

<div class="panel">
<div class="panel-head">
<h2>当前任务</h2>
<button class="btn danger" id="cancelTaskBtn" onclick="cancelCurrentTask()" disabled>取消任务</button>
</div>
<div id="taskBox" class="notice">暂无正在运行的任务。</div>
</div>

<div class="panel">
<div class="panel-head"><h2>最近同步</h2><button class="btn" onclick="fetchSyncRuns()">刷新</button></div>
<div id="homeSyncList" class="history-list"></div>
</div>
</section>

<section id="tab-assets" class="tab active">
<div class="panel">
<div class="panel-head">
<h2>资产看板</h2>
<div class="actions">
<button class="btn" onclick="copyVisibleSafeAddresses()">复制纯净地址</button>
<button class="btn" onclick="exportCurrentCsv()">导出 CSV</button>
<button class="btn" onclick="fetchAll()">刷新</button>
</div>
</div>

<div class="asset-tools">
<input id="assetSearch" placeholder="搜索地址 / 关键词" oninput="renderList(true)">
<select id="sourceFilter" onchange="renderList(true)">
<option value="all">全部数据源</option>
</select>
<button class="btn" onclick="renderList(true)">查询</button>
</div>

<div id="sourceSummary" class="source-summary"></div>
<div class="status-tabs">
<button id="statusBtn_all" class="active" onclick="setStatusFilter('all')">全部 <span id="tabCount_all">0</span></button>
<button id="statusBtn_safe" onclick="setStatusFilter('safe')">纯净可用 <span id="tabCount_safe">0</span></button>
<button id="statusBtn_danger" onclick="setStatusFilter('danger')">风控中 <span id="tabCount_danger">0</span></button>
<button id="statusBtn_unknown" onclick="setStatusFilter('unknown')">未知 <span id="tabCount_unknown">0</span></button>
</div>

<details class="advanced">
<summary>高级筛选 / 批量操作</summary>
<div class="advanced-box">
<select id="sortFilter" onchange="renderList(true)">
<option value="latest">最新更新优先</option>
<option value="shortest">倒计时由短到长</option>
<option value="longest">倒计时由长到短</option>
</select>
<select id="pageSize" onchange="renderList(true)">
<option value="10" selected>10/页</option>
<option value="20">20/页</option>
<option value="50">50/页</option>
<option value="100">100/页</option>
</select>
<button class="btn" onclick="exportCurrentCsv()">导出当前结果</button>
<button class="btn danger" onclick="deleteSelectedRecords()">删除勾选记录</button>
</div>
</details>

<ul id="addressList" class="list asset-table"></ul>
<div class="pagination">
<button class="btn" id="prevBtn" onclick="changePage(-1)">上一页</button>
<span id="pageInfo">第 1 / 1 页</span>
<div class="page-jump"><input id="jumpPageInput" type="number" inputmode="numeric" min="1" placeholder="输入页码" onkeydown="if(event.key==='Enter') jumpToPage()"><button class="btn" onclick="jumpToPage()">跳转</button></div>
<button class="btn" id="nextBtn" onclick="changePage(1)">下一页</button>
</div>
</div>
</section>

<section id="tab-sources" class="tab">
<div class="source-layout">
<div class="panel">
<div class="panel-head"><h2>数据源列表</h2><button class="btn" onclick="loadSources()">刷新</button></div>
<div id="sourceList" class="source-list"></div>
</div>

<div class="panel">
<div class="panel-head">
<h2>数据源编辑</h2>
<div class="actions">
<button class="btn" onclick="newSource()">新建</button>
<button class="btn" onclick="testSelectedSource()">测试连接</button>
<button class="btn primary" id="saveSourceBtn" onclick="saveSource()">保存</button>
<button class="btn danger" onclick="deleteSource()">删除</button>
</div>
</div>

<div class="form-grid">
<div class="full">
<label>选择数据源</label>
<select id="sourceSelect" onchange="onSourceChange()"><option value="">手动输入或新建数据源</option></select>
</div>
<div>
<label>数据源名称</label>
<input id="sourceName" placeholder="例如：创世地址库">
</div>
<div>
<label>默认关键词</label>
<input id="keywordInput" placeholder="例如：管城">
</div>
<div class="full">
<label>数据源地址</label>
<input id="urlInput" placeholder="https://example.com/">
</div>
<div>
<label>是否启用</label>
<select id="sourceEnabled">
<option value="1">启用</option>
<option value="0">停用</option>
</select>
</div>
<div>
<label>立即同步</label>
<button class="btn primary" onclick="triggerScrapeFromForm()">同步当前数据源</button>
</div>
</div>

<details class="clean" style="margin-top:12px">
<summary>高级设置</summary>
<div style="height:12px"></div>
<div class="form-grid">
<div class="full">
<label>请求 Cookie，可选</label>
<textarea id="sourceCookie" placeholder="普通网站留空。需要防护 Cookie 的网站粘贴：_ok1_=...; PHPSESSID=..."></textarea>
</div>
<div>
<label>定时同步</label>
<select id="scheduleEnabled">
<option value="0">关闭</option>
<option value="1">开启</option>
</select>
</div>
<div>
<label>定时间隔</label>
<select id="scheduleInterval">
<option value="0">不启用</option>
<option value="5">每 5 分钟</option>
<option value="15">每 15 分钟</option>
<option value="30">每 30 分钟</option>
<option value="60">每 1 小时</option>
<option value="180">每 3 小时</option>
<option value="360">每 6 小时</option>
<option value="1440">每天</option>
</select>
</div>
</div>
</details>
</div>
</div>
</section>

<section id="tab-backup" class="tab">
<div class="panel">
<div class="panel-head">
<h2>备份恢复</h2>
<div class="actions">
<button class="btn primary" onclick="createBackup()">创建备份</button>
<button class="btn" onclick="downloadLatestBackup()">下载最新备份</button>
<button class="btn" onclick="loadBackups()">刷新列表</button>
</div>
</div>
<div class="notice">
备份包可用于迁移、恢复和合并导入。建议重要操作前先创建并下载备份。
</div>
</div>

<div class="panel">
<div class="panel-head"><h2>上传备份并合并</h2></div>
<input id="backupFile" type="file" accept=".gz,.tar.gz">
<div style="height:10px"></div>
<button class="btn primary" onclick="mergeUploadBackup()">上传并合并导入</button>
</div>

<div class="panel">
<div class="panel-head"><h2>服务器本地备份</h2></div>
<div id="backupList" class="backup-list"></div>
</div>
</section>

<section id="tab-system" class="tab">
<div class="panel">
<div class="panel-head">
<h2>系统维护</h2>
<div class="actions">
<button class="btn" onclick="fetchStats()">数据库自检</button>
<button class="btn" onclick="fetchLogs()">刷新日志</button>
<button class="btn warn" onclick="cleanupMaintenance()">清理旧日志</button>
</div>
</div>
<div id="systemInfo" class="notice">系统运行正常。</div>
</div>

<div class="panel">
<div class="panel-head"><h2>回收站</h2><div class="actions"><button class="btn" onclick="loadTrash()">刷新</button><button class="btn" onclick="restoreSelectedTrash()">恢复勾选</button><button class="btn danger" onclick="purgeTrash()">清空回收站</button></div></div>
<div id="trashList" class="source-list"></div>
</div>

<div class="panel">
<div class="panel-head"><h2>运行日志</h2></div>
<div id="logBox" class="notice logbox">暂无日志。</div>
</div>
</section>
</div>
</main>
</div>

<script>
const $ = (id) => document.getElementById(id);

let globalData = [];
let sources = [];
let syncRuns = [];
let backups = [];
let currentPage = 1;
let currentTotalPages = 1;
let currentStatus = "all";
let currentTaskId = null;
let currentTaskIds = [];
let taskPollTimer = null;

window.onload = () => {
    updateClock();
    setInterval(updateClock, 1000);
    if (window.matchMedia && window.matchMedia("(max-width: 640px)").matches) {
        currentStatus = "safe";
        document.querySelectorAll(".status-tabs button").forEach(x => x.classList.remove("active"));
        const btn = $("statusBtn_safe");
        if (btn) btn.classList.add("active");
    }
    fetchAll();
    fetchLogs();
    fetchSyncRuns();
    loadBackups();
    setInterval(fetchLogs, 5000);
    setInterval(fetchSyncRuns, 10000);
};

function updateClock() {
    $("clockText").innerText = "本地时间 " + new Date().toLocaleString();
}

function openDrawer(){ document.body.classList.add("drawer-open"); }
function closeDrawer(){ document.body.classList.remove("drawer-open"); }

function showTab(name) {
    document.querySelectorAll(".tab").forEach(x => x.classList.remove("active"));
    document.querySelectorAll(".nav button").forEach(x => x.classList.remove("active"));
    $("tab-" + name).classList.add("active");
    const btn = document.querySelector(`.nav button[data-tab="${name}"]`);
    if (btn) btn.classList.add("active");

    const titles = {home:"首页", assets:"资产看板", sources:"数据源管理", backup:"备份恢复", system:"系统维护"};
    $("pageTitle").innerText = titles[name] || "资产智能管控台";
    if ($("mobilePageTitle")) $("mobilePageTitle").innerText = titles[name] || "资产智能管控台";
    if (name === "system") loadTrash();
    closeDrawer();
}

async function apiFetch(url, opt = {}) {
    const r = await fetch(url, opt);
    if (r.status === 401) {
        location.href = "/login";
        return null;
    }
    return r;
}

function msg(text, type = "") {
    const el = $("statusMsg");
    el.className = "notice" + (type ? " " + type : "");
    el.textContent = text;
}

function escapeHtml(s) {
    return String(s ?? "").replace(/[&<>"']/g, c => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
    }[c]));
}

function formatSize(n) {
    n = Number(n || 0);
    if (n > 1024 * 1024) return (n / 1024 / 1024).toFixed(2) + " MB";
    return (n / 1024).toFixed(1) + " KB";
}

function cls(t, st) {
    st = st || "";
    t = t || "未知状态";

    if (st === "safe") return "safe";
    if (st === "unknown") return "unknown";
    if (st === "countdown") return "danger";

    if (t.includes("已释放") || t.includes("已过期") || t.includes("纯净可用") || t.includes("空闲") || t.includes("可用")) return "safe";
    if (t.includes("未知状态")) return "unknown";
    return "danger";
}

function parseTimeValue(t) {
    t = t || "未知状态";
    if (t.includes("已过期") || t.includes("已释放") || t.includes("纯净可用")) return -1;
    if (t.includes("未知状态")) return 999999;

    const d = t.match(/(\d+)\s*天/);
    const h = t.match(/(\d+)\s*小时/);

    if (d || h) {
        let n = 0;
        if (d) n += parseInt(d[1]) * 24;
        if (h) n += parseInt(h[1]);
        return n;
    }

    return 99999;
}

async function fetchAll() {
    await fetchStats();
    await loadSources();
    await fetchRecords();
}

async function fetchStats() {
    try {
        const r = await apiFetch("/api/stats");
        if (!r) return;
        const j = await r.json();

        $("totalCount").textContent = j.assets || 0;
        $("safeCount").textContent = j.safe || 0;
        $("dangerCount").textContent = j.danger || 0;
        $("sourceCount").textContent = j.sources || 0;
        $("dbInfo").textContent = `数据库：${j.assets || 0} 条资产 / ${formatSize(j.db_size)}`;

        if ($("systemInfo")) {
            $("systemInfo").textContent =
                `数据库自检正常。\n` +
                `资产数量：${j.assets || 0}\n` +
                `数据源数量：${j.sources || 0}\n` +
                `日志数量：${j.logs || 0}\n` +
                `同步历史：${j.sync_runs || 0}\n` +
                `数据库大小：${formatSize(j.db_size)}\n` +
                `数据库路径：${j.db_path || "-"}\n` +
                `检查时间：${j.time || "-"}`;
        }
    } catch (e) {}
}

function fillSourceForm(s) {
    $("sourceName").value = s ? String(s.name || "") : "";
    $("urlInput").value = s ? String(s.source_url || "") : "";
    $("keywordInput").value = s ? String(s.default_keyword || "") : "";
    $("sourceCookie").value = s ? String(s.request_cookie || "") : "";
    $("sourceEnabled").value = s && !s.enabled ? "0" : "1";
    $("scheduleEnabled").value = s && s.schedule_enabled ? "1" : "0";
    $("scheduleInterval").value = s ? String(s.schedule_interval_minutes || 0) : "0";
    $("saveSourceBtn").textContent = s ? "更新" : "保存";
}

function selectedSource() {
    const id = parseInt($("sourceSelect").value || "0");
    return sources.find(x => x.id === id) || null;
}

function onSourceChange() {
    fillSourceForm(selectedSource());
}

function onQuickSourceChange() {
    const id = parseInt($("quickSourceSelect").value || "0");
    const s = sources.find(x => x.id === id);
    $("quickKeyword").value = s ? (s.default_keyword || "") : "";
}

function newSource() {
    $("sourceSelect").value = "";
    fillSourceForm(null);
    msg("已切换到新建数据源模式。", "");
}


function splitSourceName(name) {
    name = String(name || "未命名数据源").trim();
    const parts = name.split("|").map(x => x.trim()).filter(Boolean);
    if (parts.length >= 2) return {group: parts[0], item: parts.slice(1).join("|")};
    return {group: name, item: name};
}

function sourceDisplayName(s) {
    if (!s) return "当前数据源";
    const parts = splitSourceName(s.name || "未命名数据源");
    if (parts.group && parts.item && parts.group !== parts.item) return `${parts.group}｜${parts.item}`;
    return parts.item || parts.group || "未命名数据源";
}

function appendGroupedSourceOptions(selectEl, firstLabel, firstValue, mode) {
    selectEl.innerHTML = "";
    if (firstLabel !== null) selectEl.appendChild(new Option(firstLabel, firstValue));
    const groups = new Map();
    sources.forEach(s => {
        const parts = splitSourceName(s.name);
        const key = parts.group || "其他";
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push({source: s, item: parts.item});
    });
    Array.from(groups.entries()).sort((a,b) => a[0].localeCompare(b[0], "zh-CN")).forEach(([groupName, rows]) => {
        const optgroup = document.createElement("optgroup");
        optgroup.label = groupName;
        rows.sort((a,b) => String(a.item).localeCompare(String(b.item), "zh-CN"));
        rows.forEach(({source:s}) => {
            let label = sourceDisplayName(s);
            if (!s.enabled) label += " [停用]";
            optgroup.appendChild(new Option(label, String(s.id)));
        });
        selectEl.appendChild(optgroup);
    });
}

async function loadSources() {
    try {
        const r = await apiFetch("/api/sources");
        if (!r) return;

        const j = await r.json();
        sources = j.data || [];

        const old = $("sourceSelect").value;
        const oldQuick = $("quickSourceSelect").value;
        const oldFilter = $("sourceFilter").value;

        appendGroupedSourceOptions($("sourceSelect"), "手动输入或新建数据源", "", "edit");
        appendGroupedSourceOptions($("quickSourceSelect"), "请选择数据源", "", "quick");
        appendGroupedSourceOptions($("sourceFilter"), "全部数据源", "all", "filter");

        const list = $("sourceList");
        list.innerHTML = "";

        sources.forEach(s => {
            const item = document.createElement("div");
            item.className = "item";
            const healthClass = s.health_status === "ok" ? "safe" : (s.health_status === "error" ? "danger" : "unknown");
            item.innerHTML = `
                <div class="item-head source-health-head">
                    <div class="item-title">${escapeHtml(sourceDisplayName(s))}</div>
                    <div class="source-badges">
                        <span class="badge ${healthClass}">${escapeHtml(s.health_label || "状态")}</span>
                        <span class="badge ${s.enabled ? "safe" : "unknown"}">${s.enabled ? "启用" : "停用"}</span>
                    </div>
                </div>
                <div class="item-meta source-health-meta">
                    <span>地址：${escapeHtml(s.source_url || "-")}</span>
                    <span>关键词：${escapeHtml(s.default_keyword || "-")}</span>
                    <span>Cookie：${s.request_cookie ? "有" : "无"}</span>
                    <span>定时：${s.schedule_enabled ? (s.schedule_interval_minutes + " 分钟") : "关闭"}</span>
                    <span>资产：${s.asset_count || 0} 条</span>
                    <span>活跃任务：${s.active_job_count || 0}</span>
                    <span>上次成功：${escapeHtml(s.last_success_at || "-")}</span>
                    <span>上次失败：${escapeHtml(s.last_failed_at || "-")}</span>
                    ${s.health_detail ? `<span class="health-detail ${s.health_status === "error" ? "danger-text" : ""}">${escapeHtml(s.health_detail)}</span>` : ""}
                </div>
            `;
            item.onclick = () => {
                $("sourceSelect").value = String(s.id);
                fillSourceForm(s);
            };
            list.appendChild(item);
        });

        if (sources.some(s => String(s.id) === old)) {
            $("sourceSelect").value = old;
            fillSourceForm(selectedSource());
        }

        if (sources.some(s => String(s.id) === oldQuick)) {
            $("quickSourceSelect").value = oldQuick;
        }
        if (oldFilter === "all" || sources.some(s => String(s.id) === oldFilter)) {
            $("sourceFilter").value = oldFilter;
        }
    } catch (e) {
        msg("加载数据源失败。", "bad");
    }
}


async function testSelectedSource() {
    const id = $("sourceSelect").value;
    if (!id) { msg("请先选择一个已保存的数据源。", "warn"); return; }
    msg("正在测试数据源连接，请稍候...", "");
    try {
        const r = await apiFetch(`/api/sources/${id}/test`, {method:"POST"});
        if (!r) return;
        const j = await r.json();
        msg(j.message || "测试完成", j.success ? "ok" : "bad");
        fetchLogs();
    } catch (e) {
        msg("测试连接失败。", "bad");
    }
}

function visibleSafeAddresses() {
    return filteredData().filter(x => cls(x.status_timer, x.status_type) === "safe").map(x => String(x.content_text || "").trim()).filter(Boolean);
}

async function copyVisibleSafeAddresses() {
    const rows = visibleSafeAddresses();
    if (!rows.length) { msg("当前筛选下没有纯净地址。", "warn"); return; }
    await copyAddress(rows.join("\n"), null);
    msg(`已复制 ${rows.length} 条纯净地址`, "ok");
}

async function saveSource() {
    const id = $("sourceSelect").value;
    const body = {
        name: $("sourceName").value.trim(),
        source_url: $("urlInput").value.trim(),
        default_keyword: $("keywordInput").value.trim(),
        request_cookie: $("sourceCookie").value.trim(),
        schedule_enabled: $("scheduleEnabled").value === "1",
        schedule_interval_minutes: parseInt($("scheduleInterval").value || "0"),
        enabled: $("sourceEnabled").value === "1",
        sort_order: 0,
    };

    if (!body.name || !body.source_url) {
        msg("请填写数据源名称和地址。", "bad");
        return;
    }

    try {
        const r = await apiFetch(id ? "/api/sources/" + id : "/api/sources", {
            method: id ? "PUT" : "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(body),
        });
        if (!r) return;

        const j = await r.json();
        msg(j.message || "数据源操作完成", j.success ? "ok" : "bad");
        await loadSources();
        if (j.id) $("sourceSelect").value = String(j.id);
        fetchStats();
        fetchLogs();
    } catch (e) {
        msg("保存数据源失败。", "bad");
    }
}

async function deleteSource() {
    const id = $("sourceSelect").value;
    if (!id) {
        msg("请先选择一个已保存的数据源。", "warn");
        return;
    }
    const s = selectedSource();
    const count = s ? (s.asset_count || 0) : 0;
    if (!confirm(`确定删除数据源「${s ? sourceDisplayName(s) : id}」吗？\n当前关联资产：${count} 条。\n默认只删除数据源配置，资产会保留。`)) return;

    let deleteAssets = false;
    if (count > 0) {
        const text = prompt(`如需同时删除该数据源下 ${count} 条资产，请输入 DELETE。\n直接留空或取消则只删除数据源配置，保留资产。`);
        deleteAssets = text === "DELETE";
    }

    try {
        const r = await apiFetch("/api/sources/" + id + "?delete_assets=" + (deleteAssets ? "true" : "false"), {method: "DELETE"});
        if (!r) return;
        const j = await r.json();
        msg(j.message || "数据源已删除", j.success ? "ok" : "bad");
        newSource();
        await loadSources();
        fetchStats();
        fetchRecords();
        fetchLogs();
    } catch (e) {
        msg("删除数据源失败。", "bad");
    }
}

async function syncAllEnabled() {
    if (!confirm("将为所有启用且填写了默认关键词的数据源创建同步任务。确定继续吗？")) return;
    try {
        msg("正在创建批量同步任务...", "");
        const r = await apiFetch("/api/sync_tasks/all_enabled", {method: "POST"});
        if (!r) return;
        const j = await r.json();
        msg(j.message || "批量任务已创建", j.success ? "ok" : "bad");
        showTab("home");

        const created = Array.isArray(j.created) ? j.created : [];
        currentTaskIds = created.map(x => x.task_id).filter(Boolean);
        currentTaskId = currentTaskIds.length === 1 ? currentTaskIds[0] : null;

        if (currentTaskIds.length) {
            $("cancelTaskBtn").disabled = false;
            renderBulkTaskMessage([], currentTaskIds.length, Array.isArray(j.skipped) ? j.skipped.length : 0);
            pollCurrentTasks();
        } else {
            $("cancelTaskBtn").disabled = true;
            $("taskBox").className = "notice warn";
            $("taskBox").innerHTML = `没有创建新的同步任务。${Array.isArray(j.skipped) && j.skipped.length ? "已跳过正在运行的数据源：" + escapeHtml(j.skipped.join("、")) : ""}`;
        }

        fetchLogs();
        fetchSyncRuns();
    } catch (e) {
        msg("创建批量同步任务失败。", "bad");
    }
}

function startQuickSync() {
    const id = parseInt($("quickSourceSelect").value || "0");
    const s = sources.find(x => x.id === id);
    const keyword = $("quickKeyword").value.trim();

    if (!s) {
        msg("请先选择数据源。", "warn");
        return;
    }

    if (!keyword) {
        msg("请填写关键词。", "warn");
        return;
    }

    startTask({
        target_url: s.source_url,
        keyword: keyword,
        request_cookie: s.request_cookie || "",
        source_id: s.id,
    });
}

function triggerScrapeFromForm() {
    const target_url = $("urlInput").value.trim();
    const keyword = $("keywordInput").value.trim();
    const request_cookie = $("sourceCookie").value.trim();
    const source_id = $("sourceSelect").value ? parseInt($("sourceSelect").value) : null;

    if (!target_url || !keyword) {
        msg("请填写数据源地址和关键词。", "warn");
        return;
    }

    startTask({target_url, keyword, request_cookie, source_id});
}

async function startTask(payload) {
    msg("正在创建后台同步任务...", "");

    try {
        const r = await apiFetch("/api/sync_tasks", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload),
        });
        if (!r) return;

        const j = await r.json();
        if (!j.success) {
            msg(j.message || "任务创建失败", "bad");
            return;
        }

        currentTaskId = j.task_id;
        currentTaskIds = [j.task_id];
        $("cancelTaskBtn").disabled = false;
        showTab("home");
        pollCurrentTask();
    } catch (e) {
        msg("创建后台任务失败。", "bad");
    }
}

function renderTaskMessage(task) {
    const progress = Math.max(0, Math.min(100, parseInt(task.progress || 0)));
    const box = $("taskBox");
    box.className = "notice";
    if (task.status === "success") box.className = "notice ok";
    if (task.status === "failed" || task.status === "cancelled") box.className = "notice bad";
    if (task.status === "empty") box.className = "notice warn";

    box.innerHTML = `
任务：${escapeHtml(task.task_id || "-")}<br>
状态：${escapeHtml(task.status || "-")}<br>
阶段：${escapeHtml(task.phase || "-")}<br>
进度：${progress}%<br>
页数：${task.current_page || 0} / ${task.total_pages || 0}<br>
已解析：${task.total_found || 0} 条<br>
${task.message ? ("结果：" + escapeHtml(task.message) + "<br>") : ""}
<div class="progress-wrap"><div class="progress-bar" style="width:${progress}%"></div></div>
`;
}

function renderBulkTaskMessage(tasks, expectedCount, skippedCount=0) {
    const list = Array.isArray(tasks) ? tasks : [];
    const total = expectedCount || list.length || currentTaskIds.length || 0;
    const doneStatuses = ["success", "failed", "empty", "cancelled"];
    const doneCount = list.filter(t => doneStatuses.includes(t.status)).length;
    const runningCount = list.filter(t => ["queued", "running"].includes(t.status)).length;
    const failedCount = list.filter(t => ["failed", "empty", "cancelled"].includes(t.status)).length;
    const successCount = list.filter(t => t.status === "success").length;
    const avgProgress = list.length ? Math.round(list.reduce((sum, t) => sum + Math.max(0, Math.min(100, parseInt(t.progress || 0))), 0) / list.length) : 0;
    const overall = total ? Math.round((doneCount / total) * 100) : avgProgress;
    const box = $("taskBox");
    box.className = failedCount ? "notice warn" : "notice";

    const rows = list.map((t, idx) => {
        const p = Math.max(0, Math.min(100, parseInt(t.progress || 0)));
        const title = t.source_name || t.source_title || t.keyword || t.source_url || ("任务 " + (idx + 1));
        const status = t.status || "queued";
        const phase = t.phase || "等待执行";
        const msgText = t.message ? `<div class="bulk-task-msg">${escapeHtml(t.message)}</div>` : "";
        return `<div class="bulk-task-row">
            <div class="bulk-task-main">
                <strong>${escapeHtml(title)}</strong>
                <span>${escapeHtml(status)} · ${escapeHtml(phase)} · ${p}%</span>
                ${msgText}
            </div>
            <div class="bulk-task-mini"><div style="width:${p}%"></div></div>
        </div>`;
    }).join("");

    box.innerHTML = `
<div class="bulk-task-head">
  <strong>批量同步进度</strong>
  <span>完成 ${doneCount}/${total}，成功 ${successCount}，异常 ${failedCount}${skippedCount ? "，跳过 " + skippedCount : ""}</span>
</div>
<div class="progress-wrap"><div class="progress-bar" style="width:${overall}%"></div></div>
<div class="bulk-task-list">${rows || "任务正在创建或等待调度..."}</div>`;
}

async function pollCurrentTasks() {
    if (!currentTaskIds.length) return;
    try {
        const results = await Promise.all(currentTaskIds.map(async (id) => {
            const r = await apiFetch("/api/sync_tasks/" + id);
            if (!r) return null;
            const j = await r.json();
            return j && j.success ? j.task : null;
        }));
        const tasks = results.filter(Boolean);
        renderBulkTaskMessage(tasks, currentTaskIds.length);
        const doneStatuses = ["success", "failed", "empty", "cancelled"];
        const done = tasks.length === currentTaskIds.length && tasks.every(t => doneStatuses.includes(t.status));
        if (done) {
            $("cancelTaskBtn").disabled = true;
            currentTaskId = null;
            currentTaskIds = [];
            await fetchAll();
            fetchLogs();
            fetchSyncRuns();
            return;
        }
        clearTimeout(taskPollTimer);
        taskPollTimer = setTimeout(pollCurrentTasks, 2000);
    } catch (e) {
        msg("批量任务轮询失败。", "bad");
    }
}

async function pollCurrentTask() {
    if (!currentTaskId) return;

    try {
        const r = await apiFetch("/api/sync_tasks/" + currentTaskId);
        if (!r) return;

        const j = await r.json();
        if (!j.success || !j.task) {
            msg(j.message || "任务状态获取失败", "bad");
            $("cancelTaskBtn").disabled = true;
            currentTaskId = null;
            currentTaskIds = [];
            return;
        }

        const task = j.task;
        renderTaskMessage(task);

        if (["success", "failed", "empty", "cancelled"].includes(task.status)) {
            $("cancelTaskBtn").disabled = true;
            currentTaskId = null;
            currentTaskIds = [];
            await fetchAll();
            fetchLogs();
            fetchSyncRuns();
            return;
        }

        clearTimeout(taskPollTimer);
        taskPollTimer = setTimeout(pollCurrentTask, 2000);
    } catch (e) {
        msg("任务轮询失败。", "bad");
        $("cancelTaskBtn").disabled = true;
        currentTaskId = null;
    }
}

async function cancelCurrentTask() {
    const ids = currentTaskIds.length ? currentTaskIds.slice() : (currentTaskId ? [currentTaskId] : []);
    if (!ids.length) {
        msg("当前没有正在运行的任务。", "warn");
        return;
    }
    if (!confirm(ids.length > 1 ? `确定取消这 ${ids.length} 个同步任务吗？` : "确定取消当前同步任务吗？")) return;

    try {
        let ok = 0;
        for (const id of ids) {
            const r = await apiFetch("/api/sync_tasks/" + id + "/cancel", {method: "POST"});
            if (!r) continue;
            const j = await r.json();
            if (j.success) ok += 1;
        }
        msg(`已请求取消 ${ok}/${ids.length} 个任务`, ok ? "warn" : "bad");
        if (currentTaskIds.length > 1) pollCurrentTasks(); else pollCurrentTask();
    } catch (e) {
        msg("取消任务失败。", "bad");
    }
}

async function fetchRecords() {
    try {
        const r = await apiFetch("/api/get_records");
        if (!r) return;

        const j = await r.json();
        globalData = j.data || [];
        renderList(true);
    } catch (e) {
        showEmpty("拉取数据失败。");
    }
}

function setStatusFilter(v) {
    currentStatus = v;
    document.querySelectorAll(".status-tabs button").forEach(x => x.classList.remove("active"));
    const btn = $("statusBtn_" + v);
    if (btn) btn.classList.add("active");
    renderList(true);
}

function recordSourceId(x) {
    return String(x.effective_source_id || x.source_id || "");
}

function sourceById(id) {
    id = String(id || "");
    return sources.find(s => String(s.id) === id) || null;
}

function looksLikeUrl(v) {
    return /^https?:\/\//i.test(String(v || "").trim());
}

function sourceForRecord(x) {
    const byId = sourceById(recordSourceId(x));
    if (byId) return byId;
    const url = String(x.source_url || "").trim();
    const kw = String(x.keyword || "").trim();
    const exact = sources.find(s => String(s.source_url || "").trim() === url && String(s.default_keyword || "").trim() === kw);
    if (exact) return exact;
    const sameUrl = sources.filter(s => String(s.source_url || "").trim() === url);
    if (sameUrl.length === 1) return sameUrl[0];
    return null;
}

function formatRecordSourceName(name, record) {
    const s = record ? sourceForRecord(record) : null;
    if (s) return sourceDisplayName(s);
    if (!looksLikeUrl(name)) {
        const parts = splitSourceName(name);
        if (parts.group && parts.item && parts.group !== parts.item) return `${parts.group}｜${parts.item}`;
        return parts.item || parts.group || name || "-";
    }
    const kw = record && record.keyword ? String(record.keyword).trim() : "";
    return kw ? `未归类｜${kw}` : "未归类";
}

function recordMatchesSelectedSource(x, selected) {
    if (!selected || selected === "all") return true;
    if (recordSourceId(x) === selected) return true;
    const s = sourceById(selected);
    const rs = sourceForRecord(x);
    if (rs && String(rs.id) === String(selected)) return true;
    return !!(s && !recordSourceId(x) && x.source_url === s.source_url && String(x.keyword || "").trim() === String(s.default_keyword || "").trim());
}

function sourceScopedData() {
    const source = $("sourceFilter").value;
    const q = $("assetSearch").value.trim().toLowerCase();
    return globalData.filter(x => {
        if (!recordMatchesSelectedSource(x, source)) return false;
        if (q) {
            const text = `${x.content_text || ""} ${x.keyword || ""} ${formatRecordSourceName(x.source_name || "", x)} ${x.source_url || ""}`.toLowerCase();
            if (!text.includes(q)) return false;
        }
        return true;
    });
}

function countByStatus(rows) {
    const out = {all: rows.length, safe: 0, danger: 0, unknown: 0};
    rows.forEach(x => { out[cls(x.status_timer, x.status_type)]++; });
    return out;
}

function updateLocalCounters(scopedRows, visibleRows) {
    const c = countByStatus(scopedRows);
    const selected = $("sourceFilter").value;
    const selectedSource = sourceById(selected);
    const title = selected === "all" ? "全部数据源" : (selectedSource ? sourceDisplayName(selectedSource) : "当前数据源");

    $("tabCount_all").textContent = c.all;
    $("tabCount_safe").textContent = c.safe;
    $("tabCount_danger").textContent = c.danger;
    $("tabCount_unknown").textContent = c.unknown;

    $("totalCount").textContent = visibleRows.length;
    $("safeCount").textContent = visibleRows.filter(x => cls(x.status_timer, x.status_type) === "safe").length;
    $("dangerCount").textContent = visibleRows.filter(x => cls(x.status_timer, x.status_type) === "danger").length;

    const box = $("sourceSummary");
    if (box) {
        box.innerHTML = `
            <strong>${escapeHtml(title)} 的当前统计</strong>
            <span class="mini">全部 ${c.all}</span>
            <span class="mini safe">纯净 ${c.safe}</span>
            <span class="mini danger">风控 ${c.danger}</span>
            <span class="mini unknown">未知 ${c.unknown}</span>
        `;
    }
}

function filteredData() {
    const sort = $("sortFilter").value;
    const scoped = sourceScopedData();

    let arr = scoped.filter(x => {
        const s = cls(x.status_timer, x.status_type);
        if (currentStatus !== "all" && s !== currentStatus) return false;
        return true;
    });

    if (sort === "shortest") arr.sort((a,b) => parseTimeValue(a.status_timer) - parseTimeValue(b.status_timer));
    else if (sort === "longest") arr.sort((a,b) => parseTimeValue(b.status_timer) - parseTimeValue(a.status_timer));
    else arr.sort((a,b) => new Date((b.last_checked || "").replace(" ","T")) - new Date((a.last_checked || "").replace(" ","T")));

    updateLocalCounters(scoped, arr);
    return arr;
}



async function copyAddress(text, btn) {
    const value = String(text || "").trim();
    if (!value) return;
    try {
        if (navigator.clipboard && window.isSecureContext) {
            await navigator.clipboard.writeText(value);
        } else {
            const ta = document.createElement("textarea");
            ta.value = value;
            ta.style.position = "fixed";
            ta.style.left = "-9999px";
            document.body.appendChild(ta);
            ta.focus();
            ta.select();
            document.execCommand("copy");
            ta.remove();
        }
        if (btn) {
            const old = btn.textContent;
            btn.textContent = "已复制";
            btn.classList.add("copied");
            setTimeout(() => { btn.textContent = old || "复制"; btn.classList.remove("copied"); }, 1200);
        }
        msg("地址已复制", "ok");
    } catch (e) {
        msg("复制失败，请长按地址手动复制", "bad");
    }
}

function renderList(reset = false) {
    if (reset) currentPage = 1;

    const size = parseInt($("pageSize").value);
    const arr = filteredData();
    const pages = Math.ceil(arr.length / size) || 1;
    currentTotalPages = pages;
    if (currentPage > pages) currentPage = pages;

    $("pageInfo").textContent = `第 ${currentPage} / ${pages} 页`;
    $("prevBtn").disabled = currentPage === 1;
    $("nextBtn").disabled = currentPage === pages;
    const paginationEl = document.querySelector(".pagination");
    if (paginationEl) paginationEl.classList.toggle("one-page", pages <= 1);
    $("addressList").innerHTML = "";

    const part = arr.slice((currentPage - 1) * size, (currentPage - 1) * size + size);
    if (!part.length) {
        showEmpty("暂无匹配数据。可调整状态、搜索词或数据源筛选。");
        return;
    }

    part.forEach(x => {
        const s = cls(x.status_timer, x.status_type);
        const li = document.createElement("li");
        li.className = "record";
        li.innerHTML = `
            <div class="record-select">
                <div class="record-body">
                    <div class="record-head">
                        <input type="checkbox" class="record-check" value="${Number(x.id)}" aria-label="选择这条记录">
                        <div class="addr">${escapeHtml(x.content_text || "")}</div>
                        <button type="button" class="copy-btn">复制</button>
                        <span class="badge ${s}">${s === "safe" ? "纯净" : s === "danger" ? "风控" : "未知"}</span>
                    </div>
                    <div class="mobile-meta">
                        <span>${escapeHtml(formatRecordSourceName(x.source_name || "当前分类", x))}</span>
                        <span>${escapeHtml(x.status_timer || "未知状态")}</span>
                    </div>
                    <div class="meta">
                        <span>状态：${escapeHtml(x.status_timer || "未知状态")}</span>
                        <span>关键词：${escapeHtml(x.keyword || "-")}</span>
                        <span>更新：${escapeHtml(x.last_checked || "-")}</span>
                        <span class="source-label">分类：${escapeHtml(formatRecordSourceName(x.source_name || "-", x))}</span>
                        <span>当前网址：${escapeHtml(x.source_url || "-")}</span>
                    </div>
                </div>
            </div>
        `;
        const copyBtn = li.querySelector(".copy-btn");
        if (copyBtn) copyBtn.addEventListener("click", () => copyAddress(x.content_text || "", copyBtn));
        $("addressList").appendChild(li);
    });
}


function showEmpty(text) {
    $("addressList").innerHTML = `<li class="empty">${escapeHtml(text)}</li>`;
}

function changePage(d) {
    currentPage += d;
    renderList(false);
}

function jumpToPage() {
    const input = $("jumpPageInput");
    const n = parseInt(input ? input.value : "", 10);
    if (!Number.isInteger(n) || n < 1) { msg("请输入有效页码", "warn"); return; }
    currentPage = Math.min(Math.max(1, n), currentTotalPages || 1);
    if (input) input.value = "";
    renderList(false);
}

function exportCurrentCsv() {
    const params = new URLSearchParams();
    params.set("source_id", $("sourceFilter").value || "all");
    params.set("status_filter", currentStatus || "all");
    params.set("q", $("assetSearch").value.trim() || "");
    window.location.href = "/api/export_records?" + params.toString();
}

function selectedRecordIds() {
    return Array.from(document.querySelectorAll(".record-check:checked"))
        .map(x => parseInt(x.value))
        .filter(x => Number.isInteger(x) && x > 0);
}

async function deleteSelectedRecords() {
    const ids = selectedRecordIds();
    if (!ids.length) {
        msg("请先勾选要删除的具体记录。不会再按当前网址一键全删。", "warn");
        return;
    }

    const confirmText = prompt(`将只删除你勾选的 ${ids.length} 条记录。\n请输入 DELETE 确认：`);
    if (confirmText !== "DELETE") {
        msg("已取消删除。", "warn");
        return;
    }

    try {
        const r = await apiFetch("/api/records/bulk_delete", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ids}),
        });
        if (!r) return;
        const j = await r.json();
        msg(j.message || "删除完成", j.success ? "ok" : "bad");
        await fetchAll();
        fetchLogs();
    } catch (e) {
        msg("删除失败。", "bad");
    }
}


async function fetchSyncRuns() {
    try {
        const r = await apiFetch("/api/sync_runs");
        if (!r) return;
        const j = await r.json();
        syncRuns = j.data || [];
        renderSyncRuns();
    } catch (e) {}
}

function renderSyncRuns() {
    const home = $("homeSyncList");
    if (!home) return;
    home.innerHTML = "";

    const rows = (syncRuns || []).slice(0, 5);
    if (!rows.length) {
        home.innerHTML = `<div class="empty">暂无同步记录。</div>`;
        return;
    }

    rows.forEach(x => {
        let text = "未知", clsName = "unknown";
        if (x.status === "success") { text = "成功"; clsName = "safe"; }
        else if (x.status === "failed") { text = "失败"; clsName = "danger"; }
        else if (x.status === "empty") { text = "空结果"; clsName = "unknown"; }
        else if (x.status === "running") { text = "进行中"; clsName = "running"; }

        const item = document.createElement("div");
        item.className = "item";
        item.innerHTML = `
            <div class="item-head">
                <div class="item-title">${escapeHtml(x.keyword || "-")} ｜ ${escapeHtml(x.source_url || "-")}</div>
                <span class="badge ${clsName}">${text}</span>
            </div>
            <div class="item-meta">
                <span>解析：${x.total_found || 0}</span>
                <span>新增：${x.inserted_count || 0}</span>
                <span>更新：${x.updated_count || 0}</span>
                <span>时间：${escapeHtml(x.started_at || "-")}</span>
            </div>
        `;
        home.appendChild(item);
    });
}


async function loadTrash() {
    const box = $("trashList");
    if (!box) return;
    try {
        const r = await apiFetch("/api/records/trash");
        if (!r) return;
        const j = await r.json();
        const rows = j.data || [];
        if (!rows.length) { box.innerHTML = `<div class="empty">回收站为空。</div>`; return; }
        box.innerHTML = rows.map(x => `
            <div class="item">
                <label style="display:flex;gap:8px;align-items:flex-start;margin:0">
                    <input type="checkbox" class="trash-check" value="${Number(x.id)}" style="width:auto;min-height:auto;margin-top:3px">
                    <div>
                        <div class="item-title">${escapeHtml(x.content_text || "")}</div>
                        <div class="item-meta"><span>删除时间：${escapeHtml(x.deleted_at || "-")}</span><span>状态：${escapeHtml(x.status_timer || "-")}</span><span>关键词：${escapeHtml(x.keyword || "-")}</span></div>
                    </div>
                </label>
            </div>
        `).join("");
    } catch (e) { box.innerHTML = `<div class="empty">回收站加载失败。</div>`; }
}

function selectedTrashIds() {
    return Array.from(document.querySelectorAll(".trash-check:checked")).map(x => parseInt(x.value)).filter(x => Number.isInteger(x) && x > 0);
}

async function restoreSelectedTrash() {
    const ids = selectedTrashIds();
    if (!ids.length) { msg("请先勾选要恢复的资产。", "warn"); return; }
    const r = await apiFetch("/api/records/restore", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({ids})});
    if (!r) return;
    const j = await r.json();
    msg(j.message || "恢复完成", j.success ? "ok" : "bad");
    await fetchRecords();
    loadTrash();
}

async function purgeTrash() {
    const t = prompt("这会永久删除回收站内所有资产，无法恢复。请输入 DELETE 确认：");
    if (t !== "DELETE") { msg("已取消清空回收站。", "warn"); return; }
    const r = await apiFetch("/api/records/trash", {method:"DELETE"});
    if (!r) return;
    const j = await r.json();
    msg(j.message || "清空完成", j.success ? "ok" : "bad");
    loadTrash();
}

async function fetchLogs() {
    try {
        const r = await apiFetch("/api/logs");
        if (!r) return;
        const j = await r.json();
        const rows = j.data || [];
        $("logBox").textContent = rows.slice().reverse().map(x => `[${x.time}] [${x.level}] ${x.message}`).join("\n") || "暂无日志。";
    } catch (e) {
        $("logBox").textContent = "日志拉取失败。";
    }
}

async function cleanupMaintenance() {
    if (!confirm("确定清理旧日志和旧同步历史吗？")) return;
    try {
        const r = await apiFetch("/api/maintenance/cleanup", {method:"POST"});
        if (!r) return;
        const j = await r.json();
        msg(j.message || "清理完成", j.success ? "ok" : "bad");
        fetchLogs();
        fetchStats();
    } catch (e) {
        msg("清理失败。", "bad");
    }
}

async function loadBackups() {
    try {
        const r = await apiFetch("/api/backups");
        if (!r) return;
        const j = await r.json();
        backups = j.data || [];

        const box = $("backupList");
        box.innerHTML = "";
        if (!backups.length) {
            box.innerHTML = `<div class="empty">暂无本地备份。建议先点击“创建备份”。</div>`;
            return;
        }

        backups.forEach(x => {
            const item = document.createElement("div");
            item.className = "item";
            item.innerHTML = `
                <div class="item-head">
                    <div class="item-title">${escapeHtml(x.filename)}</div>
                    <span class="badge unknown">${formatSize(x.size)}</span>
                </div>
                <div class="item-meta">
                    <span>创建时间：${escapeHtml(x.modified_at || "-")}</span>
                </div>
                <div class="actions" style="margin-top:10px">
                    <button class="btn primary" onclick="downloadBackup('${escapeHtml(x.filename)}')">下载</button>
                    <button class="btn" onclick="mergeLocalBackup('${escapeHtml(x.filename)}')">合并导入</button>
                    <button class="btn danger" onclick="deleteBackup('${escapeHtml(x.filename)}')">删除</button>
                </div>
            `;
            box.appendChild(item);
        });
    } catch (e) {
        $("backupList").innerHTML = `<div class="empty">备份列表加载失败。</div>`;
    }
}

async function createBackup() {
    try {
        const r = await apiFetch("/api/backups/create", {method:"POST"});
        if (!r) return;
        const j = await r.json();
        msg(j.message || "备份已创建", j.success ? "ok" : "bad");
        await loadBackups();
        fetchLogs();
    } catch (e) {
        msg("创建备份失败。", "bad");
    }
}

function downloadBackup(filename) {
    window.location.href = "/api/backups/download/" + encodeURIComponent(filename);
}

function downloadLatestBackup() {
    if (!backups.length) {
        msg("暂无可下载备份，请先创建备份。", "warn");
        return;
    }
    downloadBackup(backups[0].filename);
}

async function deleteBackup(filename) {
    if (!confirm("确定删除服务器上的这个本地备份吗？删除后无法从服务器恢复它。")) return;

    try {
        const r = await apiFetch("/api/backups/" + encodeURIComponent(filename), {method:"DELETE"});
        if (!r) return;
        const j = await r.json();
        msg(j.message || "备份已删除", j.success ? "ok" : "bad");
        loadBackups();
        fetchLogs();
    } catch (e) {
        msg("删除备份失败。", "bad");
    }
}

async function mergeLocalBackup(filename) {
    if (!confirm("确定合并导入这个备份吗？合并不会删除当前数据，会自动去重。")) return;

    try {
        const r = await apiFetch("/api/backups/merge_local", {
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({filename}),
        });
        if (!r) return;

        const j = await r.json();
        const res = j.result || {};
        msg(`${j.message || "合并完成"}\n新增资产：${res.assets || 0}\n新增数据源：${res.sources || 0}\n新增日志：${res.logs || 0}\n新增同步历史：${res.sync_runs || 0}`, j.success ? "ok" : "bad");
        fetchAll();
        fetchLogs();
        fetchSyncRuns();
    } catch (e) {
        msg("合并失败。", "bad");
    }
}

async function mergeUploadBackup() {
    const file = $("backupFile").files[0];
    if (!file) {
        msg("请先选择一个 .tar.gz 备份包。", "warn");
        return;
    }

    if (!confirm("确定上传并合并这个备份吗？合并不会删除当前数据，会自动去重。")) return;

    const fd = new FormData();
    fd.append("file", file);

    try {
        const r = await apiFetch("/api/backups/merge_upload", {method:"POST", body:fd});
        if (!r) return;

        const j = await r.json();
        const res = j.result || {};
        msg(`${j.message || "合并完成"}\n新增资产：${res.assets || 0}\n新增数据源：${res.sources || 0}\n新增日志：${res.logs || 0}\n新增同步历史：${res.sync_runs || 0}`, j.success ? "ok" : "bad");
        fetchAll();
        fetchLogs();
        fetchSyncRuns();
        loadBackups();
    } catch (e) {
        msg("上传合并失败。", "bad");
    }
}

async function calibrateStatus() {
    if (!confirm("确定执行状态校准吗？系统会把倒计时已经结束的资产自动转为纯净可用。")) return;

    try {
        const r = await apiFetch("/api/maintenance/calibrate_status", {method:"POST"});
        if (!r) return;

        const j = await r.json();
        msg(j.message || "状态校准完成", j.success ? "ok" : "bad");

        await fetchAll();
        fetchLogs();
        fetchSyncRuns();
    } catch (e) {
        msg("状态校准失败。", "bad");
    }
}


</script>
</body>
</html>
"""
    return html.replace("__USERNAME__", html_escape(username))
