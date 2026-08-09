"""
CloudWatch Log Groups 本地缓存。

缓存策略：
- 有缓存时直接读取并展示，不自动重新请求 AWS
- 无缓存时自动触发一次拉取
- 用户手动点"刷新"强制重新拉取并更新缓存

缓存文件路径（与 MFA config_store 同目录）：
  Windows: %LOCALAPPDATA%/AwsTools/cw_log_groups_<profile>_<region>.json
  其他:     ~/.local/share/AwsTools/cw_log_groups_<profile>_<region>.json

缓存文件格式：
{
    "fetched_at": "2026-05-07T10:00:00+00:00",   # ISO 8601 UTC 时间戳
    "profile": "my-profile",
    "region": "cn-northwest-1",
    "groups": [
        {
            "log_group_name": "/aws/lambda/Foo",
            "creation_time": "2024-01-01T00:00:00+00:00",   # 可为 null
            "last_ingestion_time": "2026-05-06T12:00:00+00:00",  # 可为 null
            "stored_bytes": 12345,
            "retention_in_days": 30
        },
        ...
    ]
}
"""

from __future__ import annotations

import json
import os
import platform
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from services.cloudwatch.log_groups_service import LogGroupInfo


# ── 缓存目录（复用 MFA config_store 的逻辑） ─────────────────

def _cache_dir() -> Path:
    if platform.system() == "Windows":
        base = os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
    else:
        base = str(Path.home() / ".local" / "share")
    d = Path(base) / "AwsTools"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _safe_key(s: str) -> str:
    """将 profile/region 中不适合作文件名的字符替换为 _"""
    return re.sub(r'[^\w\-]', '_', s)


def _cache_path(profile: str, region: str) -> Path:
    return _cache_dir() / f"cw_log_groups_{_safe_key(profile)}_{_safe_key(region)}.json"


# ── 序列化 / 反序列化 ────────────────────────────────────────

def _dt_to_str(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.isoformat()


def _str_to_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ── 公开 API ────────────────────────────────────────────────

def get_cache_dir() -> Path:
    """返回缓存目录路径（供 UI 打开文件夹用）"""
    return _cache_dir()


def open_cache_dir() -> None:
    """用系统文件管理器打开缓存目录，全平台支持。"""
    import platform
    import subprocess

    path = str(_cache_dir())
    system = platform.system()
    if system == "Windows":
        subprocess.Popen(["explorer.exe", path])
    elif system == "Darwin":
        subprocess.Popen(["open", path])
    else:
        # Linux / BSD：依赖桌面环境提供的 xdg-open
        subprocess.Popen(["xdg-open", path])

def load_cache(profile: str, region: str) -> tuple[list[LogGroupInfo], Optional[datetime]]:
    """
    读取本地缓存。

    返回 (groups, fetched_at)：
    - 无缓存或解析失败时返回 ([], None)
    - fetched_at 为上次拉取的 UTC datetime
    """
    path = _cache_path(profile, region)
    if not path.exists():
        return [], None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        fetched_at = _str_to_dt(data.get("fetched_at"))
        groups = [
            LogGroupInfo(
                log_group_name=g["log_group_name"],
                region=region,
                creation_time=_str_to_dt(g.get("creation_time")),
                last_ingestion_time=_str_to_dt(g.get("last_ingestion_time")),
                stored_bytes=g.get("stored_bytes"),
                retention_in_days=g.get("retention_in_days"),
            )
            for g in data.get("groups", [])
        ]
        return groups, fetched_at
    except Exception:
        return [], None


def save_cache(profile: str, region: str, groups: list[LogGroupInfo]) -> datetime:
    """
    将日志组列表写入本地缓存，返回写入时的 UTC 时间戳。
    """
    fetched_at = datetime.now(timezone.utc)
    path = _cache_path(profile, region)
    data = {
        "fetched_at": _dt_to_str(fetched_at),
        "profile": profile,
        "region": region,
        "groups": [
            {
                "log_group_name": g.log_group_name,
                "creation_time": _dt_to_str(g.creation_time),
                "last_ingestion_time": _dt_to_str(g.last_ingestion_time),
                "stored_bytes": g.stored_bytes,
                "retention_in_days": g.retention_in_days,
            }
            for g in groups
        ],
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return fetched_at
