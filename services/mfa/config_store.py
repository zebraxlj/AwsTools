"""
持久化存储：管理收藏状态等 UI 配置。
存储位置：%LOCALAPPDATA%/AwsTools/mfa_ui_config.json
"""

import json
import os
from pathlib import Path


def _get_config_dir() -> Path:
    """获取配置目录路径"""
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        base = Path(local_app_data)
    else:
        # 非 Windows 回退
        base = Path.home() / ".local" / "share"
    return base / "AwsTools"


def _get_config_path() -> Path:
    return _get_config_dir() / "mfa_ui_config.json"


def _load_config() -> dict:
    """加载配置文件"""
    config_path = _get_config_path()
    if not config_path.exists():
        return {}
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_config(config: dict):
    """保存配置文件"""
    config_dir = _get_config_dir()
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = _get_config_path()
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def get_starred_profiles() -> set[str]:
    """获取所有收藏的 profile 名称"""
    config = _load_config()
    return set(config.get("starred", []))


def set_profile_starred(profile_name: str, starred: bool):
    """设置 profile 的收藏状态"""
    config = _load_config()
    starred_list: list[str] = config.get("starred", [])

    if starred and profile_name not in starred_list:
        starred_list.append(profile_name)
    elif not starred and profile_name in starred_list:
        starred_list.remove(profile_name)

    config["starred"] = starred_list
    _save_config(config)


def toggle_profile_starred(profile_name: str) -> bool:
    """切换 profile 的收藏状态，返回新的状态"""
    starred = get_starred_profiles()
    new_state = profile_name not in starred
    set_profile_starred(profile_name, new_state)
    return new_state
