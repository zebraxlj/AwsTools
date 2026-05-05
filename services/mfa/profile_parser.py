"""
解析 ~/.aws/credentials 和 ~/.aws/config，
识别 MFA profile 对（xxx-long-term ↔ xxx），计算过期状态。
"""

import configparser
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional


class ProfileStatus(Enum):
    """Profile 会话状态"""
    ACTIVE = "active"         # 未过期
    EXPIRED = "expired"       # 已过期
    NO_SESSION = "no_session"  # 无会话信息（从未刷新过）


class RegionGroup(Enum):
    """区域分组"""
    CN = "CN"
    US = "US"
    OTHER = "OTHER"


@dataclass
class LongTermInfo:
    """长期凭证信息（来自 xxx-long-term profile）"""
    profile_name: str            # e.g. "default-long-term"
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_mfa_device: str          # MFA device ARN
    assume_role: Optional[str] = None  # role ARN（如果有则用 AssumeRole）


@dataclass
class SessionInfo:
    """会话凭证信息（来自 xxx profile）"""
    profile_name: str
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    expiration: Optional[datetime] = None
    assumed_role_arn: Optional[str] = None


@dataclass
class MfaProfile:
    """一个完整的 MFA profile 对"""
    session_name: str            # 会话 profile 名称，e.g. "default", "cn-aws-tools"
    long_term: LongTermInfo      # 长期凭证
    session: Optional[SessionInfo] = None  # 当前会话凭证（可能不存在）
    region: Optional[str] = None
    duration_seconds: Optional[int] = None  # get_session_token_duration_seconds

    @property
    def region_group(self) -> RegionGroup:
        if self.region and self.region.startswith("cn-"):
            return RegionGroup.CN
        elif self.region:
            return RegionGroup.US
        # 从 MFA device ARN 判断
        if "aws-cn" in self.long_term.aws_mfa_device:
            return RegionGroup.CN
        return RegionGroup.US

    def get_status_and_remaining(self) -> tuple[ProfileStatus, Optional[int]]:
        """
        一次性获取状态和剩余秒数，避免多次调用 datetime.now() 导致边界不一致。
        返回 (status, remaining_seconds)，remaining_seconds 仅在 ACTIVE 时有值。
        """
        if self.session is None or self.session.expiration is None:
            return ProfileStatus.NO_SESSION, None
        now = datetime.now(timezone.utc)
        delta = (self.session.expiration - now).total_seconds()
        if delta > 0:
            return ProfileStatus.ACTIVE, max(0, int(delta))
        return ProfileStatus.EXPIRED, None

    @property
    def status(self) -> ProfileStatus:
        s, _ = self.get_status_and_remaining()
        return s

    @property
    def remaining_seconds(self) -> Optional[int]:
        """剩余秒数，已过期或无会话时返回 None"""
        _, r = self.get_status_and_remaining()
        return r

    @property
    def display_name(self) -> str:
        return self.session_name

    @property
    def role_arn(self) -> Optional[str]:
        return self.long_term.assume_role

    @property
    def mfa_device(self) -> str:
        return self.long_term.aws_mfa_device


def _get_aws_dir() -> Path:
    return Path.home() / ".aws"


def _parse_ini_file(filepath: Path) -> dict[str, dict[str, str]]:
    """
    解析 INI 文件，返回 {section_name: {key: value}} 的字典。
    对 config 文件中的 'profile xxx' 格式自动去掉前缀。
    """
    if not filepath.exists():
        return {}

    parser = configparser.RawConfigParser()
    parser.optionxform = str  # 保持 key 原始大小写
    parser.read(str(filepath), encoding="utf-8")

    result = {}
    for section in parser.sections():
        # config 文件中 profile 名称格式为 "profile xxx"，去掉前缀
        name = section
        if name.startswith("profile "):
            name = name[len("profile "):]
        result[name] = dict(parser.items(section))
    return result


_EXPIRATION_FORMATS = [
    "%Y-%m-%d %H:%M:%S",       # 2026-05-02 15:25:07
    "%Y-%m-%dT%H:%M:%S%z",     # 2026-04-29T17:22:08+00:00
    "%Y-%m-%dT%H:%M:%S",       # 2026-04-29T17:22:08
]


def parse_expiration(value: str) -> Optional[datetime]:
    """解析过期时间字符串，支持多种格式"""
    value = value.strip()
    for fmt in _EXPIRATION_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            # 如果没有时区信息，假定 UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def parse_profiles() -> list[MfaProfile]:
    """
    解析 AWS 配置文件，返回所有 MFA profile 对的列表。

    匹配规则：
    - credentials 中存在 'xxx-long-term' section，且包含 aws_mfa_device 字段
    - 对应的会话 profile 为 'xxx'
    """
    aws_dir = _get_aws_dir()
    credentials = _parse_ini_file(aws_dir / "credentials")
    config = _parse_ini_file(aws_dir / "config")

    profiles: list[MfaProfile] = []

    # 找出所有 long-term profiles
    for section_name, section_data in credentials.items():
        if not section_name.endswith("-long-term"):
            continue
        if "aws_mfa_device" not in section_data:
            continue

        # 构建 long-term info
        long_term = LongTermInfo(
            profile_name=section_name,
            aws_access_key_id=section_data.get("aws_access_key_id", ""),
            aws_secret_access_key=section_data.get("aws_secret_access_key", ""),
            aws_mfa_device=section_data["aws_mfa_device"],
            assume_role=section_data.get("assume_role"),
        )

        # 对应的 session profile 名称
        session_name = section_name[: -len("-long-term")]

        # 解析 session profile
        session_info = None
        if session_name in credentials:
            sess_data = credentials[session_name]
            expiration = None
            if "expiration" in sess_data:
                expiration = parse_expiration(sess_data["expiration"])

            session_info = SessionInfo(
                profile_name=session_name,
                aws_access_key_id=sess_data.get("aws_access_key_id"),
                aws_secret_access_key=sess_data.get("aws_secret_access_key"),
                aws_session_token=sess_data.get("aws_session_token"),
                expiration=expiration,
                assumed_role_arn=sess_data.get("assumed_role_arn"),
            )

        # 从 config 文件获取 region 和 duration
        config_data = config.get(session_name, {})
        region = config_data.get("region")
        duration_str = config_data.get("get_session_token_duration_seconds")
        duration = int(duration_str) if duration_str else None

        profiles.append(MfaProfile(
            session_name=session_name,
            long_term=long_term,
            session=session_info,
            region=region,
            duration_seconds=duration,
        ))

    # 按区域分组排序：CN 在前，US 在后；组内按名称排序
    group_order = {RegionGroup.CN: 0, RegionGroup.US: 1, RegionGroup.OTHER: 2}
    profiles.sort(key=lambda p: (group_order[p.region_group], p.session_name))

    return profiles
