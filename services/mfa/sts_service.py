"""
STS 调用服务：根据 MFA profile 的 long-term 凭证 + MFA code，
调用 AssumeRole 或 GetSessionToken 获取临时凭证，并写回 credentials 文件。
"""

import configparser
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config as BotoConfig

from services.mfa.profile_parser import MfaProfile, SessionInfo

# credentials 文件写入锁，防止多个 worker 同时写导致数据丢失
_credentials_lock = threading.Lock()


@dataclass
class StsResult:
    """STS 调用结果"""
    success: bool
    error_message: Optional[str] = None
    session_info: Optional[SessionInfo] = None


def _get_sts_client(profile: MfaProfile):
    """
    根据 profile 的 region 创建 STS client。
    使用 long-term 凭证，不使用 session token。
    """
    region = profile.region or "us-east-1"

    # 中国区需要使用特定的 endpoint
    endpoint_url = None
    if region.startswith("cn-"):
        endpoint_url = f"https://sts.{region}.amazonaws.com.cn"

    client = boto3.client(
        "sts",
        aws_access_key_id=profile.long_term.aws_access_key_id,
        aws_secret_access_key=profile.long_term.aws_secret_access_key,
        region_name=region,
        endpoint_url=endpoint_url,
        config=BotoConfig(
            connect_timeout=10,
            read_timeout=30,
        ),
    )
    return client


def refresh_mfa_session(profile: MfaProfile, mfa_code: str) -> StsResult:
    """
    使用 MFA code 刷新 profile 的临时凭证。

    根据 long-term profile 是否配置了 assume_role 决定调用方式：
    - 有 assume_role：调用 sts:AssumeRole（带 MFA）
    - 无 assume_role：调用 sts:GetSessionToken（带 MFA）

    成功后将临时凭证写回 ~/.aws/credentials 文件。
    """
    try:
        client = _get_sts_client(profile)

        duration = profile.duration_seconds or 3600

        if profile.long_term.assume_role:
            # AssumeRole with MFA
            response = client.assume_role(
                RoleArn=profile.long_term.assume_role,
                RoleSessionName="mfa-ui-session",
                DurationSeconds=duration,
                SerialNumber=profile.long_term.aws_mfa_device,
                TokenCode=mfa_code,
            )
            credentials = response["Credentials"]
            assumed_role_arn = profile.long_term.assume_role
        else:
            # GetSessionToken with MFA
            response = client.get_session_token(
                DurationSeconds=duration,
                SerialNumber=profile.long_term.aws_mfa_device,
                TokenCode=mfa_code,
            )
            credentials = response["Credentials"]
            assumed_role_arn = None

        # 构建 SessionInfo
        # boto3 返回的 Expiration 是 datetime 对象（已有 tzinfo）
        expiration_dt: datetime = credentials["Expiration"]
        if expiration_dt.tzinfo is None:
            expiration_dt = expiration_dt.replace(tzinfo=timezone.utc)

        session_info = SessionInfo(
            profile_name=profile.session_name,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            expiration=expiration_dt,
            assumed_role_arn=assumed_role_arn,
        )

        # 写回 credentials 文件
        _write_session_to_credentials(profile, session_info)

        return StsResult(success=True, session_info=session_info)

    except Exception as e:
        error_msg = str(e)
        # 提取 botocore 错误中的有用信息
        if hasattr(e, "response"):
            error_detail = e.response.get("Error", {})
            code = error_detail.get("Code", "")
            message = error_detail.get("Message", "")
            if code and message:
                error_msg = f"{code}: {message}"
        return StsResult(success=False, error_message=error_msg)


def _write_session_to_credentials(profile: MfaProfile, session: SessionInfo):
    """将临时凭证写回 ~/.aws/credentials 文件（线程安全）"""
    cred_path = Path.home() / ".aws" / "credentials"

    with _credentials_lock:
        parser = configparser.RawConfigParser()
        parser.optionxform = str  # 保持 key 原始大小写

        if cred_path.exists():
            parser.read(str(cred_path), encoding="utf-8")

        section = profile.session_name
        if not parser.has_section(section):
            parser.add_section(section)

        # 写入凭证字段
        if profile.long_term.assume_role:
            parser.set(section, "assumed_role", "True")
            parser.set(section, "assumed_role_arn", profile.long_term.assume_role)

        parser.set(section, "aws_access_key_id", session.aws_access_key_id)
        parser.set(section, "aws_secret_access_key", session.aws_secret_access_key)
        parser.set(section, "aws_session_token", session.aws_session_token)
        parser.set(section, "aws_security_token", session.aws_session_token)

        # 过期时间格式：2026-05-02 15:25:07
        if session.expiration:
            exp_str = session.expiration.strftime("%Y-%m-%d %H:%M:%S")
            parser.set(section, "expiration", exp_str)

        with open(str(cred_path), "w", encoding="utf-8") as f:
            parser.write(f)
