"""
CloudWatch Log Groups 服务层：获取指定 profile 下的所有日志组。
纯逻辑层，不依赖 PyQt。

两阶段拉取：
  1. fetch_log_groups: 分页拉取日志组列表（describe_log_groups）
  2. fetch_last_ingestion_time: 对单个日志组查 describe_log_streams 取 lastIngestionTime

全部 API 调用均接受外部传入的 RateLimiter 限速。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import boto3

from utils.rate_limiter import RateLimiter


@dataclass
class LogGroupInfo:
    """单个 CloudWatch Log Group 的信息"""
    log_group_name: str
    region: str
    creation_time: Optional[datetime] = None
    last_ingestion_time: Optional[datetime] = None
    stored_bytes: Optional[int] = None
    retention_in_days: Optional[int] = None

    @property
    def console_url(self) -> str:
        from utils.aws_urls import get_cloud_watch_log_group_url
        return get_cloud_watch_log_group_url(self.region, self.log_group_name)


def _ms_to_utc(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def fetch_log_groups(
    profile_name: str,
    region: str,
    limiter: Optional[RateLimiter] = None,
) -> list[LogGroupInfo]:
    """
    第一阶段：分页拉取日志组列表。

    describe_log_groups 在中国区可能不返回 lastIngestionTime，
    有返回时会直接填入，无返回的留 None 交由第二阶段补充。
    """
    if limiter is None:
        limiter = RateLimiter.for_cloudwatch()

    session = boto3.Session(profile_name=profile_name, region_name=region)
    client = session.client("logs")

    try:
        groups: list[LogGroupInfo] = []

        # 手动分页：在每次 API 调用前 acquire，语义精确
        next_token: Optional[str] = None
        while True:
            limiter.acquire()
            kwargs: dict = {}
            if next_token:
                kwargs["nextToken"] = next_token
            resp = client.describe_log_groups(**kwargs)

            for lg in resp.get("logGroups", []):
                groups.append(LogGroupInfo(
                    log_group_name=lg["logGroupName"],
                    region=region,
                    creation_time=_ms_to_utc(lg.get("creationTime")),
                    last_ingestion_time=_ms_to_utc(lg.get("lastIngestionTime")),
                    stored_bytes=lg.get("storedBytes"),
                    retention_in_days=lg.get("retentionInDays"),
                ))

            next_token = resp.get("nextToken")
            if not next_token:
                break

        groups.sort(key=lambda g: g.log_group_name.lower())
        return groups
    finally:
        client.close()


def fetch_last_ingestion_time(
    client,
    log_group_name: str,
    limiter: RateLimiter,
) -> Optional[datetime]:
    """
    第二阶段（单个）：调用 describe_log_streams 获取最后写入时间。

    :return: lastIngestionTime 的 UTC datetime，获取失败或无数据时返回 None
    """
    try:
        limiter.acquire()
        resp = client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy="LastEventTime",
            descending=True,
            limit=1,
        )
        streams = resp.get("logStreams", [])
        if streams:
            return _ms_to_utc(streams[0].get("lastIngestionTime"))
    except Exception:
        pass
    return None
