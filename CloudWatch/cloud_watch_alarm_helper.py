"""CloudWatch Alarm 操作：列出告警、修改告警状态。

纯逻辑层，不依赖 PyQt。所有 API 调用都受 RateLimiter 限速，
`cancel` 事件可用于中途取消（供 UI 关闭/切换查询时使用）。
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Iterable, Optional

import boto3

from CloudWatch.cloud_watch_alarm_dataclass import AlarmInfo, AlarmStateValue
from utils.rate_limiter import Cancelled, RateLimiter


def _ts_to_utc(ts) -> Optional[datetime]:
    """describe_alarms 返回的是 datetime（tz-aware），传 None 则保持 None。"""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return None


def _build_client(profile_name: str, region: str):
    session = boto3.Session(profile_name=profile_name, region_name=region)
    return session.client("cloudwatch")


def fetch_alarms(
    profile_name: str,
    region: str,
    state_value: Optional[AlarmStateValue] = None,
    limiter: Optional[RateLimiter] = None,
    cancel: Optional[threading.Event] = None,
) -> list[AlarmInfo]:
    """分页拉取 Metric Alarms。

    :param state_value: 传入时只返回该状态下的告警（服务端过滤）
    :raises Cancelled: 分页途中被 `cancel` 中断
    """
    if limiter is None:
        limiter = RateLimiter.for_cloudwatch()

    client = _build_client(profile_name, region)
    try:
        alarms: list[AlarmInfo] = []
        next_token: Optional[str] = None
        while True:
            if not limiter.acquire(cancel=cancel):
                raise Cancelled("fetch_alarms cancelled")

            kwargs: dict = {"AlarmTypes": ["MetricAlarm"]}
            if state_value is not None:
                kwargs["StateValue"] = state_value.value
            if next_token:
                kwargs["NextToken"] = next_token

            resp = client.describe_alarms(**kwargs)

            for a in resp.get("MetricAlarms", []):
                try:
                    state = AlarmStateValue(a["StateValue"])
                except (KeyError, ValueError):
                    continue
                alarms.append(AlarmInfo(
                    alarm_name=a["AlarmName"],
                    region=region,
                    state_value=state,
                    state_reason=a.get("StateReason"),
                    state_updated_timestamp=_ts_to_utc(a.get("StateUpdatedTimestamp")),
                    metric_name=a.get("MetricName"),
                    namespace=a.get("Namespace"),
                    alarm_description=a.get("AlarmDescription"),
                    alarm_arn=a.get("AlarmArn"),
                ))

            next_token = resp.get("NextToken")
            if not next_token:
                break

        alarms.sort(key=lambda x: x.alarm_name.lower())
        return alarms
    finally:
        client.close()


def set_alarm_state(
    profile_name: str,
    region: str,
    alarm_name: str,
    state_value: AlarmStateValue,
    state_reason: str,
    limiter: Optional[RateLimiter] = None,
    cancel: Optional[threading.Event] = None,
) -> None:
    """将单个告警设置为指定状态。

    AWS 要求 StateReason 长度 1~1023 字符，这里按上限截断。
    """
    if not state_reason:
        raise ValueError("state_reason must be a non-empty string")
    if limiter is None:
        limiter = RateLimiter.for_cloudwatch()

    client = _build_client(profile_name, region)
    try:
        if not limiter.acquire(cancel=cancel):
            raise Cancelled("set_alarm_state cancelled")
        client.set_alarm_state(
            AlarmName=alarm_name,
            StateValue=state_value.value,
            StateReason=state_reason[:1023],
        )
    finally:
        client.close()


def set_alarm_states_batch(
    profile_name: str,
    region: str,
    alarm_names: Iterable[str],
    state_value: AlarmStateValue,
    state_reason: str,
    limiter: Optional[RateLimiter] = None,
    cancel: Optional[threading.Event] = None,
) -> list[tuple[str, Optional[str]]]:
    """批量修改告警状态，返回 [(alarm_name, error_or_None), ...]。

    共用同一个 client + limiter，避免每次都建 session。
    单个告警失败不会中断整个批次；被 cancel 时提前退出并把剩余告警标为取消错误。
    """
    if not state_reason:
        raise ValueError("state_reason must be a non-empty string")
    if limiter is None:
        limiter = RateLimiter.for_cloudwatch()

    names = list(alarm_names)
    results: list[tuple[str, Optional[str]]] = []
    client = _build_client(profile_name, region)
    try:
        for name in names:
            if cancel is not None and cancel.is_set():
                results.append((name, "cancelled"))
                continue
            if not limiter.acquire(cancel=cancel):
                results.append((name, "cancelled"))
                continue
            try:
                client.set_alarm_state(
                    AlarmName=name,
                    StateValue=state_value.value,
                    StateReason=state_reason[:1023],
                )
                results.append((name, None))
            except Exception as exc:
                results.append((name, str(exc)))
    finally:
        client.close()
    return results
