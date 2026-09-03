"""CloudWatch Alarm 相关的数据结构。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class AlarmStateValue(Enum):
    """CloudWatch Alarm 的三种官方状态。"""
    OK = "OK"
    ALARM = "ALARM"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@dataclass
class AlarmInfo:
    """单个 CloudWatch Metric Alarm 的信息（describe_alarms 返回子集）。"""
    alarm_name: str
    region: str
    state_value: AlarmStateValue
    state_reason: Optional[str] = None
    state_updated_timestamp: Optional[datetime] = None
    metric_name: Optional[str] = None
    namespace: Optional[str] = None
    alarm_description: Optional[str] = None
    alarm_arn: Optional[str] = None

    @property
    def console_url(self) -> str:
        from utils.aws_urls import get_cloud_watch_alarm_url
        return get_cloud_watch_alarm_url(self.region, self.alarm_name)
