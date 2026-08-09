"""
CloudWatch Log Groups Workers：

1. FetchLogGroupsWorker    — 第一阶段：拉日志组列表 + 写缓存
2. FetchIngestionTimeWorker — 第二阶段：并发补充 lastIngestionTime，逐条通过信号回填
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import boto3
from PyQt5.QtCore import QObject, QRunnable, pyqtSignal, pyqtSlot

from services.cloudwatch.log_groups_cache import save_cache
from services.cloudwatch.log_groups_service import (
    LogGroupInfo,
    fetch_last_ingestion_time,
    fetch_log_groups,
)
from utils.rate_limiter import RateLimiter

_log = logging.getLogger(__name__)


# ── 第一阶段：拉日志组列表 ──────────────────────────────


class _ListSignals(QObject):
    # (log_groups, fetched_at, error_message)
    finished = pyqtSignal(list, object, str)


class FetchLogGroupsWorker(QRunnable):
    """拉日志组列表 + 写缓存，完成后发射 finished 信号。"""

    def __init__(self, profile_name: str, region: str):
        super().__init__()
        self._profile_name = profile_name
        self._region = region
        self.signals = _ListSignals()

    @pyqtSlot()
    def run(self):
        try:
            groups = fetch_log_groups(self._profile_name, self._region)
            fetched_at = save_cache(self._profile_name, self._region, groups)
            self.signals.finished.emit(groups, fetched_at, "")
        except Exception as exc:
            self.signals.finished.emit([], None, str(exc))


# ── 第二阶段：逐条补充 lastIngestionTime ────────────────


class _IngestionSignals(QObject):
    # 每补充完一条：(log_group_name, last_ingestion_time_or_None)
    item_ready = pyqtSignal(str, object)
    # 全部完成
    all_done = pyqtSignal()


class FetchIngestionTimeWorker(QRunnable):
    """
    后台并发获取 lastIngestionTime，每完成一个日志组就发射 item_ready 信号。

    只处理 last_ingestion_time 为 None 的日志组。
    """

    def __init__(self, profile_name: str, region: str, log_group_names: list[str]):
        super().__init__()
        self._profile_name = profile_name
        self._region = region
        self._names = log_group_names
        self.signals = _IngestionSignals()

    @pyqtSlot()
    def run(self):
        client = None
        try:
            limiter = RateLimiter.for_cloudwatch()
            session = boto3.Session(
                profile_name=self._profile_name,
                region_name=self._region,
            )
            client = session.client("logs")

            max_workers = max(1, int(limiter.rate))

            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        fetch_last_ingestion_time, client, name, limiter
                    ): name
                    for name in self._names
                }
                for fut in as_completed(futures):
                    name = futures[fut]
                    try:
                        ts = fut.result()
                    except Exception:
                        ts = None
                    self.signals.item_ready.emit(name, ts)
        except Exception as exc:
            _log.warning("Failed to fetch lastIngestionTime: %s", exc)
        finally:
            if client is not None:
                client.close()
            self.signals.all_done.emit()
