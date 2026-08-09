"""
CloudWatch Log Groups Workers：

1. FetchLogGroupsWorker    — 第一阶段：拉日志组列表 + 写缓存
2. FetchIngestionTimeWorker — 第二阶段：并发补充 lastIngestionTime，逐条通过信号回填

两个 worker 都支持 cancel()：调用后放弃剩余 API 调用并停止发射信号。
UI 侧必须在切换查询条件和关闭窗口时调用，否则 QThreadPool 析构会一直等到
限速队列跑完（几百个日志组 @5TPS 可达数分钟），且 signals 的 C++ 对象
已随 widget 销毁，emit 会抛 RuntimeError。
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Optional

import boto3
from PyQt5.QtCore import QObject, QRunnable, pyqtSignal, pyqtSlot

from services.cloudwatch.log_groups_cache import save_cache
from services.cloudwatch.log_groups_service import (
    LogGroupInfo,
    fetch_last_ingestion_time,
    fetch_log_groups,
)
from utils.rate_limiter import Cancelled, RateLimiter

_log = logging.getLogger(__name__)


# ── 可取消 worker 基类 ─────────────────────────────────


class _CancellableWorker(QRunnable):
    """带取消开关 + 安全 emit 的 QRunnable 基类。"""

    def __init__(self):
        super().__init__()
        self._stop = threading.Event()

    def cancel(self) -> None:
        """请求取消。可从 UI 线程调用；worker 会在下一个检查点退出。"""
        self._stop.set()

    @property
    def cancelled(self) -> bool:
        return self._stop.is_set()

    def _emit(self, emitter: Callable[[], None]) -> None:
        """
        安全发射信号：已取消则跳过；signals 的 C++ 对象已销毁时静默忽略。

        emitter 用 lambda 包住，保证属性访问也在 try 内
        （访问已删除 QObject 的属性同样会抛 RuntimeError）。
        """
        if self._stop.is_set():
            return
        try:
            emitter()
        except RuntimeError:
            # wrapped C/C++ object of type ... has been deleted
            _log.debug("signal target already destroyed, emit skipped")


# ── 第一阶段：拉日志组列表 ──────────────────────────────


class _ListSignals(QObject):
    # (log_groups, fetched_at, error_message)
    finished = pyqtSignal(list, object, str)


class FetchLogGroupsWorker(_CancellableWorker):
    """拉日志组列表 + 写缓存，完成后发射 finished 信号。"""

    def __init__(self, profile_name: str, region: str):
        super().__init__()
        self._profile_name = profile_name
        self._region = region
        self.signals = _ListSignals()

    @pyqtSlot()
    def run(self):
        try:
            groups = fetch_log_groups(
                self._profile_name, self._region, cancel=self._stop
            )
        except Cancelled:
            return
        except Exception as exc:
            self._emit(lambda: self.signals.finished.emit([], None, str(exc)))
            return

        # 取消后不再写缓存，避免把半份数据落盘
        if self._stop.is_set():
            return
        fetched_at = save_cache(self._profile_name, self._region, groups)
        self._emit(lambda: self.signals.finished.emit(groups, fetched_at, ""))


# ── 第二阶段：逐条补充 lastIngestionTime ────────────────


class _IngestionSignals(QObject):
    # 每补充完一条：(log_group_name, last_ingestion_time_or_None)
    item_ready = pyqtSignal(str, object)
    # 全部完成
    all_done = pyqtSignal()


class FetchIngestionTimeWorker(_CancellableWorker):
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
        pool = None
        try:
            limiter = RateLimiter.for_cloudwatch()
            session = boto3.Session(
                profile_name=self._profile_name,
                region_name=self._region,
            )
            client = session.client("logs")

            max_workers = max(1, int(limiter.rate))

            pool = ThreadPoolExecutor(max_workers=max_workers)
            futures = {
                pool.submit(self._fetch_one, client, name, limiter): name
                for name in self._names
            }
            for fut in as_completed(futures):
                if self._stop.is_set():
                    break
                name = futures[fut]
                try:
                    ts = fut.result()
                except Exception:
                    ts = None
                self._emit(lambda n=name, t=ts: self.signals.item_ready.emit(n, t))
        except Exception as exc:
            _log.warning("Failed to fetch lastIngestionTime: %s", exc)
        finally:
            if pool is not None:
                # 取消时丢弃尚未启动的任务，让线程池迅速 drain；
                # 必须先 shutdown 再 close client（在跑的任务还在用它）
                pool.shutdown(wait=True, cancel_futures=self._stop.is_set())
            if client is not None:
                client.close()
            self._emit(lambda: self.signals.all_done.emit())

    def _fetch_one(self, client, name: str, limiter: RateLimiter) -> Optional[datetime]:
        """单个日志组的查询任务；已取消时直接返回，避免多余的 API 调用。"""
        if self._stop.is_set():
            return None
        return fetch_last_ingestion_time(client, name, limiter, cancel=self._stop)
