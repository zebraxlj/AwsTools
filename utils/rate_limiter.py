"""
通用令牌桶限速器（线程安全）。

用法：
    limiter = RateLimiter(rate=5, capacity=10)   # 5 tokens/s，桶最多存 10 个

    # 同步调用前 acquire：
    limiter.acquire()
    client.describe_log_streams(...)

    # 或用上下文管理器：
    with limiter:
        client.describe_log_groups(...)

    # 可中断等待（UI 关闭 / 切换查询时及时退出）：
    stop = threading.Event()
    if not limiter.acquire(cancel=stop):
        raise Cancelled

设计：
    - 令牌桶算法：每秒补充 `rate` 个令牌，桶容量上限 `capacity`
    - acquire() 若令牌不足则阻塞等待，精确到毫秒
    - 传入 cancel 事件时，等待被切成小片，能在 100ms 内响应取消
    - 线程安全（threading.Lock）
    - 适用于在 QRunnable / ThreadPoolExecutor 等后台线程中调用 AWS API
"""

from __future__ import annotations

import threading
import time

# 带 cancel 事件等待时的最大单次睡眠时长（秒），决定取消的响应延迟上限
_CANCEL_POLL_INTERVAL = 0.1


class Cancelled(Exception):
    """
    限速等待被 cancel 事件中断。

    `acquire()` 本身只返回 False，由调用方决定是抛出本异常（需要区分
    "取消" 和 "拿到空结果" 时）还是直接返回。
    """


class RateLimiter:
    """
    令牌桶限速器。

    :param rate:     每秒补充的令牌数（即允许的最大 TPS）
    :param capacity: 桶的最大容量（允许的短暂突发量），默认等于 rate
    """

    def __init__(self, rate: float, capacity: float | None = None):
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}")
        self._rate = rate
        self._capacity = float(capacity if capacity is not None else rate)
        self._tokens = self._capacity       # 初始满桶
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    # ── 公开 API ─────────────────────────────────────────

    @property
    def rate(self) -> float:
        """每秒令牌补充速率（即允许的最大 TPS）"""
        return self._rate

    def acquire(
        self,
        tokens: float = 1.0,
        cancel: threading.Event | None = None,
    ) -> bool:
        """
        消耗 `tokens` 个令牌。若当前不足，阻塞直到令牌充足。

        :param cancel: 可选的取消事件。传入时等待会被切成 <=100ms 的小片，
                       事件被 set 后立即放弃等待并返回 False（不消耗令牌）。
        :return: True 表示已取得令牌；False 仅在被 `cancel` 中断时返回。
        """
        if tokens > self._capacity:
            raise ValueError(
                f"Requested {tokens} tokens exceeds bucket capacity {self._capacity}"
            )
        while True:
            if cancel is not None and cancel.is_set():
                return False

            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
                # 计算最短等待时间（秒）
                wait = (tokens - self._tokens) / self._rate

            if cancel is None:
                time.sleep(wait)
            elif self._sleep_cancellable(wait, cancel):
                return False

    def __enter__(self) -> "RateLimiter":
        self.acquire()
        return self

    def __exit__(self, *_) -> None:
        pass

    # ── 内部 ─────────────────────────────────────────────

    def _refill(self) -> None:
        """根据经过的时间补充令牌（须在持有锁时调用）。"""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    @staticmethod
    def _sleep_cancellable(seconds: float, cancel: threading.Event) -> bool:
        """
        分片睡眠 `seconds` 秒，期间轮询 `cancel`。

        :return: True 表示被取消（提前返回）；False 表示正常睡满。
        """
        deadline = time.monotonic() + seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if cancel.wait(min(remaining, _CANCEL_POLL_INTERVAL)):
                return True

    # ── 工厂方法 ─────────────────────────────────────────

    @classmethod
    def for_cloudwatch(cls) -> "RateLimiter":
        """
        适配 AWS CloudWatch Logs API 的预设限速器。

        中国区和国际区 describe_log_groups / describe_log_streams 均为 5 TPS。
        capacity=10 允许启动时短暂突发，避免第一批请求全部等待。
        """
        return cls(rate=5.0, capacity=10.0)
