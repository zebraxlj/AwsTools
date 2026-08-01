import functools
import inspect
import logging
from datetime import datetime
from typing import List, Optional


def log_span(msg: str = "", log_args: bool = False, arg_names: Optional[List[str]] = None):
    """打印函数进入/退出耗时的装饰器（同时支持同步和异步函数）。

    Args:
        msg: 前缀 tag，方便在日志里区分不同来源。
        log_args: 是否打印所有形参（当 arg_names 为 None 时生效）。
        arg_names: 白名单，只打印指定形参；一旦设置就覆盖 log_args。
    """
    def decorator(func):
        sig = inspect.signature(func)

        def _format_args(args, kwargs) -> str:
            if arg_names is None and not log_args:
                return ""
            try:
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                items = list(bound.arguments.items())
                if arg_names is not None:
                    items = [(k, v) for k, v in items if k in arg_names]
                pairs = " ".join(f"{k}={v!r}" for k, v in items)
                return f" {pairs}" if pairs else ""
            except Exception:
                return ""

        # stacklevel=2 让 logging record 的 filename/funcName/lineno 指向调用装饰器包装函数的位置，
        # 而不是本文件里的 func_wrapper 内部。
        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def func_wrapper_async(*args, **kwargs):
                dt_start = datetime.now()
                args_str = _format_args(args, kwargs)
                logging.info(f"{msg} func_name={func.__name__}{args_str} start", stacklevel=2)
                res = await func(*args, **kwargs)
                span_ms = int((datetime.now() - dt_start).total_seconds() * 1000)
                logging.info(f"{msg} func_name={func.__name__}{args_str} span={span_ms}ms", stacklevel=2)
                return res
            return func_wrapper_async

        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            dt_start = datetime.now()
            args_str = _format_args(args, kwargs)
            logging.info(f"{msg} func_name={func.__name__}{args_str} start", stacklevel=2)
            res = func(*args, **kwargs)
            span_ms = int((datetime.now() - dt_start).total_seconds() * 1000)
            logging.info(f"{msg} func_name={func.__name__}{args_str} span={span_ms}ms", stacklevel=2)
            return res

        return func_wrapper
    return decorator
