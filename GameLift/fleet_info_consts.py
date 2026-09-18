from datetime import datetime, timedelta, timezone

DT_FMT_M = '%Y-%m-%d %H:%M'
DT_FMT_S = '%m-%d %H:%M:%S'

# 表格里时间统一按 +08 显示，不跟随运行机器的本地时区。
# botocore 对 JSON 协议的 epoch 时间戳用 tzlocal() 解析，不固定的话同一个 fleet 在不同时区的机器上会显示成不同时间，截图和日志对不上
DISPLAY_TZ = timezone(timedelta(hours=8))


def fmt_dt_display(dt: datetime, fmt: str = DT_FMT_M) -> str:
    """
    把 datetime 转成 DISPLAY 时区后格式化，用于表格展示
    :param dt: 带时区的 datetime；naive datetime 会被当作运行机器的本地时间
    :param fmt: strftime 格式，默认 DT_FMT_M
    :return: 格式化后的字符串
    """
    return dt.astimezone(DISPLAY_TZ).strftime(fmt)


# 机群状态/地区状态列里 ACTIVE 的显示替换。让稳态的行退成背景，异常状态（保持文字）自己跳出来。
# 用 U+2705，Segoe UI Emoji 和 Twemoji 都有字形，不依赖额外字体；显示宽度 2 列，撑不开表头下限
STATUS_ACTIVE = 'ACTIVE'
STATUS_ACTIVE_EMOJI = '\N{WHITE HEAVY CHECK MARK}'
