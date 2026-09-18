DT_FMT_M = '%Y-%m-%d %H:%M %z'
DT_FMT_S = '%m-%d %H:%M:%S'

# 机群状态/地区状态列里 ACTIVE 的显示替换。让稳态的行退成背景，异常状态（保持文字）自己跳出来。
# 用 U+2705，Segoe UI Emoji 和 Twemoji 都有字形，不依赖额外字体；显示宽度 2 列，撑不开表头下限
STATUS_ACTIVE = 'ACTIVE'
STATUS_ACTIVE_EMOJI = '\N{WHITE HEAVY CHECK MARK}'
