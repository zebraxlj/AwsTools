import argparse
import csv
import json
import os
import platform
import re
import inspect
import sys
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from typing import List, Optional

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
__PROJ_DIR = os.path.dirname(os.path.dirname(__SCRIPT_DIR))
if __PROJ_DIR not in sys.path:
    sys.path.insert(0, __PROJ_DIR)

from CloudWatch.LambdaRequestLog.AlertDataclass import LogDetail  # noqa: E402
from CloudWatch.cloud_watch_helper import get_log_client, filter_log_events  # noqa: E402
from utils.aws_consts import AllEnvs, Env  # noqa: E402
from utils.aws_consts_profile import get_profiles_for_curr_pc, PROFILE_Samson  # noqa: E402
from utils.aws_urls import gen_cloud_watch_log_stream_url  # noqa: E402
from utils.exec_env_util import is_running_in_pycharm  # noqa: E402

"""
使用方法：
- 命令行（推荐）：
    python SearchAlertErrorRequest.py --alert-file alert.txt
    cat alert.txt | python SearchAlertErrorRequest.py --alert-stdin
    完整参数见 --help。

- PyCharm 直接 Run（开发便利路径）：
    1. 把飞书告警内容粘贴到本脚本同目录下的 input.txt 并保存
    2. 直接 Run 本脚本（Run Configuration 不需要配参数）
    脚本检测到 PyCharm 环境且无参数时，会自动以 --alert-file <script_dir>/input.txt 运行。
    若想覆盖默认行为，在 Run Configuration 里手动配置参数即可。

- GUI：运行 SearchAlertErrorRequestUI

- 时间窗口：默认告警时间往前 5 分钟、往后 0 分钟。
    --window-before / --window-after 调整窗口大小（分钟）
    --start / --end 完全自定义窗口（必须带时区后缀，如 +0800/+0000；与 --window-* 互斥）

- 输出：在 --output-dir 指定的目录（默认脚本目录）下生成两份 CSV：
    *_ERROR.csv  告警时间窗口内所有 [ERROR] 行
    *_FULL.csv   涉及上述错误的请求 id 的全部日志（不只是 ERROR 行）
    --print-result-json 时，stdout 末尾追加一行 JSON 包含路径与命中数。
"""

pc_name = platform.node()

# 默认输出目录
DEFAULT_DATA_DIR = os.path.join(__SCRIPT_DIR, 'Data', 'LambdaRequestLog')
if pc_name in {'Source-XiaLijie'}:
    DEFAULT_DATA_DIR = os.path.join(__PROJ_DIR, 'CloudWatch', 'Data', 'LambdaRequestLog')


# region HelperFunctions

FMT_DT_FILE = '%Y%m%d-%H%M%S'
FMT_DT_CONTENT_LEGACY = '%Y-%m-%d %H:%M:%S.%f'


@dataclass
class AlertDetail:
    alarm_dt_str: str
    func_name: str
    rgn: str

    @property
    def alarm_dt(self) -> datetime:
        return from_alert_dt_str(self.alarm_dt_str)

    @property
    def log_group(self) -> str:
        return f'/aws/lambda/{self.func_name}'

    def to_dict(self, include_properties: bool = True) -> dict:
        data = {f.name: getattr(self, f.name) for f in fields(self)}
        if include_properties:
            for name, prop in inspect.getmembers(self.__class__, lambda v: isinstance(v, property)):
                if name not in data:
                    data[name] = getattr(self, name)
        return data

    def __str__(self):
        return '\r\n'.join(['{', *[f'\t"{k}": "{v}"' for k, v in self.to_dict().items()], '}'])


def get_pattern(pat, msg: str) -> Optional[str]:
    match = re.search(pat, msg, re.DOTALL)
    if not match:
        return None
    return match.group(1).strip()


def get_func_from_event(alert_detail: str) -> Optional[str]:
    pat_func = r'函数:\s*(.*?)\n'
    return get_pattern(pat_func, alert_detail)


def get_log_group_from_event(alert_detail: str) -> Optional[str]:
    func_name = get_func_from_event(alert_detail)
    if not func_name:
        return None
    return f'/aws/lambda/{func_name}'


def get_time_from_event(alert_detail: str) -> Optional[str]:
    pat_time = r'时间:\s*(.*?)\n'
    return get_pattern(pat_time, alert_detail)


def get_rgn_from_event(alert_detail: str) -> Optional[str]:
    pat_rgn = r'区域:\s*(.*?)\n'
    return get_pattern(pat_rgn, alert_detail)


def from_alert_dt_str(time_str: str) -> datetime:
    return datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S.%f%z')


def parse_alert_detail(alert_detail: str) -> AlertDetail:
    func_name = get_func_from_event(alert_detail) or ''
    alarm_dt_str = get_time_from_event(alert_detail) or ''
    rgn = get_rgn_from_event(alert_detail) or ''
    missing = [
        name for name, value in (
            ('function', func_name),
            ('time', alarm_dt_str),
            ('region', rgn),
        )
        if not value
    ]
    if missing:
        raise ValueError(f'Missing required fields: {", ".join(missing)}')
    return AlertDetail(
        alarm_dt_str=alarm_dt_str,
        func_name=func_name,
        rgn=rgn,
    )


def print_reason(reason: str):
    print('原因', '='*30)
    print(reason)
    print('=' * 33)


def format_csv_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec='milliseconds')


def parse_csv_datetime(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        dt = datetime.strptime(value, FMT_DT_CONTENT_LEGACY)
        return dt.replace(tzinfo=timezone.utc)

# endregion HelperFunctions


@dataclass
class HandleAlertResult:
    error_cnt: int
    error_csv: str
    full_cnt: int
    full_csv: str


def handle_alert(
        alert_detail: AlertDetail,
        dt_start: Optional[datetime] = None, dt_end: Optional[datetime] = None,
        output_dir: Optional[str] = None,
) -> HandleAlertResult:
    alert_dt = alert_detail.alarm_dt
    alert_rgn = alert_detail.rgn
    fn_name = alert_detail.func_name
    log_group = alert_detail.log_group

    local_dt_end = dt_end if dt_end is not None else alert_dt
    local_dt_start = dt_start if dt_start is not None else local_dt_end - timedelta(minutes=5)
    out_dir = output_dir if output_dir is not None else DEFAULT_DATA_DIR

    if local_dt_start >= local_dt_end:
        raise ValueError(
            f'开始时间必须早于结束时间。dt_start={local_dt_start.isoformat()} '
            f'dt_end={local_dt_end.isoformat()}'
        )

    print('搜索: ---------------------------------------------------------')
    print(
        f'日志组: {log_group}\n'
        f'地区: {alert_rgn}\n'
        f'开始时间：{local_dt_start}\n'
        f'结束时间：{local_dt_end}\n'
        f'输出目录：{out_dir}\n'
    )

    env_name = fn_name.split('--')[0]
    env: Env = AllEnvs.get_env_by_name(env_name)
    client = get_log_client(alert_rgn, env)

    events_all, _stats = filter_log_events(
        aws_region=alert_rgn,
        log_group_name=log_group,
        pattern=r'%\[ERROR\]%',
        dt_start=local_dt_start,
        dt_end=local_dt_end,
        client=client,
    )
    log_details_err: List[LogDetail] = extract_log_details(log_group, alert_rgn, events_all)

    str_start = local_dt_start.strftime(FMT_DT_FILE)
    str_end = local_dt_end.strftime(FMT_DT_FILE)

    # 输出 Error 日志条目
    error_file = os.path.join(
        out_dir,
        f'{fn_name}_{alert_rgn}_{str_start}_{str_end}_ERROR.csv'
    )
    save_log_details_to_csv(error_file, log_details_err)

    # 准备输出 Error 事件完整日志
    id_set = set([d.id for d in log_details_err])
    if not id_set:
        print('请求 ID 集为空，没法获取完整日志')
        return HandleAlertResult(
            error_cnt=len(log_details_err),
            error_csv=error_file,
            full_cnt=0,
            full_csv='',
        )

    log_details: List[LogDetail] = []

    patterns = ['']
    for rid in id_set:
        rid_clean = re.escape(rid)
        if not patterns[-1]:
            patterns[-1] = rid_clean
        elif len(patterns[-1] + f'|{rid_clean}') < 1024 - 2:
            patterns[-1] += f'|{rid_clean}'
        else:
            patterns.append(rid_clean)

    for p in patterns:
        events, _stats = filter_log_events(
            aws_region=alert_rgn,
            log_group_name=log_group,
            pattern=rf'%{p}%',
            dt_start=local_dt_start,
            dt_end=local_dt_end,
            client=client,
        )
        log_details += extract_log_details(log_group, alert_rgn, events)  # noqa

    log_details.sort(key=lambda x: x.date_time)
    log_id_sorted = [d.id for d in log_details]
    id_index = {log_id: idx for idx, log_id in enumerate(log_id_sorted)}
    log_details.sort(key=lambda x: (id_index[x.id], x.date_time))

    # 输出 Error 事件完整日志
    full_file = os.path.join(out_dir, f'{fn_name}_{alert_rgn}_{str_start}_{str_end}_FULL.csv')
    save_log_details_to_csv(full_file, log_details)
    result: HandleAlertResult = HandleAlertResult(
        error_cnt=len(log_details_err),
        error_csv=error_file,
        full_cnt=len(log_details),
        full_csv=full_file,
    )

    if False and get_profiles_for_curr_pc() == PROFILE_Samson:
        from CloudWatch.LambdaRequestLog.AnalyzeAlertLog import check_config_center_steam_stability, \
            check_login_affected_user, check_login, check_account_info, check_mission_system, \
            check_store, check_matching

        if '-LoginFunction' in fn_name:
            check_login_affected_user(log_details)
            check_login(log_details)
        if '-ConfigCenterFunction' in fn_name:
            check_config_center_steam_stability(log_details_err)
        if '-StoreFunction' in fn_name:
            check_store(log_details)
        if '-AccountInfoFunction' in fn_name:
            check_account_info(log_details_err)
        if '-MatchingFunction' in fn_name:
            check_matching(log_details)
        if '-MissionSystemFunction' in fn_name:
            check_mission_system(log_details)

    return result


# region 日志解析与保存

def extract_log_details(
        log_group: str, rgn: str, events: List[dict], silent: bool = True
) -> List[LogDetail]:
    log_details: List[LogDetail] = []
    for e in events:
        e['message'] = e['message'].replace('\r', ' ')
        msg_new = e['message'].encode('utf-8').decode('utf-8').strip()
        if not silent:
            print('---------', e)
            print(f"before: {e['message'].strip()}")
            print(f"after : {msg_new}")
        log_details.append(LogDetail(
            date_time=datetime.fromtimestamp(e['timestamp'] / 1000, tz=timezone.utc),
            event_resp=e,
            id=e['message'].split(' ')[0],
            message=msg_new,
            url=gen_cloud_watch_log_stream_url(log_group, rgn, e)
        ))
    return log_details


def save_log_details_to_csv(file_name: str, log_details: List[LogDetail]):
    header = ['DateTime', 'Msg', 'Url']
    rows = [
        {
            'DateTime': format_csv_datetime(d.date_time),
            'Msg': d.message,
            'Url': d.url,
        }
        for d in log_details
    ]
    print(f'Saving file at {file_name} {len(log_details)}')
    dir_path = os.path.dirname(file_name)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    with open(file_name, 'w', newline='', encoding='utf8') as f_out:
        writer = csv.DictWriter(f_out, quotechar='"', fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def read_log_details_from_csv(file_name: str, top_n_lines: Optional[int] = None) -> List[LogDetail]:
    with open(file_name, 'r', encoding='utf8') as f_in:
        reader = csv.DictReader(f_in, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        rows = [row for row in reader]
        if top_n_lines:
            rows = rows[:top_n_lines]
        ret = [
            LogDetail(
                id=row['Msg'].split(' ⚕ ')[0],
                date_time=parse_csv_datetime(row['DateTime']),
                message=row['Msg'].split(' ⚕ ')[1],
                url=row['Url'],
            )
            for row in rows
            if 'check idempotent failed' not in row['Msg']
        ]
        return ret

# endregion 日志解析与保存


def __parse_args(argv: List[str]):
    parser = argparse.ArgumentParser(
        description='解析飞书 Lambda 错误告警，拉取告警时间附近的 ERROR 日志和涉及请求的完整日志。',
        epilog=(
            '示例：\n'
            '  python SearchAlertErrorRequest.py --alert-file alert.txt\n'
            '  cat alert.txt | python SearchAlertErrorRequest.py --alert-stdin\n'
            '  python SearchAlertErrorRequest.py --alert-file alert.txt --window-before 30 --print-result-json\n'
            '  python SearchAlertErrorRequest.py --alert-file alert.txt '
            '--start "2026-04-18 12:00:00+0800" --end "2026-04-18 12:30:00+0800"\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    g_input = parser.add_mutually_exclusive_group(required=True)
    g_input.add_argument('--alert-file', '-f',
                         help='告警文本文件路径（飞书告警内容粘贴后保存的 .txt）。')
    g_input.add_argument('--alert-stdin', action='store_true',
                         help='从 stdin 读取告警文本。')

    parser.add_argument('--output-dir', '-o',
                        default=None,
                        help=f'输出目录。默认: {DEFAULT_DATA_DIR}')

    # 注意 default=None：argparse mutex 通过 "action 是否被触发" 判断冲突，
    # 默认值若非 None 则即便用户没传也会让 mutex 失效——所以默认值放到 __resolve_window 里。
    g_start = parser.add_mutually_exclusive_group()
    g_start.add_argument('--window-before',
                         type=int, default=None,
                         help='告警时间往前推多少分钟，默认 5。与 --start 互斥。')
    g_start.add_argument('--start', '-s',
                         default=None,
                         help='开始时间，必须带时区后缀，格式 "YYYY-MM-DD HH:MM:SS+0800" 或 "+0000"。'
                              '与 --window-before 互斥。')

    g_end = parser.add_mutually_exclusive_group()
    g_end.add_argument('--window-after',
                       type=int, default=None,
                       help='告警时间往后推多少分钟，默认 0。与 --end 互斥。')
    g_end.add_argument('--end', '-e',
                       default=None,
                       help='结束时间，必须带时区后缀，格式 "YYYY-MM-DD HH:MM:SS+0800" 或 "+0000"。'
                            '与 --window-after 互斥。')

    parser.add_argument('--print-result-json', action='store_true',
                        help='完成后在 stdout 末尾打印一行 JSON，包含 csv 路径和命中数（便于脚本/skill 解析）。')

    return parser.parse_args(argv)


def __parse_aware_dt(value: str, label: str) -> datetime:
    """解析带时区的时间字符串。strptime '%z' 强制时区后缀，缺时区会抛 ValueError。"""
    fmt = '%Y-%m-%d %H:%M:%S%z'
    try:
        return datetime.strptime(value, fmt)
    except ValueError as e:
        raise ValueError(
            f'{label} 解析失败：{value!r}。要求格式 "YYYY-MM-DD HH:MM:SS+ZZZZ"（必须带时区后缀，如 +0800/+0000）。'
            f'底层错误：{e}'
        )


def __resolve_window(args, alert_dt: datetime) -> tuple:
    """根据 args 解析最终的 dt_start / dt_end，并校验先后顺序。
    --window-before / --window-after 默认 None（用 mutex 检测用），未传时用 5 / 0 回填。"""
    if args.start:
        dt_start = __parse_aware_dt(args.start, '--start')
    else:
        wb = args.window_before if args.window_before is not None else 5
        dt_start = alert_dt - timedelta(minutes=wb)
    if args.end:
        dt_end = __parse_aware_dt(args.end, '--end')
    else:
        wa = args.window_after if args.window_after is not None else 0
        dt_end = alert_dt + timedelta(minutes=wa)

    if dt_start >= dt_end:
        raise ValueError(
            f'开始时间必须早于结束时间。当前 dt_start={dt_start.isoformat()} '
            f'dt_end={dt_end.isoformat()}。检查 --window-before/--window-after 或 --start/--end。'
        )
    return dt_start, dt_end


def __read_alert_text(args) -> str:
    if args.alert_stdin:
        text = sys.stdin.read()
        print('告警信息来源：stdin')
    else:
        with open(args.alert_file, 'r', encoding='utf8') as f:
            text = f.read()
        print(f'告警信息来源：{args.alert_file}')
    if not text.strip():
        raise ValueError('告警文本为空')
    return text


def main():
    argv = sys.argv[1:]
    if is_running_in_pycharm() and not argv:
        # PyCharm 直接 Run（未配 Run Configuration 参数）：默认读取脚本目录下的 input.txt
        default_input = os.path.join(__SCRIPT_DIR, 'input.txt')
        argv = ['--alert-file', default_input]
    args = __parse_args(argv)

    alert_text = __read_alert_text(args)
    alert_lines = alert_text.strip().split('\n')
    print('告警信息: -----------------------------------------------------')
    print('\n'.join(alert_lines[:min(8, len(alert_lines))]), '\n', sep='')

    alert_detail: AlertDetail = parse_alert_detail(alert_text)
    dt_start, dt_end = __resolve_window(args, alert_detail.alarm_dt)

    result = handle_alert(
        alert_detail,
        dt_start=dt_start,
        dt_end=dt_end,
        output_dir=args.output_dir,
    )

    if args.print_result_json:
        print(json.dumps({
            'error_csv': result.error_csv,
            'error_cnt': result.error_cnt,
            'full_csv': result.full_csv,
            'full_cnt': result.full_cnt,
        }, ensure_ascii=False))


if __name__ == '__main__':
    main()
