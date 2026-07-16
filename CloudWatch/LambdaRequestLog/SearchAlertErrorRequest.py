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
from typing import Dict, List, Optional, Tuple

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
__PROJ_DIR = os.path.dirname(os.path.dirname(__SCRIPT_DIR))
if __PROJ_DIR not in sys.path:
    sys.path.insert(0, __PROJ_DIR)

from CloudWatch.LambdaRequestLog.AlertDataclass import LogDetail  # noqa: E402
from CloudWatch.cloud_watch_helper import get_log_client, filter_log_events, get_log_events  # noqa: E402
from utils.aws_consts import AllEnvs, Env  # noqa: E402
from utils.aws_consts_profile import get_profiles_for_curr_pc, PROFILE_Samson  # noqa: E402
from utils.aws_urls import gen_cloud_watch_log_stream_url, gen_cloud_watch_log_stream_url1  # noqa: E402
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


# 请求首行未被 app 日志覆盖时，往前找 START RequestId 的时间窗（Lambda 超时上限约 15s + 冷启动/时钟抖动余量）
ORPHAN_BACK_SECONDS = 20
ORPHAN_FORWARD_SECONDS = 5

# 匹配 Lambda 运行时 START RequestId: <uuid> 一行
PAT_START_LINE = re.compile(
    r'^START\s+RequestId:\s+(?P<aws_req_id>[0-9a-fA-F-]{8,})',
)
# END RequestId / REPORT RequestId 都能标记请求结束边界
PAT_END_LINE = re.compile(
    r'^(?:END|REPORT)\s+RequestId:\s+(?P<aws_req_id>[0-9a-fA-F-]{8,})',
)
# Lambda 运行时打的行首标记，用于识别一行日志是否属于 app（否则视为 runtime 行）
RUNTIME_FIRST_TOKENS = frozenset([
    'START', 'END', 'REPORT', 'INIT_START', 'LOGS', 'EXTENSION',
    'LAMBDA_WARNING', 'LAMBDA_RUNTIME',
])


def is_orphan_first_token(first_token: str) -> bool:
    """判断 message 首个空格分隔的 token 是不是 app log_id。
    app 网关日志形如 "XVJGbp ⚕ [INFO] ..." —— 首 token 是随机 log_id。
    orphan 行首 token 会是 "[ERROR]" / "[INFO]" / runtime 关键字，此时视为无 log_id。"""
    if not first_token:
        return True
    if first_token.startswith('[') and first_token.endswith(']'):
        return True
    if first_token in RUNTIME_FIRST_TOKENS:
        return True
    return False


@dataclass
class OrphanRequest:
    log_stream: str
    aws_req_id: str
    dt_start: datetime
    dt_end: datetime


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

    # 分流：first-token 是 app log_id 的 ERROR 走原路径；否则视为 orphan（handler 崩溃、无 app log_id）
    log_id_events: List[dict] = []
    orphan_events: List[dict] = []
    for e in events_all:
        first_token = e['message'].split(' ', 1)[0]
        if is_orphan_first_token(first_token):
            orphan_events.append(e)
        else:
            log_id_events.append(e)

    log_details_err: List[LogDetail] = extract_log_details(log_group, alert_rgn, log_id_events)

    # orphan 请求先定位边界，再拉完整日志，从中提取真实 app log_id
    orphan_requests, orphan_missing_cnt = resolve_orphan_requests(
        client=client,
        aws_region=alert_rgn,
        log_group=log_group,
        orphan_events=orphan_events,
    )
    orphan_full_by_key, orphan_id_by_key = fetch_orphan_full_logs(
        client=client,
        log_group=log_group,
        aws_region=alert_rgn,
        orphan_requests=orphan_requests,
    )

    orphan_err_details = build_orphan_error_details(
        log_group=log_group,
        aws_region=alert_rgn,
        orphan_events=orphan_events,
        orphan_requests=orphan_requests,
        orphan_id_by_key=orphan_id_by_key,
    )
    log_details_err += orphan_err_details

    if orphan_missing_cnt > 0:
        print(
            f'[WARN] {orphan_missing_cnt} 条 orphan ERROR 在 [-{ORPHAN_BACK_SECONDS}s, '
            f'+{ORPHAN_FORWARD_SECONDS}s] 窗口内未找到 START RequestId；'
            f'如果需要它们的完整请求日志，请把 --window-before 调大后重试。'
        )

    str_start = local_dt_start.strftime(FMT_DT_FILE)
    str_end = local_dt_end.strftime(FMT_DT_FILE)

    # 输出 Error 日志条目
    error_file = os.path.join(
        out_dir,
        f'{fn_name}_{alert_rgn}_{str_start}_{str_end}_ERROR.csv'
    )
    log_details_err.sort(key=lambda x: x.date_time)
    save_log_details_to_csv(error_file, log_details_err)

    # 准备输出 Error 事件完整日志
    # orphan path 已经全量拉过的请求，其 log_id 从 normal path 的 id_set 里剔除，避免重复搜
    orphan_covered_ids = set(v for v in orphan_id_by_key.values() if v)
    id_set = set(d.id for d in log_details_err if d.id and d.id not in orphan_covered_ids)
    if not id_set and not orphan_full_by_key:
        print('请求 ID 集为空，没法获取完整日志')
        return HandleAlertResult(
            error_cnt=len(log_details_err),
            error_csv=error_file,
            full_cnt=0,
            full_csv='',
        )

    log_details: List[LogDetail] = []

    if id_set:
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

    # orphan 请求已经拉过整段日志，直接并入 —— 每条 LogDetail 的 id 已被统一成该请求的合成/真实 log_id
    for details in orphan_full_by_key.values():
        log_details += details

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


# region orphan 请求处理

def resolve_orphan_requests(
        client,
        aws_region: str,
        log_group: str,
        orphan_events: List[dict],
) -> Tuple[List[OrphanRequest], int]:
    """给每条 orphan ERROR 找到它所在的请求 (stream, awsRequestId, dt_start, dt_end)，按请求去重。

    返回: (去重后的 OrphanRequest 列表, 未匹配到 START 的 orphan 事件数)
    """
    if not orphan_events:
        return [], 0

    # 按 stream 聚合 orphan 事件 —— 一个 stream 内串行，可以一次 filter 拿到该 stream 内所有 START/END
    by_stream: Dict[str, List[dict]] = {}
    for e in orphan_events:
        by_stream.setdefault(e['logStreamName'], []).append(e)

    resolved: Dict[Tuple[str, str], OrphanRequest] = {}
    missing_cnt = 0
    for stream, evts in by_stream.items():
        ts_min = min(e['timestamp'] for e in evts)
        ts_max = max(e['timestamp'] for e in evts)
        win_start = datetime.fromtimestamp(
            ts_min / 1000 - ORPHAN_BACK_SECONDS, tz=timezone.utc,
        )
        win_end = datetime.fromtimestamp(
            ts_max / 1000 + ORPHAN_FORWARD_SECONDS, tz=timezone.utc,
        )
        marker_events, _stats = filter_log_events(
            aws_region=aws_region,
            log_group_name=log_group,
            pattern=r'%RequestId:%',
            dt_start=win_start,
            dt_end=win_end,
            client=client,
            log_stream_names=[stream],
        )
        # 分离 START / END 行，按 timestamp 升序（filter_log_events 返回本身是升序）
        starts: List[Tuple[int, str]] = []  # (timestamp, aws_req_id)
        ends: List[Tuple[int, str]] = []
        for me in marker_events:
            msg = me['message'].lstrip()
            m_start = PAT_START_LINE.match(msg)
            if m_start:
                starts.append((me['timestamp'], m_start.group('aws_req_id')))
                continue
            m_end = PAT_END_LINE.match(msg)
            if m_end:
                ends.append((me['timestamp'], m_end.group('aws_req_id')))

        for oe in evts:
            oe_ts = oe['timestamp']
            # 找 oe 之前最近的一条 START
            matched_start: Optional[Tuple[int, str]] = None
            for s_ts, s_id in reversed(starts):
                if s_ts <= oe_ts:
                    matched_start = (s_ts, s_id)
                    break
            if matched_start is None:
                missing_cnt += 1
                print(
                    f'[WARN] orphan ERROR at {datetime.fromtimestamp(oe_ts/1000, tz=timezone.utc).isoformat()} '
                    f'in stream {stream} 未找到 START RequestId'
                )
                continue

            start_ts, aws_req_id = matched_start
            key = (stream, aws_req_id)
            if key in resolved:
                # 同一请求出现过多条 orphan ERROR，去重
                continue

            # 找该请求的结束边界：aws_req_id 匹配的最近一条 END/REPORT；否则用 start + 15s 兜底
            end_ts: Optional[int] = None
            for e_ts, e_id in ends:
                if e_id == aws_req_id and e_ts >= start_ts:
                    end_ts = e_ts
                    break
            if end_ts is None:
                end_ts = start_ts + 15_000
            resolved[key] = OrphanRequest(
                log_stream=stream,
                aws_req_id=aws_req_id,
                dt_start=datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc),
                dt_end=datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc),
            )

    return list(resolved.values()), missing_cnt


def fetch_orphan_full_logs(
        client,
        log_group: str,
        aws_region: str,
        orphan_requests: List[OrphanRequest],
) -> Tuple[Dict[Tuple[str, str], List[LogDetail]], Dict[Tuple[str, str], str]]:
    """对每个 orphan 请求拉整段日志，并从中挑一条 app 行的 log_id 作为该请求的 id。

    返回:
        - {(stream, awsRequestId): [LogDetail, ...]} 完整日志（已把 id 统一为下面挑选出的 log_id）
        - {(stream, awsRequestId): log_id} 每个请求的最终 log_id（可能是真实 app log_id，也可能是 fallback 合成 id）
    """
    full_by_key: Dict[Tuple[str, str], List[LogDetail]] = {}
    id_by_key: Dict[Tuple[str, str], str] = {}
    for req in orphan_requests:
        events, _stats = get_log_events(
            client=client,
            logStreamName=req.log_stream,
            logGroupName=log_group,
            startTime=req.dt_start,
            endTime=req.dt_end + timedelta(milliseconds=1),
            startFromHead=True,
        )
        # 从整段日志里挑第一条 app log 行的 first-token 作为 log_id
        picked_id: Optional[str] = None
        for e in events:
            first_token = e['message'].split(' ', 1)[0]
            if not is_orphan_first_token(first_token):
                picked_id = first_token
                break
        if picked_id is None:
            # handler 首行就崩，整段都是 runtime 行 —— 用 awsRequestId 作为合成 key
            picked_id = f'AWS:{req.aws_req_id}'
        id_by_key[(req.log_stream, req.aws_req_id)] = picked_id

        details: List[LogDetail] = []
        stream_url_base = gen_cloud_watch_log_stream_url1(
            log_group=log_group, log_rgn=aws_region, log_stream=req.log_stream,
        )
        for e in events:
            msg = e['message'].replace('\r', ' ').encode('utf-8').decode('utf-8').strip()
            details.append(LogDetail(
                date_time=datetime.fromtimestamp(e['timestamp'] / 1000, tz=timezone.utc),
                event_resp=e,
                id=picked_id,
                message=msg,
                url=stream_url_base,
            ))
        full_by_key[(req.log_stream, req.aws_req_id)] = details
    return full_by_key, id_by_key


def build_orphan_error_details(
        log_group: str,
        aws_region: str,
        orphan_events: List[dict],
        orphan_requests: List[OrphanRequest],
        orphan_id_by_key: Dict[Tuple[str, str], str],
) -> List[LogDetail]:
    """把 orphan ERROR 事件转成 LogDetail，id 用它所属请求的最终 log_id，方便 ERROR.csv 阅读。"""
    if not orphan_events:
        return []
    # 每个 stream 内按 timestamp 升序的 (start_ts, aws_req_id)
    req_starts_by_stream: Dict[str, List[Tuple[int, str]]] = {}
    for r in orphan_requests:
        req_starts_by_stream.setdefault(r.log_stream, []).append(
            (int(r.dt_start.timestamp() * 1000), r.aws_req_id)
        )
    for lst in req_starts_by_stream.values():
        lst.sort()

    out: List[LogDetail] = []
    for e in orphan_events:
        stream = e['logStreamName']
        oe_ts = e['timestamp']
        aws_req_id: Optional[str] = None
        for s_ts, s_id in reversed(req_starts_by_stream.get(stream, [])):
            if s_ts <= oe_ts:
                aws_req_id = s_id
                break
        if aws_req_id is None:
            # 未匹配到 START 的 orphan（missing 情况）：给个占位 id，让它仍能出现在 ERROR.csv 里
            picked_id = f'AWS:unknown:{stream}'
        else:
            picked_id = orphan_id_by_key.get((stream, aws_req_id), f'AWS:{aws_req_id}')

        msg = e['message'].replace('\r', ' ').encode('utf-8').decode('utf-8').strip()
        out.append(LogDetail(
            date_time=datetime.fromtimestamp(oe_ts / 1000, tz=timezone.utc),
            event_resp=e,
            id=picked_id,
            message=msg,
            url=gen_cloud_watch_log_stream_url(log_group, aws_region, e),
        ))
    return out


# endregion orphan 请求处理


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
