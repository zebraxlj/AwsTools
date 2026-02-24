import csv
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

"""
使用方法：
1. 脚本内搜索 PROFILE_CN、PROFILE_US，根据你的计算机名与 aws config 中 profile 的命名
2. 脚本内搜索 ALERT_DETAIL，从飞书中复制 PA生产网关告警 内容并填入
3. 用 PyTest 运行 test_prod_alarm，或在终端中直接运行脚本
4. 注意 Console 中注明的输出文件位置
"""

ALERT_DETAIL = '''
Lambda Log 告警
区域: cn-northwest-1
函数:
告警内容:
告警时间:
错误数量：
首行错误：
查看详情
'''

pc_name = platform.node()
PROFILE_CN, PROFILE_US = '', ''

# 设置数据文件夹
DATA_DIR = __SCRIPT_DIR
if pc_name in {'Source-XiaLijie'}:
    DATA_DIR = os.path.join(__PROJ_DIR, 'CloudWatch', 'Data', 'LambdaRequestLog')


def main():
    print()
    global ALERT_DETAIL
    if not ALERT_DETAIL.strip() or get_profiles_for_curr_pc() == PROFILE_Samson:
        try:
            input_file_path = os.path.join(DATA_DIR, 'input.txt')
            with open(input_file_path, 'r', encoding='utf8') as f_in:
                input_data = f_in.read()
                if input_data:
                    ALERT_DETAIL = input_data
                print(f'告警信息来源：{input_file_path}')
        except FileNotFoundError:
            print('告警信息来源：当前脚本')
        except Exception as e:
            print(e)
            import traceback
            traceback_log = traceback.format_tb(e.__traceback__)
            traceback_list = list(reversed(traceback_log))
            print(''.join(traceback_list))

    alert_detail_lines = ALERT_DETAIL.strip().split('\n')
    print('告警信息: -----------------------------------------------------')
    print('\n'.join(alert_detail_lines[:min(8, len(alert_detail_lines))]), '\n', sep='')
    alert_detail: AlertDetail = parse_alert_detail(ALERT_DETAIL)
    handle_alert(alert_detail)


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
        alert_detail: AlertDetail, dt_start: Optional[datetime] = None, dt_end: Optional[datetime] = None
) -> HandleAlertResult:
    alert_dt = alert_detail.alarm_dt
    alert_rgn = alert_detail.rgn
    fn_name = alert_detail.func_name
    log_group = alert_detail.log_group

    local_dt_end = dt_end if dt_end is not None else alert_dt
    local_dt_start = dt_start if dt_start is not None else local_dt_end - timedelta(minutes=5)

    print('搜索: ---------------------------------------------------------')
    print(
        f'日志组: {log_group}\n'
        f'地区: {alert_rgn}\n'
        f'开始时间：{local_dt_start}\n'
        f'结束时间：{local_dt_end}\n'
    )

    env_name = fn_name.split('--')[0]
    env: Env = AllEnvs.get_env_by_name(env_name)
    client = get_log_client(alert_rgn, env)

    events_all: List[dict] = filter_log_events(
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
        DATA_DIR,
        f'{fn_name}_{alert_rgn}_{str_start}_{str_end}_ERROR.csv'
    )
    save_log_details_to_csv(error_file, log_details_err)

    # print(f'Error 日志条目: ----------------------------------------------')
    # for d in log_details:
    #     print(f"{d.date_time}\t{d.message}\n{d.url}\n")

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
        events = filter_log_events(
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
    full_file = os.path.join(DATA_DIR, f'{fn_name}_{alert_rgn}_{str_start}_{str_end}_FULL.csv')
    save_log_details_to_csv(full_file, log_details)
    result: HandleAlertResult = HandleAlertResult(
        error_cnt=len(log_details_err),
        error_csv=error_file,
        full_cnt=len(log_details),
        full_csv=full_file,
    )

    # print(f'Error 请求完整日志: -------------------------------------------')
    # for d in log_details:
    #     print(f"{d.date_time}\t{d.message}\n{d.url}\n")

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


if __name__ == '__main__':
    main()
