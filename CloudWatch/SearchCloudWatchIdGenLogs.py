"""搜索各日志组中每种 ID 类型最后一次生成的日志。

按 (日志组, 该日志组中的 ID 类型列表) 进行倒序搜索，
使用 on_receive_batch 回调逐段解析，每种类型只保留最新一条，
该日志组的所有类型都找到后提前停止。

输出文件：
- 运行输出文件: {DATA_DIR}/IdGen_{timestamp}_run.log     各 worker 的运行日志
- 日志数据文件: {DATA_DIR}/IdGen_{timestamp}_data.csv    拉取到的日志数据
"""
import csv
import multiprocessing
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
__PROJ_DIR = os.path.dirname(__SCRIPT_DIR)
if __PROJ_DIR not in sys.path:
    sys.path.insert(0, __PROJ_DIR)

from CloudWatch.cloud_watch_dataclass import FilterLogEventsResp  # noqa: E402
from CloudWatch.cloud_watch_helper import filter_log_events_descending  # noqa: E402
from utils import aws_urls  # noqa: E402
from utils.aws_consts import REGION_TO_ABBR  # noqa: E402

"""
/aws/lambda/PartyAnimals--209820-AccountInfoFnStateChange
/aws/lambda/PartyAnimals--209820-AccountInfoFunction
/aws/lambda/PartyAnimals--209820-AppraiseFunction
/aws/lambda/PartyAnimals--209820-Collab3PartyFunction
/aws/lambda/PartyAnimals--209820-CustomerServiceFunction
/aws/lambda/PartyAnimals--209820-DebugFunction
/aws/lambda/PartyAnimals--209820-DeveloperToolsFunction
/aws/lambda/PartyAnimals--209820-EmailFunction
/aws/lambda/PartyAnimals--209820-FishFunction
/aws/lambda/PartyAnimals--209820-FriendFnStateChange
/aws/lambda/PartyAnimals--209820-FriendFunction
/aws/lambda/PartyAnimals--209820-GMServiceFunction
/aws/lambda/PartyAnimals--209820-GachaFunction
/aws/lambda/PartyAnimals--209820-GameAdminFunction
/aws/lambda/PartyAnimals--209820-HomelandFunction
/aws/lambda/PartyAnimals--209820-IpLocationFunction
/aws/lambda/PartyAnimals--209820-LittleBlackBoxFunction
/aws/lambda/PartyAnimals--209820-LoginAlertFunction
/aws/lambda/PartyAnimals--209820-LoginFnStateChange
/aws/lambda/PartyAnimals--209820-LoginFunction
/aws/lambda/PartyAnimals--209820-MailFunction
/aws/lambda/PartyAnimals--209820-MatchingFnStateChange
/aws/lambda/PartyAnimals--209820-MatchingFunction
/aws/lambda/PartyAnimals--209820-MiniGameFunction
/aws/lambda/PartyAnimals--209820-MissionSystemFunction
/aws/lambda/PartyAnimals--209820-MysteryFnStateChange
/aws/lambda/PartyAnimals--209820-MysteryFunction
/aws/lambda/PartyAnimals--209820-PhoneCertFunction
/aws/lambda/PartyAnimals--209820-PhotoDownloadFunction
/aws/lambda/PartyAnimals--209820-PhotoUploadFunction
/aws/lambda/PartyAnimals--209820-QuestionnaireFunction
/aws/lambda/PartyAnimals--209820-RemoteBundleFunction
/aws/lambda/PartyAnimals--209820-RewardFunction
/aws/lambda/PartyAnimals--209820-StoreFnStateChange
/aws/lambda/PartyAnimals--209820-StoreFunction
/aws/lambda/PartyAnimals--209820-VoteFunction
/aws/lambda/PartyAnimals--209820-WSAuthorizerFunction
/aws/lambda/PartyAnimals--209820-WSFnStateChange
/aws/lambda/PartyAnimals--209820-WSOnConnectFnStateChange
/aws/lambda/PartyAnimals--209820-WSSendMessageFnStateChange
/aws/lambda/PartyAnimals--209820-WebSocketFunction
/aws/lambda/PartyAnimals--209820-WebSocketOnConnectFunction
/aws/lambda/PartyAnimals--209820-WebSocketOnDisconnectFunction
/aws/lambda/PartyAnimals--209820-WebSocketSendMessageFunction
"""

# region 脚本运行配置
LOG_GROUP_PREFIX = '/aws/lambda/PartyAnimals--209820-'

# 每个日志组中需要搜索的 ID 类型 (日志中的全名，含环境前缀)
LOG_GROUP_ID_TYPES: Dict[str, List[str]] = {
    'LoginFunction':       ['PartyAnimals_IdAccount', 'PartyAnimals_CUSTId', 'PartyAnimals_GmeId'],
    'MatchingFunction':    ['PartyAnimals_LobbyId'],
    'AccountInfoFunction': ['PartyAnimals_PlayerTrackingNumber'],
    'StoreFunction':       ['PartyAnimals_OrderId'],
    'GachaFunction':       ['PartyAnimals_OpenGachaId', 'PartyAnimals_NoRepeatGachaId', 'PartyAnimals_ClawMachineGrabId'],
    'RewardFunction':      ['PartyAnimals_BattleId'],
    'MysteryFunction':     ['PartyAnimals_SessionId'],
    'GMServiceFunction':   ['PartyAnimals_PunishRec'],
}

REGIONS = [
    # 'cn-northwest-1',
    # 'ap-northeast-1',
    'eu-central-1',
    # 'us-east-1',
]
DT_START_UTC = datetime(2026, 4, 13, tzinfo=timezone.utc)
DT_END_UTC = datetime.now(tz=timezone.utc)
SEGMENT_DURATION = timedelta(hours=24)

PATTERN = r'id_generator new id'

# ID 类型提取正则，需要有一个捕获组
# 匹配日志格式: "id_generator.py:101 ⫸ new id: PartyAnimals_OrderId = 7769672"
ID_TYPE_REGEX = r'new id:\s*(\S+)\s*='

# 输出目录
DATA_DIR = os.path.join(__PROJ_DIR, 'CloudWatch', 'Data', 'IdGenLog')
# endregion 脚本运行配置


# region 文件输出

FMT_DT_FILE = '%Y%m%d-%H%M%S'
FMT_DT_CONTENT = '%Y-%m-%d %H:%M:%S.%f'


def _ensure_dir(dir_path: str):
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)


def save_data_csv(file_path: str, rows: List[dict]):
    """保存日志数据到 CSV 文件。"""
    header = ['DateTime', 'Region', 'IdType', 'Msg', 'Url']
    _ensure_dir(os.path.dirname(file_path))
    with open(file_path, 'w', newline='', encoding='utf8') as f_out:
        writer = csv.DictWriter(f_out, quotechar='"', fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    print(f'Data CSV saved: {file_path} ({len(rows)} rows)')


def save_run_log(file_path: str, lines: List[str]):
    """保存运行输出到 log 文件。"""
    _ensure_dir(os.path.dirname(file_path))
    with open(file_path, 'w', encoding='utf8') as f_out:
        f_out.write('\n'.join(lines))
    print(f'Run log saved: {file_path} ({len(lines)} lines)')

# endregion 文件输出


# region on_receive_batch 回调工厂

def make_find_each_id_type_batch_fn(
    known_types: List[str],
    id_type_regex: str = ID_TYPE_REGEX,
) -> Callable[[List[dict]], bool]:
    """创建一个"每种 ID 类型各找最新一条"的 on_receive_batch 回调。

    倒序搜索时，每处理完一个时间段会调用此回调。回调解析段内日志，
    提取 ID 类型名称，每种类型只保留第一次出现（即最新的）。
    所有 known_types 都找到后返回 True 停止搜索。

    Args:
        known_types: 需要搜索的 ID 类型名称列表。
        id_type_regex: 从日志 message 中提取 ID 类型名称的正则，需要有一个捕获组。

    Returns:
        有状态的 on_receive_batch(seg_events_desc: List[dict]) -> bool
        回调上挂载 found_types: Dict[str, dict] 供外部获取结果。
    """
    found_types: Dict[str, dict] = {}  # type_name -> 最新的那条 event（第一次遇到即最新）
    known_set = set(known_types)
    pattern = re.compile(id_type_regex)

    def on_receive_batch(seg_events_desc: List[dict]) -> bool:
        for event in seg_events_desc:
            msg = event.get('message', '')
            m = pattern.search(msg)
            if m:
                id_type = m.group(1)
                if id_type in known_set and id_type not in found_types:
                    found_types[id_type] = event

        return all(t in found_types for t in known_set)

    on_receive_batch.found_types = found_types
    return on_receive_batch

# endregion on_receive_batch 回调工厂


def process_worker(
    log_group_name: str, region: str,
    known_types: List[str],
    dt_start_utc: datetime, dt_end_utc: datetime,
    segment_duration: timedelta,
    shared_msg_dict: dict, shared_dict: Dict[str, list], stop_event,
):
    fmt = FMT_DT_CONTENT

    def __gen_msg(msg: str):
        return f'{datetime.now().strftime(fmt)} > {msg}'

    worker_dt_start = datetime.now()
    shared_key = f'{log_group_name} {REGION_TO_ABBR.get(region, region)}'
    shared_msg_dict[shared_key] = [__gen_msg(f'{shared_key}: 开始 搜索类型：{known_types}')]
    print(shared_msg_dict[shared_key])
    shared_dict[shared_key] = []

    on_receive_batch = make_find_each_id_type_batch_fn(known_types)
    events, _stats = filter_log_events_descending(
        region, log_group_name, PATTERN,
        dt_start=dt_start_utc, dt_end=dt_end_utc,
        is_stop_on_match=True,
        stop_event=stop_event,
        segment_duration=segment_duration,
        on_receive_batch=on_receive_batch,
    )

    # 从回调的 found_types 中提取结果，按时间升序构建 CSV 行
    found_items = sorted(on_receive_batch.found_types.items(), key=lambda kv: kv[1].get('timestamp', 0))

    csv_rows = []
    for id_type_name, event in found_items:
        e = FilterLogEventsResp(**event)
        event_ts = datetime.fromtimestamp(e.timestamp / 1000, tz=timezone.utc)
        event_url = aws_urls.gen_cloud_watch_log_stream_url1(
            log_group=log_group_name, log_rgn=region, log_stream=e.logStreamName,
            timestamp=e.timestamp, event_id=e.eventId,
        )
        msg_clean = e.message.rstrip()
        csv_rows.append({
            'DateTime': event_ts.isoformat(timespec='milliseconds'),
            'Region': REGION_TO_ABBR.get(region, region),
            'IdType': id_type_name,
            'Msg': msg_clean,
            'Url': event_url,
        })
        print(f'{datetime.strftime(event_ts, fmt)[:-3]}', f'{msg_clean}', f'{event_url}', sep='\t')

    shared_dict[shared_key] = csv_rows

    found = list(on_receive_batch.found_types.keys())
    missing = [t for t in known_types if t not in on_receive_batch.found_types]
    worker_duration = (datetime.now() - worker_dt_start).total_seconds()
    finish_msg = __gen_msg(
        f'{shared_key}: 完成。耗时：{worker_duration:.3f}s 任务总结：{_stats}'
        f' 找到({len(found)}/{len(known_types)})：{found}'
        + (f' 未找到：{missing}' if missing else '')
    )
    shared_msg_dict[shared_key] = [shared_msg_dict[shared_key][0], finish_msg]
    print(finish_msg)


def run_parallel():
    run_timestamp = datetime.now().strftime(FMT_DT_FILE)

    stop_event = multiprocessing.Event()
    with multiprocessing.Manager() as manager:
        shared_msg_dict = manager.dict()
        shared_dict = manager.dict()

        processes: List[multiprocessing.Process] = []
        for func_name, id_types in LOG_GROUP_ID_TYPES.items():
            log_group_name = f'{LOG_GROUP_PREFIX}{func_name}'
            for rgn in REGIONS:
                process = multiprocessing.Process(
                    target=process_worker, args=(
                        log_group_name, rgn, id_types,
                        DT_START_UTC, DT_END_UTC, SEGMENT_DURATION,
                        shared_msg_dict, shared_dict, stop_event,
                    )
                )
                processes.append(process)
                process.start()

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        finally:
            stop_event.set()
            for process in processes:
                process.join()
        print("All Processes completed.")

        # 收集运行输出
        run_lines = []
        for key in sorted(shared_msg_dict.keys()):
            for line in shared_msg_dict[key]:
                run_lines.append(line)

        # 收集日志数据
        all_csv_rows = []
        for key in sorted(shared_dict.keys()):
            all_csv_rows.extend(shared_dict[key])
        all_csv_rows.sort(key=lambda r: r.get('DateTime', ''))

    # 保存文件
    run_log_path = os.path.join(DATA_DIR, f'IdGen_{run_timestamp}_run.log')
    data_csv_path = os.path.join(DATA_DIR, f'IdGen_{run_timestamp}_data.csv')
    save_run_log(run_log_path, run_lines)
    save_data_csv(data_csv_path, all_csv_rows)


if __name__ == '__main__':
    run_parallel()
