"""搜索各日志组中每种 ID 类型最后一次生成的日志。

按 (日志组, 该日志组中的 ID 类型列表) 进行倒序搜索，
使用 on_receive_batch 回调逐段解析，每种类型只保留最新一条，
该日志组的所有类型都找到后提前停止。
"""
import multiprocessing
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

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
# endregion 脚本运行配置


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
    fmt = '%Y-%m-%d %H:%M:%S.%f'
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

    # 从回调中提取每种 ID 类型的最新一条，按时间升序输出
    result_events = list(on_receive_batch.found_types.values())
    result_events.sort(key=lambda e: e.get('timestamp', 0))

    log_event_objs = [FilterLogEventsResp(**e) for e in result_events]
    for e in log_event_objs:
        event_ts = datetime.fromtimestamp(e.timestamp / 1000, tz=timezone.utc)
        event_url = aws_urls.gen_cloud_watch_log_stream_url1(
            log_group=log_group_name, log_rgn=region, log_stream=e.logStreamName,
            timestamp=e.timestamp, event_id=e.eventId,
        )
        print(f'{datetime.strftime(event_ts, fmt)[:-3]}', f'{e.message.rstrip()}', f'{event_url}', sep='\t')
    shared_dict[shared_key] = result_events

    found = list(on_receive_batch.found_types.keys())
    missing = [t for t in known_types if t not in on_receive_batch.found_types]
    worker_duration = (datetime.now() - worker_dt_start).total_seconds()
    shared_msg_dict[shared_key] = [__gen_msg(
        f'{shared_key}: 完成。耗时：{worker_duration:.3f}s 任务总结：{_stats}'
        f' 找到({len(found)}/{len(known_types)})：{found}'
        + (f' 未找到：{missing}' if missing else '')
    )]
    print(shared_msg_dict[shared_key])


def run_parallel():
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


if __name__ == '__main__':
    run_parallel()
