import argparse
import json
import multiprocessing
import os
import sys
import time
from datetime import datetime
from typing import Dict, List

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from CloudWatch.cloud_watch_helper import get_log_client, filter_log_events  # noqa: E402
from utils.aws_client_error_handler import print_err  # noqa: E402
from utils.aws_consts import REGION_ABBR, REGION_TO_ABBR, AllEnvs  # noqa: E402

'''
PartyAnimals--158820-StoreFunction
PartyAnimals--159490-StoreFunction

    PartyAnimals--160950-StoreFunction
'''

# region 脚本运行配置
LOG_GROUP_NAMES = [
    name.strip() for name in
    '''
    PartyAnimals--160950-LoginFunction
    '''.split('\n')
    if name.strip()
]
REGIONS = [
    'cn-northwest-1',
    # 'ap-northeast-1',
    # 'eu-central-1',
    # 'us-east-1',
    ]
DT_START_UTC = None
DT_END_UTC = None
# DT_START_UTC = datetime(2025, 3, 27)
# DT_END_UTC = datetime(2025, 4, 3)
ASCENDING = True  # filter_log_events 不支持排序，且默认顺序。当需要倒叙时，要用 describe_log_streams 倒叙获取日志
FIND_FIRST = False
# PATTERN = r'%bad publisher key%'
# PATTERN = r'request = Operation ClaimBpLoginRewardAfterFinish AccountId QGH43Y'
# PATTERN = r'%\[ERROR\]%'

# PATTERN = r'AWS.SimpleQueueService.NonExistentQueue'
# PATTERN = r' body event is Operation MiniGameSettle Info AccountId LiarGame MiniGameSettleModelDict AcctGameScoreList'

# 检查 steam 账户货币
# PATTERN = r'get_player_steam_currency_type Steam get user info timeout'  #      NX:27452  JP:7      EU:92     US:24
# PATTERN = r'START STORE LOGIC is sqs False body event is Query currencyType'  # NX:462502 JP:148510 EU:112354 US:130949

# 检查 steam 现金购买
# PATTERN = r'START STORE LOGIC request Query mutation createCashProductTxn'  # NX:7228 JP:824 EU:285 US:374
# PATTERN = r'FriendPass Player use this func'  #                               NX:0    JP:3   EU:0   US:0
# PATTERN = r'Now cash product can not be sent'  #                              NX:0    JP:0   EU:0   US:0
# PATTERN = r'Platform type not match'  #                                       NX:0    JP:0   EU:0   US:0
# PATTERN = r'Only can buy cash product through this API'  #                    NX:0    JP:0   EU:0   US:0
# PATTERN = r'Only can input one product'  #                                    NX:0    JP:0   EU:0   US:0
# PATTERN = r'User transaction is banned by Steam'  #                           NX:42   JP:15  EU:0   US:0
# PATTERN = r'GraphQLError code is 2106 message is Steam get user info Exception is The read operation timed out'  # NX:6269 JP:27 EU:48 US:21  这个是几个方法共用的

# PATTERN = r'Finalize order sent to Steam but Steam is processing before time out need AWS SQS operation'

# PATTERN = r'START LOGIN LOGIC body event is platformId sessionTicket'  # NX:639920  JP: EU: US:

# ISteamUser/CheckAppOwnership/v2/
# PATTERN = r'ERROR check_app_ownership Bad Resp'  #                       NX:0       JP: EU: US:
# PATTERN = r'ERROR check_app_ownership missing ownersteamid'  #           NX:0       JP: EU: US:
# PATTERN = r'ERROR check_app_ownership code header body'  #               NX:42      JP: EU: US:

PATTERN = r'Steam Changed ErrCode code'

# endregion 脚本运行配置


def temp_event_tracking():
    events = filter_log_events(
        'cn-northwest-1', 'PartyAnimals-EventTrackingFunction', r'%lambda_handler .*?achievement_obtain%',
        datetime.strptime('2024-07-20 18:29:00+0800', '%Y-%m-%d %H:%M:%S%z'),
        datetime.strptime('2024-07-20 18:30:00+0800', '%Y-%m-%d %H:%M:%S%z'),
    )
    # print(json.dumps(events, indent=4))
    for e in events:
        body = e["message"].split("'body': '")[1].split("',")[0].replace("\\\\", "\\")
        print(body)
        body_dict = json.loads(body)
        print(json.dumps(body_dict, indent=4))

    # m = events[-1]["message"].split("body/event is ")[1]
    # print(m)
    # print(json.loads(m))

    # d = json.loads('{"Query": "mutation { trackAchievementObtainList(eventStrList: [\\"{\\\\\\"acct_id\\\\\\": \\\\\\"Q7X9EK\\\\\\", \\\\\\"properties\\\\\\": {\\\\\\"app_version_int\\\\\\": 128820, \\\\\\"level\\\\\\": 1, \\\\\\"session_id\\\\\\": \\\\\\"Q7X9EK_V/Vo9I\\\\\\", \\\\\\"is_friend_pass\\\\\\": false, \\\\\\"is_family_share\\\\\\": true, \\\\\\"gateway_region\\\\\\": \\\\\\"cn-northwest-1\\\\\\", \\\\\\"achv_id\\\\\\": \\\\\\"ACV010\\\\\\", \\\\\\"currency_before\\\\\\": 0, \\\\\\"currency_after\\\\\\": 0, \\\\\\"kart_gear_before\\\\\\": 0, \\\\\\"kart_gear_after\\\\\\": 0, \\\\\\"nemo_buck_before\\\\\\": 0, \\\\\\"nemo_buck_after\\\\\\": 0, \\\\\\"rewards\\\\\\": [{\\\\\\"item_id\\\\\\": \\\\\\"PF0009@1.0\\\\\\", \\\\\\"item_qty\\\\\\": 1}]}, \\\\\\"time\\\\\\": 1721469035.702602, \\\\\\"uuid\\\\\\": \\\\\\"6639be7a-8f6c-4c35-8453-5b39df358529\\\\\\", \\\\\\"event_name\\\\\\": \\\\\\"achievement_obtain\\\\\\"}\\"]) {ok} }", "queueUrl": "https://cn-northwest-1.queue.amazonaws.com.cn/471636885451/PartyAnimals-EventTrkSqsQueue1"}')
    # d = json.loads('{"Query": "mutation { trackAchievementObtainList(eventStrList: [\\"{\\\\\\"acct_id\\\\\\": \\\\\\"H6GVCC\\\\\\", \\\\\\"properties\\\\\\": {\\\\\\"app_version_int\\\\\\": 128820, \\\\\\"level\\\\\\": 100, \\\\\\"session_id\\\\\\": \\\\\\"H6GVCC_V/Vo9I\\\\\\", \\\\\\"is_friend_pass\\\\\\": false, \\\\\\"is_family_share\\\\\\": false, \\\\\\"gateway_region\\\\\\": \\\\\\"cn-northwest-1\\\\\\", \\\\\\"achv_id\\\\\\": \\\\\\"ACV103\\\\\\", \\\\\\"currency_before\\\\\\": 1275589, \\\\\\"currency_after\\\\\\": 1275589, \\\\\\"kart_gear_before\\\\\\": 600, \\\\\\"kart_gear_after\\\\\\": 600, \\\\\\"nemo_buck_before\\\\\\": 8072, \\\\\\"nemo_buck_after\\\\\\": 8172, \\\\\\"rewards\\\\\\": []}, \\\\\\"time\\\\\\": 1721471341.304736, \\\\\\"uuid\\\\\\": \\\\\\"066f36b9-62e0-405f-ab6c-357a10a5e7cb\\\\\\", \\\\\\"event_name\\\\\\": \\\\\\"achievement_obtain\\\\\\"}\\"]) {ok} }", "queueUrl": "https://cn-northwest-1.queue.amazonaws.com.cn/471636885451/PartyAnimals-EventTrkSqsQueue1"}')
    # print(json.dumps(d, indent=4))
    # sql = d['Query']


def process_print(
    shared_msg_dict: Dict[str, str], shared_dict: Dict[str, list], stop_event
):
    while not stop_event.is_set():
        # sys_type = platform.system()
        # if sys_type == 'Windows':
        #     os.system('cls')
        # else:
        #     os.system('clear')
        # for k, v in shared_msg_dict.items():
        #     print(k, v)
        time.sleep(10)


def process_worker(
    log_group_name: str, region: str,
    find_first: bool,
    shared_msg_dict: dict, shared_dict: Dict[str, list], stop_event,
):
    def __gen_msg(msg: str):
        return f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} > {msg}'

    worker_dt_start = datetime.now()

    shared_key = f'{log_group_name} {REGION_TO_ABBR.get(region, region)}'
    shared_msg_dict[shared_key] = [__gen_msg(f'{shared_key}: 开始')]
    print(shared_msg_dict[shared_key])
    shared_dict[shared_key] = []

    # 获取日志
    env = AllEnvs.get_env_by_name(log_group_name.split('/')[-1].split('--')[0])
    client = get_log_client(region, env)
    next_tkn = ''
    while not stop_event.is_set():
        kwargs = {'nextToken': next_tkn} if next_tkn else dict()
        kwargs = {**kwargs, **({'filterPattern': PATTERN} if PATTERN else {})}

        response = client.filter_log_events(logGroupName=log_group_name, **kwargs)
        events, next_tkn = response['events'], response.get('nextToken', '')
        if events:
            print(events)
            shared_dict[shared_key] += events
            if find_first:
                break
        if not next_tkn:
            break

    worker_duration = (datetime.now() - worker_dt_start).total_seconds()
    shared_msg_dict[shared_key] = [__gen_msg(f'{shared_key}: 完成。耗时：{worker_duration}s 命中：{len(events)}')]
    print(shared_msg_dict[shared_key])
    print(__gen_msg(f'{shared_key}: 完成。耗时：{worker_duration}s 命中：{len(events)}'))


def run_parallel(log_group_names: List[str], regions: List[str]):
    log_group_names = log_group_names if log_group_names else LOG_GROUP_NAMES
    regions = regions if regions else REGIONS

    stop_event = multiprocessing.Event()
    with multiprocessing.Manager() as manager:
        shared_msg_dict = manager.dict()
        shared_dict = manager.dict()

        processes: List[multiprocessing.Process] = []
        for group_name in log_group_names:
            for rgn in regions:
                process = multiprocessing.Process(
                    target=process_worker, args=(group_name, rgn, FIND_FIRST, shared_msg_dict, shared_dict, stop_event)
                )
                processes.append(process)
                process.start()

        process_print_output = multiprocessing.Process(
            target=process_print, args=(shared_msg_dict, shared_dict, stop_event)
        )
        process_print_output.start()

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        finally:
            stop_event.set()
            process_print_output.join()
            for process in processes:
                process.join()
        print("All Processes completed.")


def run_sequential(log_group_names: List[str], regions: List[str]):
    log_group_names = log_group_names if log_group_names else LOG_GROUP_NAMES
    regions = regions if regions else REGIONS

    fmt = "%Y-%m-%d %H:%M:%S"
    for name in log_group_names:
        for rgn in regions:
            dt_start = datetime.now()
            print(f'开始：{name} {REGION_TO_ABBR.get(rgn, rgn)} {dt_start.strftime(fmt)}')
            try:
                events = filter_log_events(
                    rgn, name, PATTERN, dt_start=DT_START_UTC, dt_end=DT_END_UTC, is_stop_on_match=FIND_FIRST
                )
            except Exception as e:
                print_err(f'{name} {REGION_TO_ABBR.get(rgn, rgn)} {e}')
                events = []
            if events:
                print(events)
            dt_end = datetime.now()
            duration = (dt_end - dt_start).total_seconds()
            print(f'完成：{name} {REGION_TO_ABBR.get(rgn, rgn)} {dt_end} 耗时：{duration}s 命中：{len(events)}')


def __parse_args(args: List[str]):
    parser = argparse.ArgumentParser(
        description='',
        epilog='用例：',
    )
    parser.add_argument('--find-first', '-f1', action='store_true',
                        default=False,
                        help='对于每个日志组，一旦命中，则停止对这个日志组继续搜索',
                        )
    parser.add_argument('--regions', '-rgn',
                        help=(
                            'fleet 地区。允许: cn-north-1, BJ, cn-northwest-1, NX, ap-northeast-1, AP, '
                            'eu-central-1, EU, us-east-1, US'
                        ),
                        nargs='+',
                        default=None,
                        )

    # 日志开始结束时间
    parser.add_argument('--utc-start', '-utc-s',
                        help='开始时间。格式：2024-07-20 18:29:00+0800',
                        default=None, required=False,
                        )
    parser.add_argument('--utc-end', '-utc-e',
                        help='结束时间。格式：2024-07-20 18:29:00+0800',
                        default=None, required=False,
                        )

    # 排序拉取日志
    sort_str_allowed = ['asc', 'desc']
    g_sort = parser.add_mutually_exclusive_group()
    sort_arg_names = ['--order', '--orderby', '--sort', '--ascending', '--descending']
    sort_arg_names_help = f'{", ".join(sort_arg_names)} 只能提供一个'
    sort_kwargs = {
                        'dest': 'sort_order', 'default': '', 'required': False,
                        'choices': sort_str_allowed,
                        'help': f'[选填] 排序获取日志，默认正序获取。可用值：{", ".join(sort_str_allowed)}。{sort_arg_names_help}',
    }

    g_sort.add_argument('--ascending', '-asc', action='store_true',
                        default=None, required=False,
                        help=f'[选填] 正序获取日志，默认正序获取。{sort_arg_names_help}',
                        )
    g_sort.add_argument('--descending', '-desc', action='store_true',
                        default=None, required=False,
                        help=f'[选填] 倒序获取日志，默认正序获取。{sort_arg_names_help}',
                        )
    g_sort.add_argument('--order', **sort_kwargs)
    g_sort.add_argument('--orderby', **sort_kwargs)
    g_sort.add_argument('--sort', **sort_kwargs)

    return parser.parse_args(args)


def __prepare_global_var(args):
    arg_regions = args.regions if args.regions else []
    global LOG_GROUP_NAMES, REGIONS, FIND_FIRST

    for idx, name in enumerate(LOG_GROUP_NAMES):
        LOG_GROUP_NAMES[idx] = name if name.startswith('/aws/lambda/') else f'/aws/lambda/{name}'

    FIND_FIRST = args.find_first
    print('__prepare_global_var', 'FIND_FIRST', FIND_FIRST)

    # 处理地区
    bad_regions = [r for r in arg_regions if r not in REGION_ABBR and r not in REGION_TO_ABBR]
    if bad_regions:
        print_err(f'地区不支持：{bad_regions}')
        sys.exit(1)

    arg_regions = [REGION_TO_ABBR.get(r, r) for r in arg_regions if r is not None]
    REGIONS = sorted(r for r in arg_regions if r) if arg_regions else REGIONS

    # # 处理时间
    # if args.utc_start:
    #
    # if args.utc_end:
    #     pass
    #
    # 处理排序
    global ASCENDING
    sort_order = args.sort_order
    if sort_order == 'desc' or args.descending:
        ASCENDING = False


def main(is_run_parallel: bool):
    args = __parse_args(sys.argv[1:])
    __prepare_global_var(args)

    if is_run_parallel:
        run_parallel(LOG_GROUP_NAMES, REGIONS)
    else:
        run_sequential(LOG_GROUP_NAMES, REGIONS)


def test():
    print(sys.argv[1:])
    args = __parse_args(sys.argv[1:])
    print(args)

    global ASCENDING
    sort_order = args.sort_order
    if sort_order == 'desc' or args.descending:
        print('here')
        ASCENDING = False


if __name__ == '__main__':
    # sys.argv += ['--find-first']
    # main(is_run_parallel=True)
    main(is_run_parallel=False)

    # from cloud_watch_helper import describe_log_streams_all
    # log_streams_all = describe_log_streams_all('cn-northwest-1', 'PartyAnimals--159490-StoreFunction')
    # print(len(log_streams_all))

    # test()
