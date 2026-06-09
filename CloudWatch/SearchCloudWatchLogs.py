import argparse
import multiprocessing
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from CloudWatch.cloud_watch_dataclass import FilterLogEventsResp  # noqa: E402
from CloudWatch.cloud_watch_helper import filter_log_events, filter_log_events_descending  # noqa: E402
from utils import aws_urls  # noqa: E402
from utils.aws_client_error_handler import print_err  # noqa: E402
from utils.aws_consts import REGION_ABBR, REGION_TO_ABBR  # noqa: E402
from utils.exec_env_util import is_running_in_pycharm  # noqa: E402

# region 默认值（仅当 PyCharm 直接 Run 时使用，命令行运行需通过参数传入）
DEFAULT_LOG_GROUP_NAMES: List[str] = [
    name.strip() for name in
    '''
    NemoDev-trunk--47607-LoginFunction
    '''.split('\n')
    if name.strip()
]
DEFAULT_REGIONS: List[str] = [
    'cn-northwest-1',
    # 'ap-northeast-1',
    # 'eu-central-1',
    # 'us-east-1',
]
DEFAULT_DT_START_UTC: Optional[datetime] = datetime(2026, 4, 18, tzinfo=timezone.utc)
DEFAULT_DT_END_UTC: Optional[datetime] = datetime(2026, 4, 22, tzinfo=timezone.utc)
DEFAULT_ASCENDING = False
DEFAULT_FIND_FIRST = True
DEFAULT_SEGMENT_DURATION_MIN = 24 * 60
DEFAULT_PATTERN = r'IdName code resp'
# endregion 默认值


def process_print(
    shared_msg_dict: Dict[str, str], shared_dict: Dict[str, list], stop_event
):
    while not stop_event.is_set():
        time.sleep(10)


def process_worker(
    log_group_name: str, region: str, pattern: str,
    dt_start_utc: Optional[datetime], dt_end_utc: Optional[datetime],
    ascending: bool, find_first: bool, segment_duration: timedelta,
    shared_msg_dict: dict, shared_dict: Dict[str, list], stop_event,
):
    fmt = '%Y-%m-%d %H:%M:%S.%f'

    def __gen_msg(msg: str):
        return f'{datetime.now().strftime(fmt)} > {msg}'

    worker_dt_start = datetime.now()

    shared_key = f'{log_group_name} {REGION_TO_ABBR.get(region, region)}'
    shared_msg_dict[shared_key] = [__gen_msg(f'{shared_key}: 开始')]
    print(shared_msg_dict[shared_key])
    shared_dict[shared_key] = []

    if not ascending and find_first:
        events, _stats = filter_log_events_descending(
            region, log_group_name, pattern,
            dt_start=dt_start_utc, dt_end=dt_end_utc,
            is_stop_on_match=True,
            stop_event=stop_event,
            segment_duration=segment_duration,
        )
    else:
        events, _stats = filter_log_events(
            region, log_group_name, pattern,
            dt_start=dt_start_utc, dt_end=dt_end_utc,
            is_stop_on_match=find_first,
            stop_event=stop_event,
        )

    if not ascending:
        events.reverse()
    log_event_objs = [FilterLogEventsResp(**e) for e in events]
    for e in log_event_objs:
        event_ts = datetime.fromtimestamp(e.timestamp / 1000, tz=timezone.utc)
        event_url = aws_urls.gen_cloud_watch_log_stream_url1(
            log_group=log_group_name, log_rgn=region, log_stream=e.logStreamName,
            timestamp=e.timestamp, event_id=e.eventId,
        )
        print(f'{datetime.strftime(event_ts, fmt)[:-3]}', f'{e.message.rstrip()}', f'{event_url}', sep='\t')
    shared_dict[shared_key] = events

    worker_duration = (datetime.now() - worker_dt_start).total_seconds()
    shared_msg_dict[shared_key] = [__gen_msg(f'{shared_key}: 完成。耗时：{worker_duration:.3f}s 任务总结：{_stats}')]
    print(shared_msg_dict[shared_key])


def run_parallel(
    log_group_names: List[str], regions: List[str], pattern: str,
    dt_start_utc: Optional[datetime], dt_end_utc: Optional[datetime],
    ascending: bool, find_first: bool, segment_duration: timedelta,
):
    stop_event = multiprocessing.Event()
    with multiprocessing.Manager() as manager:
        shared_msg_dict = manager.dict()
        shared_dict = manager.dict()

        processes: List[multiprocessing.Process] = []
        for group_name in log_group_names:
            for rgn in regions:
                process = multiprocessing.Process(
                    target=process_worker, args=(
                        group_name, rgn, pattern,
                        dt_start_utc, dt_end_utc, ascending, find_first, segment_duration,
                        shared_msg_dict, shared_dict, stop_event,
                    )
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


def run_sequential(
    log_group_names: List[str], regions: List[str], pattern: str,
    dt_start_utc: Optional[datetime], dt_end_utc: Optional[datetime],
    ascending: bool, find_first: bool, segment_duration: timedelta,
):
    fmt = "%Y-%m-%d %H:%M:%S"
    for name in log_group_names:
        for rgn in regions:
            dt_start = datetime.now()
            print(f'开始：{name} {REGION_TO_ABBR.get(rgn, rgn)} {dt_start.strftime(fmt)}')
            try:
                if not ascending and find_first:
                    events, _ = filter_log_events_descending(
                        rgn, name, pattern, dt_start=dt_start_utc, dt_end=dt_end_utc, is_stop_on_match=True,
                        segment_duration=segment_duration,
                    )
                else:
                    events, _ = filter_log_events(
                        rgn, name, pattern, dt_start=dt_start_utc, dt_end=dt_end_utc, is_stop_on_match=find_first,
                    )
                if not ascending:
                    events.reverse()
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
        description='搜索 CloudWatch 日志：跨 region、跨 log group，按 pattern 拉取。',
        epilog=(
            '示例：\n'
            '  python SearchCloudWatchLogs.py -lg PartyAnimals--209820-LoginFunction -p \'%[ERROR]%\' '
            '-rgn NX -s "2026-04-18 08:00:00+0800" -e "2026-04-22 08:00:00+0800" --descending --find-first\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('--log-groups', '-lg',
                        nargs='+', required=True,
                        help='一个或多个日志组名。可省略 /aws/lambda/ 前缀。',
                        )
    parser.add_argument('--pattern', '-p',
                        required=True,
                        help='CloudWatch Filter Pattern。注意 %% 通配、引号转义需按 shell 规则处理。',
                        )
    parser.add_argument('--regions', '-rgn',
                        nargs='+', required=True,
                        help='一个或多个 region。允许全名或缩写：BJ/NX/AP/JP/EU/US '
                             '或 cn-north-1/cn-northwest-1/ap-northeast-1/eu-central-1/us-east-1。',
                        )

    parser.add_argument('--start', '-s',
                        default=None, required=False,
                        help='开始时间，必须带时区后缀，格式："2026-04-18 00:00:00+0800" 或 "+0000"。不传则不设下界。',
                        )
    parser.add_argument('--end', '-e',
                        default=None, required=False,
                        help='结束时间，必须带时区后缀，格式："2026-04-22 00:00:00+0800" 或 "+0000"。不传则不设上界。',
                        )

    sort_str_allowed = ['asc', 'desc']
    g_sort = parser.add_mutually_exclusive_group()
    sort_arg_names = ['--order', '--orderby', '--sort', '--ascending', '--descending']
    sort_arg_names_help = f'{", ".join(sort_arg_names)} 只能提供一个'
    sort_kwargs = dict(
        dest='sort_order', default='', required=False,
        choices=sort_str_allowed,
        help=f'[选填] 排序，默认倒序。可用值：{", ".join(sort_str_allowed)}。{sort_arg_names_help}',
    )
    g_sort.add_argument('--ascending', '-asc', action='store_true',
                        default=None, required=False,
                        help=f'[选填] 正序。{sort_arg_names_help}',
                        )
    g_sort.add_argument('--descending', '-desc', action='store_true',
                        default=None, required=False,
                        help=f'[选填] 倒序（默认）。{sort_arg_names_help}',
                        )
    g_sort.add_argument('--order', **sort_kwargs)
    g_sort.add_argument('--orderby', **sort_kwargs)
    g_sort.add_argument('--sort', **sort_kwargs)

    parser.add_argument('--find-first', '-f1', action='store_true',
                        default=False,
                        help='对每个 (log_group, region)，命中第一批后停止。倒序+find-first 用于查"最近一条"。',
                        )

    parser.add_argument('--segment-duration', '-seg',
                        type=int, default=60, required=False,
                        help='[选填] 倒序搜索时每段时长（分钟），默认 60。仅在倒序+find-first 时生效。',
                        )

    parser.add_argument('--sequential', action='store_true', default=False,
                        help='[选填] 串行执行（默认并行多进程）。',
                        )

    return parser.parse_args(args)


def __resolve_config(args) -> dict:
    """把 argparse Namespace 解析成 worker 需要的实参字典。"""
    log_group_names = [
        name if name.startswith('/aws/lambda/') else f'/aws/lambda/{name}'
        for name in args.log_groups
    ]

    bad_regions = [r for r in args.regions if r not in REGION_ABBR and r not in REGION_TO_ABBR]
    if bad_regions:
        print_err(f'地区不支持：{bad_regions}')
        sys.exit(1)
    regions = sorted({REGION_ABBR.get(r, r) for r in args.regions})

    fmt = '%Y-%m-%d %H:%M:%S%z'
    try:
        dt_start = datetime.strptime(args.start, fmt) if args.start else None
        dt_end = datetime.strptime(args.end, fmt) if args.end else None
    except ValueError as e:
        print_err(f'时间解析失败：{e}。要求格式 "YYYY-MM-DD HH:MM:SS+ZZZZ"（必须带时区后缀，如 +0800/+0000）。')
        sys.exit(1)

    if dt_start is not None and dt_end is not None and dt_start >= dt_end:
        print_err(f'开始时间必须早于结束时间。--start={dt_start.isoformat()} --end={dt_end.isoformat()}')
        sys.exit(1)

    if args.ascending:
        ascending = True
    elif args.descending:
        ascending = False
    elif args.sort_order == 'asc':
        ascending = True
    elif args.sort_order == 'desc':
        ascending = False
    else:
        ascending = False  # 默认倒序

    return dict(
        log_group_names=log_group_names,
        regions=regions,
        pattern=args.pattern,
        dt_start_utc=dt_start,
        dt_end_utc=dt_end,
        ascending=ascending,
        find_first=args.find_first,
        segment_duration=timedelta(minutes=args.segment_duration),
        sequential=args.sequential,
    )


def __build_default_argv() -> List[str]:
    """PyCharm 直接 Run 时使用的默认参数（开发模式）。"""
    argv = [
        '--log-groups', *DEFAULT_LOG_GROUP_NAMES,
        '--pattern', DEFAULT_PATTERN,
        '--regions', *DEFAULT_REGIONS,
        '--ascending' if DEFAULT_ASCENDING else '--descending',
        '--segment-duration', str(DEFAULT_SEGMENT_DURATION_MIN),
    ]
    if DEFAULT_DT_START_UTC:
        argv += ['--start', DEFAULT_DT_START_UTC.strftime('%Y-%m-%d %H:%M:%S%z')]
    if DEFAULT_DT_END_UTC:
        argv += ['--end', DEFAULT_DT_END_UTC.strftime('%Y-%m-%d %H:%M:%S%z')]
    if DEFAULT_FIND_FIRST:
        argv.append('--find-first')
    return argv


def main():
    if is_running_in_pycharm() and len(sys.argv) <= 1:
        argv = __build_default_argv()
    else:
        argv = sys.argv[1:]
    args = __parse_args(argv)
    print(f'args: {args}')
    cfg = __resolve_config(args)

    runner = run_sequential if cfg.pop('sequential') else run_parallel
    runner(**cfg)


if __name__ == '__main__':
    main()
