import argparse
import multiprocessing
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

# 脚本所在目录（在 os.chdir 之前固定下来，后续 chdir 不影响），默认输出目录基于此。
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_BASE_DIR = os.path.join(SCRIPT_DIR, 'Data', 'SearchCloudWatchLogs')

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


def __default_output_path() -> str:
    """默认输出文件路径：CloudWatch/Data/SearchCloudWatchLogs/SearchCloudWatchLogs_<时间戳>.tsv。"""
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    return os.path.join(OUTPUT_BASE_DIR, f'SearchCloudWatchLogs_{ts}.tsv')


def __format_event_tsv(e: FilterLogEventsResp, log_group_name: str, region: str) -> str:
    """把单条命中事件格式化为 TSV 行：时间\t消息\tURL（与并行模式打印一致）。"""
    fmt_event = '%Y-%m-%d %H:%M:%S.%f'
    event_ts = datetime.fromtimestamp(e.timestamp / 1000, tz=timezone.utc)
    event_url = aws_urls.gen_cloud_watch_log_stream_url1(
        log_group=log_group_name, log_rgn=region, log_stream=e.logStreamName,
        timestamp=e.timestamp, event_id=e.eventId,
    )
    return f'{datetime.strftime(event_ts, fmt_event)[:-3]}\t{e.message.rstrip()}\t{event_url}'


def __is_wsl() -> bool:
    """是否运行在 WSL（sys.platform 为 linux，但内核 release 含 microsoft/WSL）。"""
    if sys.platform != 'linux':
        return False
    try:
        return 'microsoft' in platform.uname().release.lower()
    except Exception:
        return False


def __reveal_in_explorer(path: str):
    """在系统文件管理器中打开并选中文件。找不到可用命令时静默跳过（路径已打印），仅真异常告警。"""
    path = os.path.normpath(os.path.abspath(path))
    try:
        if sys.platform == 'win32':
            # explorer 即使成功也常返回非 0，故不检查返回码；/select, 后必须紧跟路径。
            subprocess.run(['explorer', f'/select,{path}'])
        elif sys.platform == 'darwin':
            if shutil.which('open'):
                subprocess.run(['open', '-R', path], check=False)
        elif __is_wsl():
            # WSL：用 Windows 的 explorer.exe，路径需先经 wslpath -w 转成 Windows 形式。
            if shutil.which('explorer.exe'):
                win_path = path
                if shutil.which('wslpath'):
                    win_path = subprocess.run(
                        ['wslpath', '-w', path], check=True, capture_output=True, text=True,
                    ).stdout.strip()
                subprocess.run(['explorer.exe', f'/select,{win_path}'])
        else:
            # 其它 Linux：有 xdg-open 才开父目录，没有就静默跳过。
            if shutil.which('xdg-open'):
                subprocess.run(['xdg-open', os.path.dirname(path)], check=False)
    except Exception as e:
        print_err(f'打开文件夹失败（不影响结果文件）：{e}')


def run_sequential(
    log_group_names: List[str], regions: List[str], pattern: str,
    dt_start_utc: Optional[datetime], dt_end_utc: Optional[datetime],
    ascending: bool, find_first: bool, segment_duration: timedelta,
    output: Optional[str] = None, open_after: bool = False,
):
    fmt = "%Y-%m-%d %H:%M:%S"
    if output:
        os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    out_fh = open(output, 'w', encoding='utf-8', newline='') if output else None
    total_hits = 0
    try:
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
                for e in (FilterLogEventsResp(**ev) for ev in events):
                    line = __format_event_tsv(e, name, rgn)
                    print(line)
                    if out_fh:
                        out_fh.write(line + '\n')
                total_hits += len(events)
                dt_end = datetime.now()
                duration = (dt_end - dt_start).total_seconds()
                print(f'完成：{name} {REGION_TO_ABBR.get(rgn, rgn)} {dt_end} 耗时：{duration}s 命中：{len(events)}')
    finally:
        if out_fh:
            out_fh.close()
            abs_path = os.path.abspath(output)
            print(f'结果已写入：{abs_path}（共 {total_hits} 条）')
            if open_after:
                __reveal_in_explorer(abs_path)


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

    parser.add_argument('--no-file', action='store_true', default=False,
                        help='[选填] 不写结果文件（串行模式默认会写文件，用此开关关闭）。',
                        )
    parser.add_argument('--no-open', action='store_true', default=False,
                        help='[选填] 写完文件后不自动打开文件管理器选中文件（默认会打开）。',
                        )
    parser.add_argument('--output', '-o',
                        default=None, required=False,
                        help='[选填] 指定结果文件路径（TSV：时间\\t消息\\tURL）。'
                             f'不传则默认写到 {OUTPUT_BASE_DIR} 下自动命名的文件。仅在串行模式（--sequential）下生效。',
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

    if args.output and not args.sequential:
        print_err('--output 仅在串行模式下生效，请同时加上 --sequential。')
        sys.exit(1)

    # 解析输出文件：仅串行模式出文件，默认写到 OUTPUT_BASE_DIR 自动命名，--no-file 关闭，-o 覆盖路径。
    if args.sequential and not args.no_file:
        output = args.output or __default_output_path()
    else:
        output = None
    open_after = bool(output) and not args.no_open

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
        output=output,
        open_after=open_after,
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

    sequential = cfg.pop('sequential')
    output = cfg.pop('output')
    open_after = cfg.pop('open_after')
    if sequential:
        run_sequential(**cfg, output=output, open_after=open_after)
    else:
        run_parallel(**cfg)


if __name__ == '__main__':
    main()
