import argparse
import asyncio
import json
import logging
import os
import string
import sys
import time
from dataclasses import asdict
from datetime import datetime
from multiprocessing import Pool
from typing import Dict, List, Optional, Tuple

from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

CURR_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
PROJ_ROOT_PATH = os.path.dirname(CURR_DIR_PATH)
if PROJ_ROOT_PATH not in sys.path:
    sys.path.append(PROJ_ROOT_PATH)

from Lambda.lambda_info_types import Function, FunctionRow, FunctionTable  # noqa: E402
from utils.aws_consts import REGION_ABBR, AllEnvs, Env  # noqa: E402
from utils.aws_aiosession_helper import get_cached_aiosession  # noqa: E402
from utils.aws_urls import get_lambda_function_url  # noqa: E402
from utils.log_span import log_span  # noqa: E402
from utils.SystemTools.file_system_helper import create_dir_if_not_exists  # noqa: E402

# region 配置项
ENV, SUB_ENV = AllEnvs.NemoDevMaprefine, '76700'
REGIONS = [
    'cn-northwest-1',
    'ap-northeast-1',
    'eu-central-1',
    'us-east-1',
]
USE_CACHE = True
# endregion 配置项

CACHE_DIR = os.path.join(CURR_DIR_PATH, 'Data', 'output')
CACHE_TTL_SEC = 3600


class ResourceExplorerUnavailable(Exception):
    """Raised when a region has no usable Resource Explorer index."""


# region 缓存
def _cache_path(env: Env, region: str) -> str:
    tag = 'prod' if env.is_prod_aws else 'dev'
    return os.path.join(CACHE_DIR, f'lambda_cache_{tag}_{env.name}_{region}.json')


def read_cache(env: Env, region: str, ttl_sec: int = CACHE_TTL_SEC) -> Optional[List[Function]]:
    path = _cache_path(env, region)
    if not os.path.exists(path):
        return None
    age = time.time() - os.path.getmtime(path)
    if age > ttl_sec:
        return None
    with open(path, 'r', encoding='utf8') as f_in:
        data = json.load(f_in)
    return [Function.from_dict(d) for d in data]


def write_cache(env: Env, region: str, functions: List[Function]):
    path = _cache_path(env, region)
    create_dir_if_not_exists(file_path=path)
    with open(path, 'w', encoding='utf8') as f_out:
        json.dump([asdict(fn) for fn in functions], f_out, indent=2, ensure_ascii=False)
# endregion 缓存


@log_span(log_args=True, arg_names=['region'])
async def _fetch_via_list_functions(env: Env, region: str) -> List[Function]:
    """通过 list_functions 分页拉取一个 region 内所有 Lambda 函数（用于 cn-* 及 RE 回落场景）。"""
    session = get_cached_aiosession(region=region, is_prod=env.is_prod_aws)
    async with session.create_client(
        'lambda',
        region_name=region,
        config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}, max_pool_connections=50)
    ) as client:
        raw_functions: List[dict] = []
        marker = None
        while True:
            param = {'Marker': marker} if marker else dict()
            try:
                resp = await client.list_functions(**param)  # type: ignore
            except ClientError as e:
                if 'ExpiredTokenException' in e.response['Error']['Code']:
                    # handle_expired_token_exception is not async, so just log and skip
                    logging.error("ExpiredTokenException, please refresh your credentials.")
                    return []
                else:
                    logging.error(f'Exception: {e.__dict__}')
                break
            if resp.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) != 200:
                raise Exception(f'non 200 code: {resp}')
            marker = resp.get('NextMarker', '')
            raw_functions += resp.get('Functions', [])
            if not marker:
                break
        return [Function.from_dict(fn) for fn in raw_functions if 'FunctionName' in fn]


@log_span(log_args=True, arg_names=['region'])
async def _fetch_via_resource_explorer(env: Env, region: str) -> List[Function]:
    """通过 Resource Explorer 查询 Lambda ARN，再并发拉取每个函数完整配置。
    RE 不返回 Timeout/MemorySize/LastModified，所以还得调 get_function 补全。
    实际效果一般，只能只能省几秒，原因应该是 filter 的 paging 太多
    """
    session = get_cached_aiosession(region=region, is_prod=env.is_prod_aws)

    # 只要 function 本体（跳过 versions/aliases/layers），name 交给客户端做
    query = 'resourcetype:AWS::Lambda::Function'
    all_arns: List[str] = []
    total_returned = 0
    async with session.create_client(
        'resource-explorer-2', region_name=region,
        config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}),
    ) as re_client:
        next_token = None
        try:
            while True:
                params = {'QueryString': query, 'MaxResults': 1000}
                if next_token:
                    params['NextToken'] = next_token
                resp = await re_client.search(**params)  # type: ignore
                resources = resp.get('Resources', [])
                total_returned += len(resources)
                for r in resources:
                    arn = r.get('Arn', '')
                    if ':function:' in arn:
                        all_arns.append(arn)
                next_token = resp.get('NextToken')
                if not next_token:
                    break
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ('ValidationException', 'ResourceNotFoundException', 'AccessDeniedException'):
                raise ResourceExplorerUnavailable(f'{region}: {code}')
            raise

    # 客户端按 env 名字过滤
    arns = [a for a in all_arns if a.split(':function:')[1].startswith(env.name)]
    logging.info(
        f'[{region}] RE returned {total_returned} resources '
        f'({len(all_arns)} lambda ARNs, {len(arns)} match env "{env.name}")'
    )

    if not arns:
        return []

    async with session.create_client(
        'lambda', region_name=region,
        config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}, max_pool_connections=50),
    ) as lambda_client:
        semaphore = _make_lambda_semaphore(region)

        async def fetch(arn: str) -> Optional[Function]:
            async with semaphore:
                try:
                    resp = await lambda_client.get_function(FunctionName=arn)  # type: ignore
                    return Function.from_dict(resp['Configuration'])
                except ClientError as e:
                    logging.warning(f'[{region}] get_function {arn} failed: {e}')
                    return None

        results = await asyncio.gather(*(fetch(a) for a in arns))
    return [r for r in results if r is not None]


def _make_lambda_semaphore(region: str) -> asyncio.Semaphore:
    """Lambda 控制面 API 并发上限（cn-* 网络更近，可以放宽一点）。"""
    return asyncio.Semaphore(20 if region.startswith('cn-') else 10)


async def get_all_function_concurrency(env: Env, fn_list: List[Function]) -> Dict[str, dict]:
    """获取所有函数的并发设置
    Args:
        env (Env): 环境配置
        fn_list (List[Function]): 函数列表
    Returns:
        Dict[str, dict]: key为函数名，value为并发设置的字典
    """
    concurrency_data = {}

    # Group by region
    region_groups: Dict[str, List[Function]] = {}
    for fn in fn_list:
        region_groups.setdefault(fn.get_region(), []).append(fn)

    async def fetch_for_region(region: str, fn_group: List[Function]):
        session = get_cached_aiosession(region=region, is_prod=env.is_prod_aws)
        async with session.create_client(
            'lambda',
            region_name=region,
            config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}, max_pool_connections=50)
        ) as client:
            semaphore = _make_lambda_semaphore(region)

            async def get_concurrency(fn: Function):
                async with semaphore:
                    try:
                        resp = await client.get_function_concurrency(FunctionName=fn.FunctionName)  # type: ignore
                        concurrency_data[fn.FunctionName] = resp
                    except ClientError as e:
                        logging.warning(f"[{region}] Error fetching {fn.FunctionName}: {e}")
                        concurrency_data[fn.FunctionName] = {}

            await asyncio.gather(*(get_concurrency(fn) for fn in fn_group))

    await asyncio.gather(*(fetch_for_region(region, group) for region, group in region_groups.items()))
    return concurrency_data


async def get_function_currency_async(env: Env, region: str, function_name: str) -> dict:
    session = get_cached_aiosession(region=region, is_prod=env.is_prod_aws)
    async with session.create_client(
        'lambda',
        region_name=region,
        config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}, max_pool_connections=50)
    ) as client:
        try:
            resp = await client.get_function_concurrency(FunctionName=function_name)  # type: ignore
            return resp
        except ClientError as e:
            logging.warning(f'Exception: {e.__dict__}')
            return {}


def handle_function_n_ccy(fn_fn_ccy: List[Tuple[Function, dict]]):
    """处理函数和并发设置的列表，并打印表格

    Args:
        fn_fn_ccy (List[Tuple[Function, dict]]): Function: 见类定义, dict: 并发设置的返回
    """
    table = FunctionTable()
    for fn, fn_ccy in fn_fn_ccy:
        fn_rgn = fn.FunctionArn.split(':lambda:')[1].split(':')[0]
        if not fn_ccy:
            ccy_setting = '未知'
        elif 'ReservedConcurrentExecutions' in fn_ccy:
            reserved_ccy = fn_ccy['ReservedConcurrentExecutions']
            ccy_setting = 'Throttled' if reserved_ccy == 0 else f'{reserved_ccy}'
        else:
            ccy_setting = '非预留账户并发'
        reserved_ccy = fn_ccy.get('ReservedConcurrentExecutions', -1)
        last_modified_dt = datetime.strptime(fn.LastModified, '%Y-%m-%dT%H:%M:%S.%f%z') if fn.LastModified else None
        table.insert_row(FunctionRow(
            FunctionName=fn.FunctionName,
            Region=fn_rgn,
            Timeout=fn.Timeout,
            MemorySize=fn.MemorySize,
            ConcurrencySetting=ccy_setting,
            LastDeployDt=datetime.strftime(last_modified_dt, '%Y-%m-%d %H:%M %z')[:-2] if last_modified_dt else 'NA',
            FunctionName_href=get_lambda_function_url(fn_rgn, fn.FunctionName),
        ))
    table.print_table(order_by=['FunctionName', 'Region'])


async def get_functions_in_region(
        env: Env, region: str, use_cache: bool = True,
) -> Tuple[List[Function], str]:
    """统一入口：命中缓存直接返回；否则按 partition 分路径拉取，写缓存。

    Returns:
        (functions, source) source ∈ {'cache', 'list_functions', 'resource_explorer', 'list_functions_fallback'}
    """
    if use_cache:
        cached = read_cache(env, region)
        if cached is not None:
            logging.info(f'[{region}] source=cache count={len(cached)}')
            return cached, 'cache'

    if region.startswith('cn-'):
        functions = await _fetch_via_list_functions(env, region)
        source = 'list_functions'
    else:
        try:
            functions = await _fetch_via_resource_explorer(env, region)
            source = 'resource_explorer'
        except ResourceExplorerUnavailable as e:
            logging.warning(f'[{region}] Resource Explorer unavailable ({e}), falling back to list_functions')
            functions = await _fetch_via_list_functions(env, region)
            source = 'list_functions_fallback'

    write_cache(env, region, functions)
    return functions, source


async def region_function_coroutine(rgn: str) -> Dict[str, Tuple[Function, dict]]:
    """获取地区内所有函数信息和并发设置

    Args:
        rgn (str): AWS region

    Returns:
        Dict[str, Tuple[Function, dict]]: key为函数名，value为函数对象、并发设置的元组
    """
    functions, source = await get_functions_in_region(ENV, rgn, use_cache=USE_CACHE)

    fn_curr_env_rgn: List[Function] = []
    for fn in functions:
        fn_name = fn.FunctionName
        is_env_main, is_env_sub = False, False
        if not fn_name.startswith(ENV.name):
            continue
        if fn_name.endswith('FnStateChange'):
            continue
        if fn_name.startswith(f'{ENV.name}--{SUB_ENV}'):
            is_env_sub = True
        elif fn_name.replace(f'{ENV.name}-', '')[0] in string.ascii_uppercase:
            is_env_main = True
        if not (is_env_main or is_env_sub):
            continue
        fn_curr_env_rgn.append(fn)

    logging.info(f'[{rgn}] matched={len(fn_curr_env_rgn)} (from count={len(functions)})')

    fn_ccy = await get_all_function_concurrency(ENV, fn_curr_env_rgn)

    ret = {fn.FunctionName: (fn, fn_ccy.get(fn.FunctionName, {})) for fn in fn_curr_env_rgn}
    return ret


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(filename)s:%(funcName)s:%(lineno)d [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    # 屏蔽第三方库的 INFO 噪音（凭证加载、HTTP 连接池等）
    for noisy in ('botocore', 'boto3', 'aiobotocore', 'urllib3', 's3transfer'):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def region_function_task(region: str):
    # Windows spawn 下子进程不继承 parent 的全局变量修改，需要重新读一次 CLI 参数
    _setup_logging()
    load_global_vars()
    return asyncio.run(region_function_coroutine(region))


async def main_coroutine():
    results: List[Dict[str, Tuple[Function, dict]]]
    results = await asyncio.gather(*(region_function_coroutine(rgn) for rgn in REGIONS))
    fn_fn_ccy = [elem for result in results for elem in result.values()]  # Flatten the list of dicts
    handle_function_n_ccy(fn_fn_ccy)


def main_coroutine_sync():
    _setup_logging()
    load_global_vars()
    ret = asyncio.run(main_coroutine())
    if ret:
        logging.error(f'Error: {ret}')


def main_multiprocess():
    _setup_logging()
    load_global_vars()

    results: List[Dict[str, Tuple[Function, dict]]]
    with Pool(processes=len(REGIONS)) as pool:
        results = pool.map(region_function_task, REGIONS)
    fn_fn_ccy = [elem for result in results for elem in result.values()]  # Flatten the list of dicts
    handle_function_n_ccy(fn_fn_ccy)


def load_global_vars():
    global ENV, SUB_ENV, REGIONS, USE_CACHE

    sys_args = sys.argv[1:]
    if os.environ.get('TERM_PROGRAM', None) == 'vscode':
        logging.info('VsCode 本地调试')
        sys_args = ['-en', 'NemoDev-maprefine', '-sen', '76700', '-rgn', 'NX', 'AP', 'US', 'EU']

    args = parse_args(sys_args)

    # Lambda 地区
    arg_regions: List[str] = args.regions
    arg_regions = sorted([REGION_ABBR.get(rgn, rgn) for rgn in arg_regions])
    REGIONS = arg_regions
    ENV = AllEnvs.get_env_by_name(args.environment_name)
    SUB_ENV = args.sub_environment_name
    USE_CACHE = not args.no_cache


def parse_args(args: List[str]):
    parser = argparse.ArgumentParser(
        description='Get Lambda Function Concurrency'
    )
    parser.add_argument(
        '--environment-name', '-en',
        help='环境名',
        default=ENV.name,
    )
    parser.add_argument(
        '--sub-environment-name', '-sen',
        help='子环境',
        default=SUB_ENV,
    )
    parser.add_argument(
        '--regions', '-rgn',
        help='List of regions',
        default=REGIONS,
        nargs='+',
    )
    parser.add_argument(
        '--no-cache', '-nc',
        help='跳过本地缓存，强制重新拉取（拉完仍会写入缓存）',
        action='store_true',
    )
    return parser.parse_args(args)


if __name__ == '__main__':
    main_multiprocess()
    # main_coroutine_sync()
