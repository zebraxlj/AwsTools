import asyncio
import os
import string
import sys
from datetime import datetime
from multiprocessing import Pool
from typing import Dict, List, Tuple

from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

CURR_FOLDER_NAME = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from Lambda.lambda_info_types import Function, FunctionRow, FunctionTable  # noqa: E402
from utils.aws_consts import AllEnvs, Env  # noqa: E402
from utils.aws_aiosession_helper import get_cached_aiosession  # noqa: E402
from utils.aws_urls import get_lambda_function_url  # noqa: E402

# region 配置项
ENV, SUB_ENV = AllEnvs.NemoDevMaprefine, '76700'
REGIONS = [
    'cn-northwest-1',
    'ap-northeast-1',
    'eu-central-1',
    'us-east-1',
]
# endregion 配置项

DT_FMT = '%Y-%m-%d %H:%M:%S'


async def get_env_rgn_functions_async(env: Env, region: str, verbose: bool = False) -> Dict[str, dict]:
    """_summary_

    Args:
        env (Env): environment
        region (str): AWS region

    Returns:
        Dict[str, dict]: key为函数名，value为函数信息的字典
    """
    if verbose:
        dt_start = datetime.now()
        print(f'{dt_start.strftime(DT_FMT)} >> Get functions for region start. region={region}')
    session = get_cached_aiosession(region=region, is_prod=env.is_prod_aws)
    async with session.create_client(
        'lambda',
        region_name=region,
        config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}, max_pool_connections=50)
    ) as client:
        functions = []
        marker = None
        while True:
            param = {'Marker': marker} if marker else dict()
            try:
                resp = await client.list_functions(**param)  # type: ignore
            except ClientError as e:
                if 'ExpiredTokenException' in e.response['Error']['Code']:
                    # handle_expired_token_exception is not async, so just print and skip
                    print("ExpiredTokenException, please refresh your credentials.")
                    return {}
                else:
                    print(f'Exception: {e.__dict__}')
                break
            if resp.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) != 200:
                raise Exception(f'non 200 code: {resp}')
            marker = resp.get('NextMarker', '')
            functions += resp.get('Functions', [])
            if not marker:
                break
        if verbose:
            dt_stop = datetime.now()
            print(
                f'{dt_stop.strftime(DT_FMT)} >> Get functions for region stop. region={region}, '
                f'span={(dt_stop - dt_start).total_seconds()}s'
            )
        return {fn['FunctionName']: fn for fn in functions if 'FunctionName' in fn}


_RGN_SEMAPHORE: Dict[str, asyncio.Semaphore] = {}  # tune this based on your observed rate limits


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
            async def get_concurrency(fn: Function):
                if region not in _RGN_SEMAPHORE:
                    if region.startswith('cn-'):
                        _RGN_SEMAPHORE[region] = asyncio.Semaphore(20)
                    else:
                        _RGN_SEMAPHORE[region] = asyncio.Semaphore(10)
                semaphore = _RGN_SEMAPHORE[region]
                async with semaphore:
                    try:
                        resp = await client.get_function_concurrency(FunctionName=fn.FunctionName)  # type: ignore
                        concurrency_data[fn.FunctionName] = resp
                    except ClientError as e:
                        print(f"[{region}] Error fetching {fn.FunctionName}: {e}")
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
            print(f'Exception: {e.__dict__}')
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


async def region_function_coroutine(rgn: str) -> Dict[str, Tuple[Function, dict]]:
    """获取地区内所有函数信息和并发设置

    Args:
        rgn (str): AWS region

    Returns:
        Dict[str, Tuple[Function, dict]]: key为函数名，value为函数对象、并发设置的元组
    """
    fn_dict: Dict[str, dict] = await get_env_rgn_functions_async(ENV, rgn)

    fn_curr_env_rgn: List[Function] = []
    for fn_name, fn_info in fn_dict.items():
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
        fn_curr_env_rgn.append(Function.from_dict(fn_info))

    fn_ccy = await get_all_function_concurrency(ENV, fn_curr_env_rgn)

    ret = {fn.FunctionName: (fn, fn_ccy.get(fn.FunctionName, {})) for fn in fn_curr_env_rgn}
    return ret


def region_function_task(region: str):
    return asyncio.run(region_function_coroutine(region))


async def main_coroutine():
    results: List[Dict[str, Tuple[Function, dict]]]
    results = await asyncio.gather(*(region_function_coroutine(rgn) for rgn in REGIONS))
    fn_fn_ccy = [elem for result in results for elem in result.values()]  # Flatten the list of dicts
    handle_function_n_ccy(fn_fn_ccy)


def main_coroutine_sync():
    ret = asyncio.run(main_coroutine())
    if ret:
        print(f'Error: {ret}')


def main_multiprocess():
    results: List[Dict[str, Tuple[Function, dict]]]
    with Pool(processes=len(REGIONS)) as pool:
        results = pool.map(region_function_task, REGIONS)
    fn_fn_ccy = [elem for result in results for elem in result.values()]  # Flatten the list of dicts
    handle_function_n_ccy(fn_fn_ccy)


if __name__ == '__main__':
    main_multiprocess()
    # main_coroutine_sync()
