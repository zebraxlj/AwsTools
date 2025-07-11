import json
import multiprocessing
import os
import string
import sys
from datetime import datetime
from typing import Dict, List

import boto3
import boto3.session
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

CURR_FOLDER_NAME = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from Lambda.lambda_info_types import Function, FunctionRow, FunctionTable  # noqa: E402
from utils.aws_client_error_handler import handle_expired_token_exception  # noqa: E402
from utils.aws_client_helper import get_aws_profile  # noqa: E402
from utils.aws_consts import AllEnvs, Env  # noqa: E402
from utils.aws_urls import get_lambda_function_url  # noqa: E402
from utils.SystemTools.file_system_helper import create_dir_if_not_exists  # noqa: E402

# region 配置项
ENV, SUB_ENV = AllEnvs.NemoDevMaprefine, '76700'
# ENV, SUB_ENV = AllEnvs.NemoDevTrunk, '47607'
# ENV, SUB_ENV = AllEnvs.PartyAnimals, ''
REGIONS = [
    'cn-northwest-1',
    'ap-northeast-1',
    'eu-central-1',
    'us-east-1',
]
# endregion 配置项

DT_FMT = '%Y-%m-%d %H:%M:%S'
OUTPUT_DIR = f'./{CURR_FOLDER_NAME}/Data/output'


def get_env_rgn_functions(env: Env, region: str) -> List[dict]:
    session = boto3.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('lambda', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    functions = []
    marker = None
    while True:
        param = {'Marker': marker} if marker else dict()

        try:
            resp: dict = client.list_functions(**param)
        except ClientError as e:
            if 'ExpiredTokenException' in e.response['Error']['Code']:
                handle_expired_token_exception(session)
                sys.exit(0)
            else:
                print(f'Exception: {e.__dict__}')
            break
        if resp.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) != 200:
            raise Exception(f'non 200 code: {resp}')
        marker = resp.get('NextMarker', '')
        functions += resp.get('Functions', [])
        if not marker:
            break
    # 检查 aws 是否改了接口返回
    for i in range(len(functions)-1, -1, -1):
        if 'FunctionName' not in functions[i]:
            print('[Warning] resp missing key: "FunctionName"')
    return functions


def get_env_rgn_functions_worker(
        env: Env, region: str,
        shared_dict_rgn_fn: Dict[str, Dict[str, dict]],
        stop_event, manager,
        verbose: bool = False,
):
    """
    Args:
        env (Env): 环境，用于区分生产和开发环境
        region (str): lambda 地区
        shared_dict_rgn_fn (dict[str, dict[str, dict]]): dict[region, dict[fn_name, fn_info]]
        stop_event (multiprocessing.Event): 中断当前任务
        manager (_type_): 用于创建共享字典里的子共享字典

    Raises:
        Exception: _description_
    """
    if verbose:
        dt_start = datetime.now()
        print(f'{dt_start.strftime(DT_FMT)} » Get functions start. region={region}')
    session = boto3.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('lambda', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    functions = []
    marker = None
    while True and not stop_event.is_set():
        param = {'Marker': marker} if marker else dict()

        try:
            resp: dict = client.list_functions(**param)
        except ClientError as e:
            if 'ExpiredTokenException' in e.response['Error']['Code']:
                handle_expired_token_exception(session)
                sys.exit(0)
            else:
                print(f'Exception: {e.__dict__}')
            break
        if resp.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) != 200:
            raise Exception(f'non 200 code: {resp}')
        marker = resp.get('NextMarker', '')
        functions += resp.get('Functions', [])

        if region not in shared_dict_rgn_fn:
            shared_dict_rgn_fn[region] = manager.dict()
        for fn in functions:
            # 检查 aws 是否改了接口返回
            if 'FunctionName' not in fn:
                print('[Warning] resp missing key: "FunctionName"')
                continue
            shared_dict_rgn_fn[region][fn['FunctionName']] = fn

        if not marker:
            break
    if verbose:
        dt_stop = datetime.now()
        print(
            f'{dt_stop.strftime(DT_FMT)} » Get functions stop. region={region} cnt={len(functions)} '
            f'span={(dt_stop-dt_start).total_seconds()}s'
        )


def get_functions_concurrency(env: Env, region: str, function_names: List[str]) -> Dict[str, dict]:
    session = boto3.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('lambda', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    ret = {}
    for function_name in function_names[:]:
        resp = client.get_function_concurrency(FunctionName=function_name)
        ret[function_name] = resp
    return ret


def get_function_concurrency_worker(
        env: Env, region: str, function_name: str, shared_dict: dict, stop_event, verbose: bool = False
):
    if verbose:
        dt_start = datetime.now()
        print(f'{dt_start.strftime(DT_FMT)} » Get function concurrency start. region={region} fn={function_name}')
    session = boto3.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('lambda', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))
    while not stop_event.is_set():
        try:
            resp = client.get_function_concurrency(FunctionName=function_name)
            shared_dict[function_name] = resp
            break
        except ClientError as e:
            print(f'Exception: {e.__dict__}')
            break
    if verbose:
        dt_stop = datetime.now()
        print(
            f'{dt_stop.strftime(DT_FMT)} » Get function concurrency stop. region={region} fn={function_name} '
            f'span={(dt_stop-dt_start).total_seconds()}s'
        )


# region 保存、读取 json 文件
def __get_json_path(file_name: str) -> str:
    return f'{OUTPUT_DIR}/{file_name}' + ('' if file_name.endswith('.json') else '.json')


def read_dict_of_dict_from_json(file_name: str) -> Dict[str, dict]:
    file_path = __get_json_path(file_name)
    with open(file_path, 'r', encoding='utf8') as f_in:
        data = f_in.read()
        functions = json.loads(data)
        return functions


def read_list_of_dict_from_json(file_name: str) -> List[dict]:
    file_path = __get_json_path(file_name)
    with open(file_path, 'r', encoding='utf8') as f_in:
        data = f_in.read()
        functions = json.loads(data)
        return functions


def save_dict_of_dict_to_json(file_name: str, dict_of_dict: Dict[str, dict]):
    file_path = __get_json_path(file_name)
    create_dir_if_not_exists(file_path=file_path)
    print(f'Save File: {file_path}')
    with open(file_path, 'w', encoding='utf8') as f_out:
        f_out.writelines(json.dumps(dict_of_dict, indent=4))
    print('Save File: Complete')


def save_list_of_dict_to_json(file_name: str, list_of_dict: List[dict]):
    if not list_of_dict:
        return
    file_path = __get_json_path(file_name)
    create_dir_if_not_exists(file_path=file_path)
    print(f'Save File: {file_path}')
    with open(file_path, 'w', encoding='utf8') as f_out:
        f_out.writelines(json.dumps(list_of_dict, indent=4))
    print('Save File: Complete')
# endregion 保存、读取 json 文件


def parse_list_functions_resp(functions: List[dict]) -> List[Function]:
    """ 解析 client.list_functions 返回的 Functions 字段
    :param functions: client.list_functions 返回的 Functions 字段 或 同结构参数
    """
    fn_all = []
    for fn_dict in functions:
        fn_all.append(Function.from_dict(fn_dict))
        # print(fn_all[-1])
    return fn_all


def main_parallel():
    fn_ccy_all: Dict = {}
    table = FunctionTable()

    # 获取所有地区的函数信息
    rgn_fn_all: Dict[str, Dict[str, dict]] = {}
    with multiprocessing.Manager() as manager:
        stop_event = multiprocessing.Event()
        shared_dict_env_rgn_fn = manager.dict()
        processes_get_fn = []
        for rgn in REGIONS:
            process = multiprocessing.Process(
                target=get_env_rgn_functions_worker, args=(ENV, rgn, shared_dict_env_rgn_fn, stop_event, manager)
            )
            processes_get_fn.append(process)
            process.start()
        try:
            for process in processes_get_fn:
                process.join()
            for k, v in shared_dict_env_rgn_fn.items():
                rgn_fn_all[k] = dict(v)  # 将 manager.dict 转换为普通 dict
        except KeyboardInterrupt as e:
            stop_event.set()
            for process in processes_get_fn:
                process.join()
            # 重新抛出异常
            raise e

    # 过滤出当前环境的函数
    fn_curr_env_rgn: List[Function] = []
    for _, fn_dict in rgn_fn_all.items():
        print(f'Processing region: {_} {type(fn_dict)} {len(fn_dict)}')
        for fn_name, fn_info in fn_dict.items():
            is_env_main, is_env_sub = False, False
            if not fn_name.startswith(ENV.name):
                continue
            if fn_name.endswith('FnStateChange'):
                continue
            if fn_name.startswith(f'{ENV.name}--{SUB_ENV}'):
                # 子环境方法
                is_env_sub = True
            elif fn_name.replace(f'{ENV.name}-', '')[0] in string.ascii_uppercase:
                # 主环境方法
                is_env_main = True
            # 不是当前环境的方法
            if not (is_env_main or is_env_sub):
                continue
            fn_curr_env_rgn.append(Function.from_dict(fn_info))

    # 获取所有地区的函数并发设置
    with multiprocessing.Manager() as manager:
        stop_event = multiprocessing.Event()
        shared_dict_fn_ccy = manager.dict()
        processes_func_ccy = []
        for func in fn_curr_env_rgn:
            rgn, fn_name = func.get_region(), func.FunctionName
            process = multiprocessing.Process(
                target=get_function_concurrency_worker, args=(ENV, rgn, fn_name, shared_dict_fn_ccy, stop_event)
            )
            processes_func_ccy.append(process)
            process.start()
        try:
            for process in processes_func_ccy:
                process.join()
            fn_ccy_all = dict(shared_dict_fn_ccy)
        except KeyboardInterrupt as e:
            stop_event.set()
            for process in processes_func_ccy:
                process.join()
            # 重新抛出异常
            raise e

    # 输出函数表格
    for fn in fn_curr_env_rgn:
        fn_rgn = fn.FunctionArn.split(':lambda:')[1].split(':')[0]
        fn_ccy = fn_ccy_all.get(fn.FunctionName, {})
        if 'ReservedConcurrentExecutions' in fn_ccy:
            reserved_ccy = fn_ccy['ReservedConcurrentExecutions']
            ccy_setting = 'Throttled' if reserved_ccy == 0 else f'{reserved_ccy}'
        else:
            ccy_setting = '非预留账户并发'
        reserved_ccy = fn_ccy_all.get(fn.FunctionName, {}).get('ReservedConcurrentExecutions', -1)
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


def main():
    fn_all: List[Function] = []
    fn_ccy_all: Dict = {}
    table = FunctionTable()
    for rgn in REGIONS:
        file_name_functions = f'{"prod" if ENV.is_prod_aws else "dev"}_{rgn}_Functions.json'
        file_path_functions = __get_json_path(file_name_functions)
        if os.path.exists(file_path_functions) and os.path.isfile(file_path_functions):
            # 检查文件最后修改时间
            print(datetime.fromtimestamp(os.path.getmtime(file_path_functions)))
            # 读取本地文件
            rgn_funcs = read_list_of_dict_from_json(file_name_functions)
        rgn_funcs = get_env_rgn_functions(env=ENV, region=rgn)
        save_list_of_dict_to_json(file_name_functions, rgn_funcs)
        rgn_funcs = read_list_of_dict_from_json(file_name_functions)
        fn_all += parse_list_functions_resp(rgn_funcs)

    # 过滤出当前环境的函数
    fn_curr_env_rgn: List[Function] = []
    for fn in fn_all:
        is_env_main, is_env_sub = False, False
        if not fn.FunctionName.startswith(ENV.name):
            continue
        if fn.FunctionName.endswith('FnStateChange'):
            continue
        if fn.FunctionName.startswith(f'{ENV.name}--{SUB_ENV}'):
            # 子环境方法
            is_env_sub = True
        elif fn.FunctionName.replace(f'{ENV.name}-', '')[0] in string.ascii_uppercase:
            # 主环境方法
            is_env_main = True
        # 不是当前环境的方法
        if not (is_env_main or is_env_sub):
            continue
        fn_curr_env_rgn.append(fn)

    # 获取当前环境函数并发设置
    fn_ccy_all: dict = {}
    with multiprocessing.Manager() as manager:
        stop_event = multiprocessing.Event()
        shared_dict = manager.dict()
        processes_func_ccy = []
        for fn in fn_curr_env_rgn:
            process = multiprocessing.Process(
                target=get_function_concurrency_worker, args=(ENV, rgn, fn.FunctionName, shared_dict, stop_event)
            )
            processes_func_ccy.append(process)
            process.start()
        try:
            for process in processes_func_ccy:
                process.join()
            fn_ccy_all = dict(shared_dict)
        except KeyboardInterrupt as e:
            stop_event.set()
            for process in processes_func_ccy:
                process.join()
            # 重新抛出异常
            raise e

    # 输出函数表格
    for fn in fn_curr_env_rgn:
        fn_rgn = fn.FunctionArn.split(':lambda:')[1].split(':')[0]
        fn_ccy = fn_ccy_all.get(fn.FunctionName, {})
        if 'ReservedConcurrentExecutions' in fn_ccy:
            reserved_ccy = fn_ccy['ReservedConcurrentExecutions']
            ccy_setting = 'Throttled' if reserved_ccy == 0 else f'{reserved_ccy}'
        else:
            ccy_setting = '非预留账户并发'
        reserved_ccy = fn_ccy_all.get(fn.FunctionName, {}).get('ReservedConcurrentExecutions', -1)
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


if __name__ == '__main__':
    # main()
    main_parallel()
