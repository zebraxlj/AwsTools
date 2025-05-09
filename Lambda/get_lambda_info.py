import json
import multiprocessing
import os
import string
import sys
from dataclasses import dataclass, fields
from datetime import datetime
from typing import List, ClassVar, Dict

import boto3
import boto3.session
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

CURR_FOLDER_NAME = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from utils.aws_client_error_handler import handle_expired_token_exception
from utils.aws_client_helper import get_aws_profile
from utils.aws_consts import AllEnvs, Env
from utils.SystemTools.file_system_helper import create_dir_if_not_exists
from utils.TablePrinter.table_printer import BaseTable, BaseRow, ColumnConfig, ColumnAlignment, CondFmtExactMatch

# region 配置项
ENV, SUB_ENV = AllEnvs.NemoDevMaprefine, '76700'
# ENV, SUB_ENV = AllEnvs.NemoDevTrunk, '47607'
# ENV, SUB_ENV = AllEnvs.PartyAnimals, ''
REGIONS = [
    # 'cn-northwest-1',
    # 'ap-northeast-1',
    'eu-central-1',
    # 'us-east-1',
]
# endregion 配置项

OUTPUT_DIR = f'./{CURR_FOLDER_NAME}/Data/output'


@dataclass
class Function:
    """
    用于解析 list_functions 返回的 Functions 字段，返回结构见文档：
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/list_functions.html
    """
    FunctionName: str
    FunctionArn: str
    Runtime: str = None
    Role: str = None
    Handler: str = None
    CodeSize: int = None
    Description: str = None
    Timeout: int = None
    MemorySize: int = None
    LastModified: str = None
    CodeSha256: str = None
    Version: str = None
    Environment: dict = None
    Layers: dict = None
    PackageType: str = None
    Architectures: list = None
    EphemeralStorage: dict = None

    @classmethod
    def from_dict(cls, dict_data: dict):
        valid_fields = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in dict_data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class FunctionRow(BaseRow):
    FunctionName: str = 'NA'
    __FunctionName_config: ClassVar[ColumnConfig] = ColumnConfig(align=ColumnAlignment.LEFT)
    FunctionName_href: str = 'NA'
    Region: str = 'NA'
    __Region_config: ClassVar[ColumnConfig] = ColumnConfig(alias='地区')
    Timeout: int = None
    __Timeout_config: ClassVar[ColumnConfig] = ColumnConfig(alias='超时')
    MemorySize: int = None
    __MemorySize_config: ClassVar[ColumnConfig] = ColumnConfig(alias='内存')
    CurrencySetting: str = 'NA'
    __CurrencySetting_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='并发设置', conditional_format=CondFmtExactMatch(match_target='Throttled')
    )
    __Throttled_config: ClassVar[ColumnConfig] = ColumnConfig(conditional_format=CondFmtExactMatch(match_target=True))
    LastDeployDt: str = 'NA'
    __LastDeployDt_config: ClassVar[ColumnConfig] = ColumnConfig(alias='最后部署时间')


class FunctionTable(BaseTable):
    row_type = FunctionRow


def get_env_rgn_functions(env: Env, region: str) -> List[dict]:
    session = boto3.session.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
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


def get_functions_currency(env: Env, region: str, function_names: List[str]) -> Dict[str, dict]:
    session = boto3.session.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('lambda', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    ret = {}
    for function_name in function_names[:]:
        resp = client.get_function_concurrency(FunctionName=function_name)
        ret[function_name] = resp
    return ret


def get_functions_currency_worker(
        env: Env, region: str, function_name: str, shared_dict: dict, stop_event
):
    session = boto3.session.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('lambda', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))
    while not stop_event.is_set():
        try:
            resp = client.get_function_concurrency(FunctionName=function_name)
            shared_dict[function_name] = resp
            return
        except ClientError as e:
            print(f'Exception: {e.__dict__}')
            return


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


def get_lambda_url(region: str, function_name):
    region = region
    if 'cn' in region:
        # https://cn-northwest-1.console.amazonaws.cn/lambda/home?region=cn-northwest-1#/functions/PartyAnimals-FeishuNotifier
        fn_url = f'https://{region}.console.amazonaws.cn/lambda/home?region={region}#/functions/{function_name}'
    else:
        # https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/PartyAnimals-EventTrackingFunction
        fn_url = f'https://{region}.console.aws.amazon.com/lambda/home?region={region}#/functions/{function_name}'
    return fn_url


def main():
    fn_all: List[Function] = []
    fn_ccy_all: Dict = {}
    table = FunctionTable()
    for rgn in REGIONS:
        file_name_functions = f'{"prod" if ENV.is_prod_aws else "dev"}_{rgn}_Functions.json'

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
                target=get_functions_currency_worker, args=(ENV, rgn, fn.FunctionName, shared_dict, stop_event)
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
        last_modified_dt = datetime.strptime(fn.LastModified, '%Y-%m-%dT%H:%M:%S.%f%z')
        table.insert_row(FunctionRow(
            FunctionName=fn.FunctionName,
            Region=fn_rgn,
            Timeout=fn.Timeout,
            MemorySize=fn.MemorySize,
            CurrencySetting=ccy_setting,
            LastDeployDt=datetime.strftime(last_modified_dt, '%Y-%m-%d %H:%M %z')[:-2],
            FunctionName_href=get_lambda_url(fn_rgn, fn.FunctionName),
        ))
    table.print_table(order_by=['FunctionName', 'Region'])


if __name__ == '__main__':
    main()
