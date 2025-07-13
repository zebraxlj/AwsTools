import argparse
import functools
import multiprocessing
import os
import platform
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing.synchronize import Event
from typing import Optional, List, Dict, ClassVar

import boto3
import boto3.session
import botocore.exceptions
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from dataclasses_json import DataClassJsonMixin, dataclass_json

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from utils.aws_client_error_handler import handle_expired_token_exception, print_err  # noqa: E402
from utils.aws_client_helper import get_aws_profile  # noqa: E402
from utils.aws_consts import AllEnvs, REGION_ABBR, REGION_TO_ABBR  # noqa: E402
from utils.aws_urls import get_fleet_address  # noqa: E402
from utils.TablePrinter.table_printer import (  # noqa: E402
    BaseRow, BaseTable, ColumnConfig, CondFmtExactMatch, CondFmtContain
)
from utils.TablePrinter.table_printer_consts import BoxDrawingChar  # noqa: E402

# region 配置项
# ENV, SUB_ENV = AllEnvs.NemoTestComedy, ''
ENV, SUB_ENV = AllEnvs.PartyAnimals, '141270'
REGIONS = [  # 需要拉取的地区
    'cn-northwest-1',
    'ap-northeast-1',
    'eu-central-1',
    'us-east-1',
]
REFRESH_INTERVAL = 60  # 刷新间隔，单位：秒，注意：boto3 gamelift 接口并发有限，如果间隔过短，会导致接口服务过载
ENABLE_TERMINAL_UPDATE = False
# endregion 配置项

if REFRESH_INTERVAL < 30:
    raise Exception('REFRESH_INTERVAL 太小，可能导致 gamelift client 过载')

DT_FMT_M = '%Y-%m-%d %H:%M %z'
DT_FMT_S = '%m-%d %H:%M:%S'


@dataclass_json
@dataclass
class FleetAttribute(DataClassJsonMixin):
    FleetId: str
    FleetType: str
    Name: str
    CreationTime: datetime
    TerminationTime: datetime
    Status: str
    Region: Optional[str] = None


@dataclass_json
@dataclass
class FleetCapacity(DataClassJsonMixin):
    FleetId: str
    InstanceType: str
    InstanceCounts: dict
    Location: str
    LastCheckedDt: Optional[datetime] = None


@dataclass_json
@dataclass
class FleetLocationAttribute(DataClassJsonMixin):
    @dataclass_json
    @dataclass
    class FleetLocationState:
        Location: str = 'Unknown'
        Status: str = 'Unknown'

    LocationState: FleetLocationState = field(default_factory=FleetLocationState)
    StoppedActions: List = field(default_factory=list)


@dataclass_json
@dataclass
class FleetLocationCapacity(DataClassJsonMixin):
    FleetId: str
    InstanceType: str = 'Unknown'
    InstanceCounts: dict = field(default_factory=dict)
    Location: str = 'Unknown'


@dataclass
class EnvFleetStatusRow(BaseRow):
    SubEnv: int = -1
    __SubEnv_config: ClassVar[ColumnConfig] = ColumnConfig(alias='子环境', hide=True)
    Region: str = 'NA'
    __Region_config: ClassVar[ColumnConfig] = ColumnConfig(alias='地区')
    Name: str = field(default_factory=lambda: f'{ENV.name} {SUB_ENV} 无战斗服')
    __Name_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='Fleet名', conditional_format=CondFmtContain(contain_target='AWS MFA Expired')
    )
    CreateTime: str = 'NA'
    __CreateTime_config: ClassVar[ColumnConfig] = ColumnConfig(alias='Fleet创建时间')
    Status: str = 'NA'
    __Status_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='Fleet状态', conditional_format=CondFmtExactMatch(match_target='ERROR')
    )
    FleetType: str = 'NA'
    __FleetType_config: ClassVar[ColumnConfig] = ColumnConfig(alias='机群类型')
    InstanceType: str = 'NA'
    Minimum: int = -1
    __Minimum_config: ClassVar[ColumnConfig] = ColumnConfig(alias='Min')
    Maximum: int = -1
    __Maximum_config: ClassVar[ColumnConfig] = ColumnConfig(alias='Max')
    Desired: int = -1
    __Desired_config: ClassVar[ColumnConfig] = ColumnConfig(alias='所需')
    Pending: int = -1
    __Pending_config: ClassVar[ColumnConfig] = ColumnConfig(hide=True)
    Active: int = -1
    __Active_config: ClassVar[ColumnConfig] = ColumnConfig(alias='活跃')
    Idle: int = -1
    __Idle_config: ClassVar[ColumnConfig] = ColumnConfig(alias='空闲')
    Terminating: int = -1
    __Terminating_config: ClassVar[ColumnConfig] = ColumnConfig(hide=True)
    InstanceLocation: str = 'NA'
    __InstanceLocation_config: ClassVar[ColumnConfig] = ColumnConfig(alias='实例地区')
    LocationStatus: str = 'NA'
    __LocationStatus_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='地区状态', conditional_format=CondFmtExactMatch(match_target='ERROR')
    )
    FleetId: str = 'NA'
    __FleetId_config: ClassVar[ColumnConfig] = ColumnConfig(alias='FleetId(末段)')
    LastCheckedTime: Optional[str] = None  # 自动赋值
    __LastCheckedTime_config: ClassVar[ColumnConfig] = ColumnConfig(hide=True)
    LastCheckedDt: Optional[datetime] = None  # 自动赋值
    __LastCheckedDt_config: ClassVar[ColumnConfig] = ColumnConfig(alias='拉取时间', format='%H:%M:%S')
    Name_href: Optional[str] = None

    def __post_init__(self):
        if self.LastCheckedDt is not None:
            raise ValueError('不要给 EnvFleetStatusRow.LastCheckedDt 手动赋值')
        if self.LastCheckedTime is not None:
            raise ValueError('不要给 EnvFleetStatusRow.LastCheckedTime 手动赋值')
        dt_now = datetime.now()
        self.LastCheckedDt = dt_now
        self.LastCheckedTime = dt_now.strftime(DT_FMT_S)


class EnvFleetStatusTbl(BaseTable):
    row_type = EnvFleetStatusRow


def keyboard_interrupt_handler(func):
    @functools.wraps(func)
    def handle(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pass

    return handle


@keyboard_interrupt_handler
def process_get_fleet_location_status(env, sub_env, region: str, shared_output: dict, stop_event):
    session = boto3.session.Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('gamelift', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    while not stop_event.is_set():
        # 获取所有 Fleets Attributes
        fleets_dict_all = []
        next_token = None
        is_token_expired = False
        while True:
            kwargs = {'NextToken': next_token} if next_token else {}
            try:
                data = client.describe_fleet_attributes(**kwargs)
            except ClientError as e:
                if 'security token included in the request is expired' in e.response['Error']['Message']:
                    handle_expired_token_exception(session)
                    is_token_expired = True
                else:
                    print(e, ''.join(list(reversed(traceback.format_tb(e.__traceback__)))), sep='\n')
                break
            except Exception as e:
                print(e, ''.join(list(reversed(traceback.format_tb(e.__traceback__)))), sep='\n')
                time.sleep(15)
                continue
            # 返回结构： https://boto3.amazonaws.com/v1/documentation/api/1.14.25/reference/services/gamelift.html#GameLift.Client.describe_fleet_attributes # noqa
            if 'FleetAttributes' not in data:
                raise Exception('Missing Key(FleetAttributes) in boto3 describe_fleet_attributes resp')

            fleets_dict_all += data['FleetAttributes']
            next_token = data.get('NextToken', '')
            if not next_token:
                break

        # 筛选 Fleets Attributes
        sub_env = sub_env if sub_env else ''
        env_fleets_info_dict: Dict[str, FleetAttribute] = {
            f['FleetId']: FleetAttribute.from_dict(f) for f in fleets_dict_all
            if f['Name'].startswith(f'{env.name}--{sub_env}')
        }
        for k, v in env_fleets_info_dict.items():
            env_fleets_info_dict[k].Region = region

        output_key_na = f'{region}:NA:NA'
        if not env_fleets_info_dict:
            kwargs = {}
            row_kwargs = {}
            if is_token_expired:
                kwargs['Name'] = 'AWS MFA Expired'
                row_kwargs['Name'] = 'AWS MFA Expired'
            shared_output[output_key_na] = EnvFleetStatusRow(SubEnv=int(sub_env), Region=region, **row_kwargs)
            time.sleep(REFRESH_INTERVAL)
            continue

        if output_key_na in shared_output:
            shared_output.pop(output_key_na)

        env_fleets_location_attributes_dict: Dict[str, List[FleetLocationAttribute]] = {}
        env_fleets_location_capacity_dict: Dict[str, List[FleetLocationCapacity]] = {}
        env_fleets_capacity_dict: Dict[str, List[FleetCapacity]] = {}

        support_multi_location = True
        # 支持 multi location 的地区
        for fleet_id in env_fleets_info_dict.keys():
            # 支持 multi location 的地区 - 获取 Fleet Location Attribute
            next_token = None
            fleets_location_attrs: List[FleetLocationAttribute] = []
            while True and support_multi_location:
                try:
                    kwargs = {'NextToken': next_token} if next_token else {}
                    data = client.describe_fleet_location_attributes(FleetId=fleet_id, **kwargs)
                    # print(f'describe_fleet_location_attributes: {data}')
                except botocore.exceptions.ClientError as error:
                    if error.response['Error']['Code'] == 'UnsupportedRegionException':
                        support_multi_location = False
                        break
                    else:
                        raise error
                except Exception as e:
                    print(e)
                    print(''.join(list(reversed(traceback.format_tb(e.__traceback__)))))
                    time.sleep(15)
                    continue
                if 'LocationAttributes' not in data:
                    raise Exception('Missing Key(LocationAttributes) in boto3 describe_fleet_location_attributes resp')

                fleets_location_attrs += [FleetLocationAttribute.from_dict(d) for d in data['LocationAttributes']]
                next_token = data.get('NextToken', '')
                if not next_token:
                    break
            env_fleets_location_attributes_dict[fleet_id] = fleets_location_attrs

            # 支持 multi location 的地区 - 获取 Fleet Location Capacity
            fleets_location_capacity: List[FleetLocationCapacity] = []
            if support_multi_location:
                for attr in fleets_location_attrs:
                    try:
                        data = client.describe_fleet_location_capacity(FleetId=fleet_id,
                                                                       Location=attr.LocationState.Location)
                        # print(f'describe_fleet_location_capacity: {data}')
                    except Exception as e:
                        print(e)
                        print(''.join(list(reversed(traceback.format_tb(e.__traceback__)))))
                        continue

                    if 'FleetCapacity' not in data:
                        raise Exception('Missing Key(FleetCapacity) in boto3 describe_fleet_location_capacity resp')
                    fleets_location_capacity.append(FleetLocationCapacity.from_dict(data['FleetCapacity']))
                env_fleets_location_capacity_dict[fleet_id] = fleets_location_capacity

            # 支持 multi location 的地区 - 准备输出
            if support_multi_location:
                locations = set([attr.LocationState.Location for attr in fleets_location_attrs])
                locations = locations.union([elem.Location for elem in fleets_location_capacity])
                for location in locations:
                    tmp_attrs_list: List[FleetLocationAttribute] = [
                        elem for elem in fleets_location_attrs if elem.LocationState.Location == location
                    ]
                    tmp_capacity_list: List[FleetLocationCapacity] = [
                        elem for elem in fleets_location_capacity if elem.Location == location
                    ]
                    fleet_location_attrs: FleetLocationAttribute = (
                        tmp_attrs_list[0] if tmp_attrs_list
                        else FleetLocationAttribute()
                    )
                    fleet_location_capacity: FleetLocationCapacity = (
                        tmp_capacity_list[0] if tmp_capacity_list
                        else FleetLocationCapacity(FleetId=fleet_id, Location=location)
                    )

                    shared_output[f'{region}:{fleet_id}:{location}'] = EnvFleetStatusRow(
                        Region=region,
                        SubEnv=int(env_fleets_info_dict[fleet_id].Name.split('--')[-1].split('-')[0]),
                        FleetId=fleet_id,
                        FleetType=env_fleets_info_dict[fleet_id].FleetType,
                        Name=env_fleets_info_dict[fleet_id].Name,
                        Name_href=get_fleet_address(region, fleet_id),
                        CreateTime=env_fleets_info_dict[fleet_id].CreationTime.strftime(DT_FMT_M)[:-2],
                        Status=env_fleets_info_dict[fleet_id].Status,
                        InstanceType=fleet_location_capacity.InstanceType,
                        Desired=fleet_location_capacity.InstanceCounts.get('DESIRED', -1),
                        Minimum=fleet_location_capacity.InstanceCounts.get('MINIMUM', -1),
                        Maximum=fleet_location_capacity.InstanceCounts.get('MAXIMUM', -1),
                        Pending=fleet_location_capacity.InstanceCounts.get('PENDING', -1),
                        Active=fleet_location_capacity.InstanceCounts.get('ACTIVE', -1),
                        Idle=fleet_location_capacity.InstanceCounts.get('IDLE', -1),
                        Terminating=fleet_location_capacity.InstanceCounts.get('TERMINATING', -1),
                        InstanceLocation=location,
                        LocationStatus=fleet_location_attrs.LocationState.Status,
                    )
            else:

                # 不支持 multi location 的地区 - 获取 Fleet Capacity
                env_fleets_capacity_dict_all = []
                next_token = None
                # 获取所有 fleet capacity
                while True:
                    fleet_ids = [f_id for f_id in env_fleets_info_dict.keys()]
                    if not fleet_ids:
                        break
                    kwargs = {'NextToken': next_token} if next_token else {}
                    try:
                        data = client.describe_fleet_capacity(FleetIds=fleet_ids, **kwargs)
                        # print(f'describe_fleet_capacity: {data}')
                    except Exception as e:
                        print(e)
                        print(''.join(list(reversed(traceback.format_tb(e.__traceback__)))))
                        time.sleep(10)
                        continue
                    # 返回结构：https://boto3.amazonaws.com/v1/documentation/api/1.14.25/reference/services/gamelift.html#GameLift.Client.describe_fleet_capacity
                    if 'FleetCapacity' not in data:
                        raise Exception('Missing Key(FleetCapacity) in boto3 describe_fleet_capacity resp')

                    # print(data['FleetCapacity'])
                    env_fleets_capacity_dict_all += data['FleetCapacity']
                    next_token = data.get('NextToken', '')
                    if not next_token:
                        break

                for fc in env_fleets_capacity_dict_all:
                    fleet_capacity = FleetCapacity.from_dict(fc)
                    fleet_capacity.LastCheckedDt = datetime.now()
                    if fleet_capacity.FleetId in env_fleets_capacity_dict:
                        env_fleets_capacity_dict[fleet_capacity.FleetId].append(fleet_capacity)
                    else:
                        env_fleets_capacity_dict[fleet_capacity.FleetId] = [fleet_capacity]

                for fleet_id, fleet_attr in env_fleets_info_dict.items():
                    # print('fleet_attr:', fleet_attr.to_dict())

                    fleet_capacities: List[FleetCapacity] = env_fleets_capacity_dict.get(fleet_id, [])
                    for fleet_capacity in fleet_capacities:
                        if fleet_capacity is None:
                            continue
                        # print('\tfleet_capacity:', fleet_capacity.to_dict())
                        shared_output[f'{region}:{fleet_id}:{fleet_capacity.Location}'] = EnvFleetStatusRow(
                            Region=region,
                            SubEnv=int(fleet_attr.Name.split('--')[-1].split('-')[0]),
                            FleetId=fleet_attr.FleetId,
                            FleetType=fleet_attr.FleetType,
                            Name=fleet_attr.Name,
                            Name_href=get_fleet_address(region, fleet_id),
                            CreateTime=fleet_attr.CreationTime.strftime(DT_FMT_M)[:-2],
                            Status=fleet_attr.Status,
                            InstanceType=fleet_capacity.InstanceType,
                            Desired=fleet_capacity.InstanceCounts.get('DESIRED', -1),
                            Minimum=fleet_capacity.InstanceCounts.get('MINIMUM', -1),
                            Maximum=fleet_capacity.InstanceCounts.get('MAXIMUM', -1),
                            Pending=fleet_capacity.InstanceCounts.get('PENDING', -1),
                            Active=fleet_capacity.InstanceCounts.get('ACTIVE', -1),
                            Idle=fleet_capacity.InstanceCounts.get('IDLE', -1),
                            Terminating=fleet_capacity.InstanceCounts.get('TERMINATING', -1),
                            InstanceLocation=fleet_capacity.Location,
                            LocationStatus='不支持',
                        )

        time.sleep(REFRESH_INTERVAL)


def __mask_fleet_id(fleet_id: str) -> str:
    tokens = fleet_id.split('-')
    if len(tokens) < 2:
        return fleet_id
    return f'***-{tokens[-1]}'


@keyboard_interrupt_handler
def process_print_fleet_status(shared_output: Dict[str, EnvFleetStatusRow], stop_event: Event):
    last_update_dt: datetime = datetime(2024, 1, 1)
    while not stop_event.is_set():
        if any(v.LastCheckedDt > last_update_dt for v in shared_output.values() if v.LastCheckedDt is not None):
            table = EnvFleetStatusTbl()
            for k, v in shared_output.items():
                v.Region = REGION_TO_ABBR.get(v.Region, v.Region)
                v.FleetId = __mask_fleet_id(v.FleetId)
                v.FleetType = '按需OD' if v.FleetType == 'ON_DEMAND' else v.FleetType
                table.insert_row(v)
                last_update_dt = max(last_update_dt, v.LastCheckedDt) if v.LastCheckedDt is not None else last_update_dt

            # 准备输出数据：表头行、表头分割行
            lines = [table.get_table_header_str(), table.get_table_header_sep_str()]
            # 准备输出数据：表行排序
            rows_sorted: List[EnvFleetStatusRow] = table.get_sorted_rows(
                order_by=['SubEnv', 'Region', 'Name', 'InstanceType', 'Status', 'InstanceLocation'],
                ascending=[False, True, False, True, True, True],
            )
            # 准备输出数据：额外排序逻辑
            rows_sorted.sort(key=lambda _row: REGIONS.index(str(REGION_ABBR[_row.Region])))

            # 准备输出数据：数据行、数据分割行
            row_prev: Optional[EnvFleetStatusRow] = None
            for row in rows_sorted:
                # If you don't know what you are doing, it's recommended to add the separator regarding to the sorting order. # noqa
                # Otherwise, you may see same column value being separated into different chunks and the output looks weird. # noqa
                row: EnvFleetStatusRow
                if row_prev is not None and row_prev.Region != row.Region:
                    lines.append(table.get_table_line_sep_str(
                        sep_h=BoxDrawingChar.DOUBLE_HORIZONTAL,
                        sep_v=BoxDrawingChar.VERTICAL_SINGLE_AND_HORIZONTAL_DOUBLE,
                    ))
                elif row_prev is not None and row_prev.InstanceType != row.InstanceType:
                    lines.append(table.get_table_line_sep_str(
                        sep_h=BoxDrawingChar.LIGHT_HORIZONTAL, sep_v=BoxDrawingChar.LIGHT_VERTICAL, dense=False
                    ))
                lines.append(table.get_table_line_str(row))
                row_prev = row

            # 输出表单
            if ENABLE_TERMINAL_UPDATE:
                pass
            else:
                sys_type = platform.system()
                if sys_type == 'Windows':
                    os.system('cls')
                else:
                    os.system('clear')
                for line in lines:
                    print(line)
                print('\n', 'Ctrl+c 停止获取', sep='')

        time.sleep(3)


def fetch_fleet_status():
    stop_event: Event = multiprocessing.Event()

    with multiprocessing.Manager() as manager:
        shared_dict = manager.dict()

        processes = []
        for rgn in REGIONS:
            process = multiprocessing.Process(
                target=process_get_fleet_location_status, args=(ENV, SUB_ENV, rgn, shared_dict, stop_event,))
            processes.append(process)
            process.start()

        process_print = multiprocessing.Process(
            target=process_print_fleet_status, args=(shared_dict, stop_event,))
        process_print.start()

        try:
            time.sleep(20 * 60)  # 默认最长运行 20 分钟
        except KeyboardInterrupt:
            print('Keyboard Interrupted')
        finally:
            stop_event.set()
            process_print.join()
            for process in processes:
                process.join()
        print("All Processes completed.")


def parse_args(args: List[str]):
    parser = argparse.ArgumentParser(
        description='获取特定环境、子环境、地区的 fleet 状态',
        epilog='用例：python env_fleet_status_fetcher.py -prod -sen 141270'
    )
    parser.add_argument('--environment-name', '-en',
                        help='环境名',
                        default='')
    parser.add_argument('--sub-environment-name', '-sen',
                        help='子环境',
                        default='')
    parser.add_argument('--regions', '-rgn',
                        help=(
                            'fleet 地区。允许: cn-north-1, BJ, cn-northwest-1, NX, ap-northeast-1, AP, '
                            'eu-central-1, EU, us-east-1, US'
                        ),
                        nargs='+',
                        default=None)
    parser.add_argument('-prod',
                        help=(
                            '简写：直接获取 PartyAnimals 生产环境 fleet 情况。'
                            '效果等同于 -en PartyAnimals, 会忽略 --environment-name 输入'
                        ),
                        action='store_true',
                        default=False)
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])

    arg_is_prod = args.prod
    arg_env_name = args.environment_name
    arg_sub_env = args.sub_environment_name
    arg_regions: list[str] = args.regions if args.regions else []

    global ENV, SUB_ENV, REGIONS

    if arg_is_prod:
        if arg_env_name:
            print(f'同时输入 -prod 和 -en，将获取 PA 生产 Fleet 情况，忽略 -en={arg_env_name}')
        ENV = AllEnvs.PartyAnimals
    elif arg_env_name:
        ENV = AllEnvs.get_env_by_name(arg_env_name)
        if not ENV:
            print_err(f'环境不存在： -en={arg_env_name}')
            sys.exit(1)

    SUB_ENV = str(arg_sub_env) if arg_sub_env else SUB_ENV

    arg_regions = sorted(REGION_ABBR.get(r, r) for r in arg_regions)
    bad_regions = [r for r in arg_regions if r not in REGION_ABBR.values()]
    if bad_regions:
        print_err(f'地区不支持：{bad_regions}')
        sys.exit(1)

    REGIONS = arg_regions if arg_regions else REGIONS

    fetch_fleet_status()


if __name__ == '__main__':
    main()
