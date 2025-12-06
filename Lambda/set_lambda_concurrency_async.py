import argparse
import asyncio
import os
import sys
from enum import Enum, auto
from typing import Awaitable, List, cast

from botocore.client import Config as BotoConfig

CURR_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
PROJ_ROOT_PATH = os.path.dirname(CURR_DIR_PATH)
if PROJ_ROOT_PATH not in sys.path:
    sys.path.insert(0, PROJ_ROOT_PATH)

from utils.aws_aiosession_helper import get_cached_aiosession  # noqa: E402
from utils.aws_consts import REGION_ABBR  # noqa: E402


class CcyTypeEnum(Enum):
    UNKNOWN = '未知'
    RESERVE = '预留并发'
    UNRESERVE = '账户并发'


class EnvTypeEnum(Enum):
    UNKNOWN = auto()
    DEV = auto()
    PROD = auto()
    UAT = auto()


def parse_args(args: List[str]):
    parser = argparse.ArgumentParser(
        description='Set Lambda Concurrency Async',
        epilog='用例: python -fn  -rgn NX AP',
    )
    parser.add_argument(
        '--function-names', '-fn',
        required=True,
        nargs='+',
        help='Lambda function name',
    )
    parser.add_argument(
        '--regions', '-rgn',
        help=(
            'fleet 地区。允许: cn-north-1, BJ, cn-northwest-1, NX, ap-northeast-1, AP, '
            'eu-central-1, EU, us-east-1, US'
        ),
        nargs='+',
        required=True,
    )

    ccy_group = parser.add_mutually_exclusive_group(required=True)
    ccy_group.add_argument(
        '-reset',
        action='store_true',
        help='是否设置 Concurrency 为 unreserved account concurrency',
        default=None,
    )
    ccy_group.add_argument(
        '-throttle',
        action='store_true',
        help='是否 Throttle',
        default=None,
    )
    ccy_group.add_argument(
        '--concurrency', '-ccy',
        type=int,
        help='设置 Reserve concurrency 的值，当为 0 时，等同于 -throttle',
        default=None,
    )

    env_group = parser.add_mutually_exclusive_group(required=True)
    env_group.add_argument(
        '-prod',
        action='store_true',
        help='是生产环境',
        default=None,
    )
    env_group.add_argument(
        '-dev',
        action='store_true',
        help='是开发环境',
        default=None,
    )

    return parser.parse_args(args)


async def set_func_concurrency_async(
    function_name: str,
    is_prod: bool,
    region: str,
    concurrency_type: CcyTypeEnum,
    concurrency: int = 0,
) -> None:
    print(f'设置函数 {function_name} 在 {region} 地区')
    session = get_cached_aiosession(region=region, is_prod=is_prod)
    async with session.create_client(
        'lambda',
        region_name=region,
        config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}, max_pool_connections=50)
    ) as client:
        if concurrency_type == CcyTypeEnum.RESERVE:
            # 加 reserve concurrency 限制
            concurrency = 0 if concurrency is None else concurrency
            resp = await cast(Awaitable, client.put_function_concurrency(
                FunctionName=function_name,
                ReservedConcurrentExecutions=concurrency,
            ))
        elif concurrency_type == CcyTypeEnum.UNRESERVE:
            # 删 reserve concurrency 限制
            resp = await cast(Awaitable, client.delete_function_concurrency(
                FunctionName=function_name,
            ))
        print(resp)


async def main_coroutine():
    sys_args = sys.argv[1:]
    if os.environ.get('TERM_PROGRAM', None) == 'vscode':
        # VsCode 本地调试时，默认设置 Standalone-LoginRecFunction 在 NX 地区的并发为 0
        print('VsCode 本地调试')
    args = parse_args(sys_args)

    # region 校验命令行入参
    arg_fn_names = args.function_names

    # 是否是生产环境
    arg_dev = args.dev
    arg_prod = args.prod
    env_type = EnvTypeEnum.DEV if arg_dev else EnvTypeEnum.PROD if arg_prod else EnvTypeEnum.UNKNOWN
    is_prod = env_type == EnvTypeEnum.PROD
    # Lambda 地区
    arg_regions: List[str] = args.regions
    arg_regions = sorted([REGION_ABBR.get(rgn, rgn) for rgn in arg_regions])
    # 并发类型
    arg_reset = args.reset
    arg_throttle = args.throttle
    arg_concurrency = args.concurrency
    ccy_type = (
        CcyTypeEnum.UNRESERVE if arg_reset is not None
        else CcyTypeEnum.RESERVE if arg_throttle is not None or arg_concurrency is not None
        else CcyTypeEnum.UNKNOWN
    )
    # endregion

    print(f'并发类型={ccy_type.value}{f"， 并发数={args.concurrency}" if ccy_type == CcyTypeEnum.RESERVE else ""}')

    # 处理所有 函数-地区 组合
    tasks = []
    for fn_name in arg_fn_names:
        for rgn in arg_regions:
            tasks.append(set_func_concurrency_async(
                    function_name=fn_name,
                    is_prod=is_prod,
                    region=rgn,
                    concurrency_type=ccy_type,
                    concurrency=args.concurrency,
            ))
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main_coroutine())
