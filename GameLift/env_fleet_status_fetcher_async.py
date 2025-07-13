import asyncio
import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from utils.aws_consts import AllEnvs  # noqa: E402

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
# endregion 配置项

if REFRESH_INTERVAL < 30:
    raise Exception('REFRESH_INTERVAL 太小，可能导致 gamelift client 过载')


async def main_coroutine():
    pass


if __name__ == '__main__':
    ret = asyncio.run(main_coroutine())
    if ret:
        print(f'Error: {ret}')
