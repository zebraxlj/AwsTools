import boto3
import boto3.session
from datetime import datetime
from typing import List, Optional

from utils.aws_client_helper import get_aws_profile
from utils.aws_consts import AllEnvs, Env
from utils.proxy_helper import check_proxy


def describe_log_streams_all(aws_region: str, log_group_name: str, stop_event=None):
    log_group_name = get_complete_log_group_name(log_group_name)
    env = get_env_from_log_group_name(log_group_name)
    client = get_log_client(aws_region, env)

    log_stream_all = []
    next_tkn = ''
    while True:
        kwargs = dict()
        if next_tkn:
            kwargs['nextToken'] = next_tkn
        response = client.describe_log_streams(
            logGroupName=log_group_name,
        )
        log_streams, next_tkn = response['logStreams'], response.get('nextToken', '')
        log_stream_all += log_streams

        if not next_tkn:
            break
        if stop_event is not None and stop_event.is_set():
            break
    return log_stream_all


def get_complete_log_group_name(log_group_name: str):
    return log_group_name if log_group_name.startswith('/aws/lambda/') else f'/aws/lambda/{log_group_name}'


def get_env_from_log_group_name(log_group_name: str):
    log_group_name = get_complete_log_group_name(log_group_name)
    return AllEnvs.get_env_by_name(log_group_name.split('/')[-1].split('--')[0])


def get_log_client(rgn: str, env: Env):
    # 配置 proxy
    config = None
    proxy_enable, proxy = check_proxy()
    if proxy_enable:
        from botocore.config import Config
        config = Config(proxies={'http': proxy, 'https': proxy})

    session = boto3.session.Session(region_name=rgn, profile_name=get_aws_profile(rgn, env.is_prod_aws))
    return session.client('logs', config=config)


def filter_log_events(
        aws_region: str, log_group_name: str, pattern: str = '',
        dt_start: Optional[datetime] = None, dt_end: Optional[datetime] = None,
        is_stop_on_match: bool = False,
        stop_event=None,
        ) -> List[dict]:
    """从日志组获取所有

    Args:
        aws_region (str): _description_
        log_group_name (str): _description_
        pattern (str, optional): _description_. Defaults to ''.
        dt_start (datetime, optional): _description_. Defaults to None.
        dt_end (datetime, optional): _description_. Defaults to None.
        is_stop_on_match (bool, optional): _description_. Defaults to False.

    Returns:
        List[dict]: _description_
    """
    log_group_name = log_group_name if log_group_name.startswith('/aws/lambda/') else f'/aws/lambda/{log_group_name}'

    env = AllEnvs.get_env_by_name(log_group_name.split('/')[-1].split('--')[0])
    client = get_log_client(aws_region, env)

    next_tkn = ''
    events_all: List[dict] = []
    while True:
        kwargs = dict()
        if dt_start is not None:
            kwargs['startTime'] = int(dt_start.timestamp() * 1000)
        if dt_end is not None:
            kwargs['endTime'] = int(dt_end.timestamp() * 1000)
        if pattern:
            kwargs['filterPattern'] = pattern
        if next_tkn:
            kwargs['nextToken'] = next_tkn
        response = client.filter_log_events(
            logGroupName=log_group_name,
            **kwargs
        )
        events, next_tkn = response['events'], response.get('nextToken', '')
        # if events:
        #     print(events)
        events_all += events

        if is_stop_on_match and events:
            break
        if not next_tkn:
            break
        if stop_event is not None and stop_event.is_set():
            break

    return events_all
