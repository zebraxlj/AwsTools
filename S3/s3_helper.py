import logging
from typing import Dict

import boto3
from botocore.exceptions import ClientError

from utils.aws_client_helper import get_aws_profile
from utils.aws_consts import Env
from utils.proxy_helper import check_proxy

SESSION_CACHE: Dict[str, boto3.Session] = {}


def get_s3_client(env: Env, rgn: str):
    # 配置 proxy
    config = None
    proxy_enable, proxy = check_proxy()
    if proxy_enable:
        from botocore.config import Config
        config = Config(proxies={'http': proxy, 'https': proxy})

    session = get_session(env=env, rgn=rgn)
    return session.client('s3', config=config)


def get_session(env: Env, rgn: str) -> boto3.Session:
    key = f"{rgn}:{env.is_prod_aws}"
    if key not in SESSION_CACHE:
        SESSION_CACHE[key] = boto3.Session(region_name=rgn, profile_name=get_aws_profile(rgn, env.is_prod_aws))
    return SESSION_CACHE[key]


def is_bucket_exists(env: Env, region: str, bucket_name: str) -> bool:
    client = get_s3_client(rgn=region, env=env)
    try:
        response = client.head_bucket(
            Bucket=bucket_name,
        )
        logging.debug(f'response={response}')
        return True
    except ClientError as e:
        logging.debug(f'error={e}')
        if e.response and e.response.get('Error', {}).get('Code', None) == '404':
            return False
        raise e


def create_bucket_if_not_exists(env: Env, region: str, bucket_name: str):
    if not is_bucket_exists(region=region, env=env, bucket_name=bucket_name):
        logging.info(f'Createing bucket {bucket_name}')
        client = get_s3_client(rgn=region, env=env)
        response = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region,
            }
        )
        logging.debug(f'response={response}')
