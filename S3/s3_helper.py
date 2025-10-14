import json
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
        if e.response:
            err_code = e.response.get('Error', {}).get('Code', None)
            if err_code == '404':
                return False
            if err_code == '400':
                logging.error(f'MFA may be expired. {e}')
                raise e
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


def update_bucket_policy_if_needed(env: Env, rgn: str, bucket_name: str, policy: str) -> bool:
    client = get_s3_client(env=env, rgn=rgn)
    try:
        # 获取当前 policy
        resp_get_policy = client.get_bucket_policy(Bucket=bucket_name)

        if 'Policy' not in resp_get_policy:
            logging.error(f'Missing Policy in response. {resp_get_policy}')
            return False
        policy_old = resp_get_policy['Policy']
    except ClientError as e:
        if e.response:
            err_code = e.response.get('Error', {}).get('Code', None)
            if err_code == 'NoSuchBucketPolicy':
                # bucket 没有 policy，需要更新
                logging.info(f'Bucket policy not found. bucket_name={bucket_name}')
                policy_old = None
            else:
                logging.error(f'Other error. bucket_name={bucket_name} error={e} e.response={e.response}')
                raise e
        else:
            logging.error(f'Client Error without response. e={e}')
            raise e

    policy_new = policy.replace(' ', '').replace('\r', '').replace('\n', '').replace('\t', '')
    if policy_old == policy_new:
        return True
    if policy_old:
        # policy 顺序可能不一样，但实际效果一样
        policy_old_dict = json.loads(policy_old)
        policy_new_dict = json.loads(policy_new)
        if policy_old_dict == policy_new_dict:
            return True

    logging.info(f'policy_old={policy_old}')
    logging.info(f'policy_new={policy_new}')

    try:
        resp_put_policy = client.put_bucket_policy(Bucket=bucket_name, Policy=policy_new)
        logging.info(f'Bucket policy updated. bucket_name={bucket_name} resp={resp_put_policy}')
        return True
    except ClientError as e:
        logging.error(f'error={e}')
        raise e
    return False
