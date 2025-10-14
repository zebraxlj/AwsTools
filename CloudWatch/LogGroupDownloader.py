import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
PROJ_PATH = os.path.dirname(SCRIPT_DIR_PATH)
if PROJ_PATH not in sys.path:
    sys.path.insert(0, PROJ_PATH)

from S3.s3_downloader import download_dir_from_s3  # noqa: E402
from S3.s3_helper import get_s3_client, is_bucket_exists, update_bucket_policy_if_needed  # noqa: E402
from cloud_watch_helper import get_log_client  # noqa: E402
from utils.aws_consts import ACCT_DEV_CN, ACCT_DEV_US, ACCT_PROD_CN, ACCT_PROD_US, AllEnvs, Env  # noqa: E402
from utils.aws_urls import get_s3_bucket_url  # noqa: E402
from utils.logging_helper import setup_logging  # noqa: E402

setup_logging()

# region 配置项
# ENV = AllEnvs.PartyAnimals
ENV = AllEnvs.Standalone
REGION = 'cn-northwest-1'

LOG_GROUP_NAME = '/aws/lambda/Audit2022-ConfigCenterFunction'
S3_BUCKET_NAME_PREFIX = 'lambda-log-export'
S3_PREFIX = ''
DATA_DOWNLOAD_DIR = os.path.join(SCRIPT_DIR_PATH, 'Data/LogGroup')
START_TS_MS = 0
END_TS_MS = 0
# endregion 配置项


@dataclass
class __CmdArgs:
    environment_name: str
    region: str
    log_group_name: str
    s3_bucket_name_prefix: Optional[str]
    s3_prefix: Optional[str]
    start_ts_ms: Optional[int]
    end_ts_ms: Optional[int]


def __parse_args(args: List[str]) -> __CmdArgs:
    parser = argparse.ArgumentParser(
        description='Download CloudWatch Log Group to S3'
    )
    parser.add_argument(
        '--environment_name', '-en',
        help='环境名',
        default='',
        required=True,
    )
    parser.add_argument(
        '--region', '-rgn',
        help='cloudwatch 地区',
        default='',
        required=True
    )
    parser.add_argument(
        '--log_group_name', '-lg',
        help='CloudWatch Log Group名称',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--s3_bucket_name_prefix', '-bp',
        help=(
            'S3 Bucket 前缀。因为 bucket 必须和 cloudwatch 地区相同，bucket 名又必须唯一，'
            '实际 bucket 名会是 <s3_bucket_name_prefix>-<region>。例：lambda-log-export-cn-northwest-1'
        ),
        type=str,
        default=None,
    )
    parser.add_argument(
        '--s3_prefix', '-sp',
        type=str,
        default='',
        help='S3 保存路径前缀'
    )
    parser.add_argument(
        '--start_ts_ms', '-st',
        type=int,
        default=None, required=False,
        help='开始时间戳（毫秒）'
    )
    parser.add_argument(
        '--end_ts_ms', '-et',
        type=int,
        default=None, required=False,
        help='结束时间戳（毫秒）'
    )
    parsed_args = parser.parse_args(args)
    return __CmdArgs(**vars(parsed_args))


def create_s3_export_bucket_if_not_exists(env: Env, region: str, bucket_name: str):
    policy_str = '''
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowGetBucketAclForLogsService",
            "Effect": "Allow",
            "Principal": {
                "Service": "{#service}"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "{#resource_root}"
        },
        {
            "Sid": "AllowPutObjectForLogsService",
            "Effect": "Allow",
            "Principal": {
                "Service": "{#service}"
            },
            "Action": "s3:PutObject",
            "Resource": "{#resource_root}/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control",
                    "aws:SourceAccount": "{#aws_acct_id}"
                },
                "ArnLike": {
                    "aws:SourceArn": "{#source_arn}"
                }
            }
        }
    ]
}
'''
    if region.startswith('cn'):
        service = f'logs.{region}.amazonaws.com.cn'
        resource_root = f'arn:aws-cn:s3:::{bucket_name}'
        aws_acct_id = ACCT_PROD_CN if env.is_prod_aws else ACCT_DEV_CN
        source_arn = f'arn:aws-cn:logs:{region}:{aws_acct_id}:*'
    else:
        service = f'logs.{region}.amazonaws.com'
        resource_root = f'arn:aws:s3:::{bucket_name}'
        aws_acct_id = ACCT_PROD_US if env.is_prod_aws else ACCT_DEV_US
        source_arn = f'arn:aws:logs:{region}:{aws_acct_id}:*'

    policy_str = policy_str.replace('{#service}', service)
    policy_str = policy_str.replace('{#resource_root}', resource_root)
    policy_str = policy_str.replace('{#aws_acct_id}', aws_acct_id)
    policy_str = policy_str.replace('{#source_arn}', source_arn)
    # 检查 policy 是否有未替换的占位符
    if '"{#' in policy_str:
        logging.error(f'policy_str 有为替换完的占位符：{policy_str}')
        return False

    if not is_bucket_exists(region=region, env=env, bucket_name=bucket_name):
        logging.info(f'Createing bucket {bucket_name}')
        client = get_s3_client(rgn=region, env=env)
        resp_create_bucket = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region,
            },
        )
        logging.info(f'Bucket created. response={resp_create_bucket}')

    success = update_bucket_policy_if_needed(env=env, rgn=region, bucket_name=bucket_name, policy=policy_str)
    if not success:
        return False

    return True


def export_log_group_to_s3(
        region: str, env: Env, log_group_name: str, s3_bucket_name: str, s3_prefix: Optional[str] = '',
        start_ts_ms: Optional[int] = None, end_ts_ms: Optional[int] = None
) -> Tuple[bool, str]:
    """导出CloudWatch Log Group到S3

    Args:
        region (str): AWS region
        env (Env): environment
        log_group_name (str): CloudWatch Log Group名称
        s3_bucket_name (str): S3 Bucket名称
        s3_prefix (str, optional): S3前缀. Defaults to None.
        start_ts_ms (int, optional): 开始时间戳（毫秒）Defaults to None.
        end_ts_ms (int, optional): 结束时间戳（毫秒）Defaults to None.
    Returns:
        Tuple[bool, str]: 导出任务是否成功, export task_id
    """
    logging.info(
        f'Start log_group={log_group_name} s3_bucket={s3_bucket_name} s3_prefix={s3_prefix} '
        f'start_ts_ms={start_ts_ms} end_ts_ms={end_ts_ms}'
    )

    client = get_log_client(rgn=region, env=env)

    # 获取 log group 信息
    resp_desc = client.describe_log_groups(logGroupNamePrefix=log_group_name)
    logging.debug(f'resp_desc={resp_desc}')
    # 检查 log group 是否存在
    log_groups = resp_desc.get('logGroups', None)
    log_group = None
    if log_groups:
        for lg in resp_desc['logGroups']:
            if lg.get('logGroupName', None) == log_group_name:
                log_group = lg
                break
    if not log_group:
        logging.error(f'Log group not found. log_group_name={log_group_name} region={region}')
        return False, ''

    # 建导出任务
    ts_ms_now = int(datetime.now(timezone.utc).timestamp() * 1000)
    task_name = f'{log_group_name}-{ts_ms_now}'
    if start_ts_ms is None:
        start_ts_ms = int(log_group['creationTime'])
    if end_ts_ms is None:
        end_ts_ms = ts_ms_now
    if s3_prefix is None:
        s3_prefix = ''

    resp_create_task = client.create_export_task(
        taskName=task_name,
        logGroupName=log_group_name,
        fromTime=start_ts_ms,
        to=end_ts_ms,
        destination=s3_bucket_name,
        destinationPrefix=s3_prefix,
    )

    # 检查导出任务是否创建成功
    task_id = resp_create_task.get('taskId', None)
    if not task_id:
        logging.error(f'Create export task failed. resp_create_task={resp_create_task}')
        return False, ''

    # 等待导出任务完成
    logging.debug(f'response={resp_create_task}, s3_bucket_url={get_s3_bucket_url(region, s3_bucket_name)}')

    while True:
        resp_desc_task = client.describe_export_tasks(taskId=task_id)
        logging.debug(f'resp_desc_task={resp_desc_task}')
        task_status = resp_desc_task['exportTasks'][0].get('status', {}).get('code', None)
        if task_status not in ('PENDING', 'RUNNING'):
            execution_info = resp_desc_task['exportTasks'][0].get('executionInfo', {})
            span = execution_info.get('completionTime', -1) - execution_info.get('creationTime', 0)
            logging.info(f'Export task completed. task_id={task_id} task_status={task_status} span={span}ms')
            break
        logging.debug(f'Export task {task_id} is {task_status}')
        time.sleep(5)
    return True, task_id


def main():
    logging.info(f'Start {__file__}')
    global LOG_GROUP_NAME, S3_PREFIX, START_TS_MS, END_TS_MS

    sys_argv: list = sys.argv[1:]
    # sys_argv_str = f'-en {ENV.name} -rgn {REGION} -lg {LOG_GROUP_NAME} -bp {S3_BUCKET_NAME_PREFIX} -sp {S3_PREFIX}'
    # sys_argv = sys_argv_str.split(' ')
    cmd_args: __CmdArgs = __parse_args(sys_argv)

    LOG_GROUP_NAME = cmd_args.log_group_name
    S3_PREFIX = cmd_args.s3_prefix
    START_TS_MS = cmd_args.start_ts_ms
    END_TS_MS = cmd_args.end_ts_ms

    s3_bucket_name = f'{S3_BUCKET_NAME_PREFIX}-{REGION}'

    # 创建 S3 Bucket
    success = create_s3_export_bucket_if_not_exists(env=ENV, region=REGION, bucket_name=s3_bucket_name)
    if not success:
        return

    s3_prefix = S3_PREFIX if S3_PREFIX else [tkn for tkn in LOG_GROUP_NAME.split('/') if tkn][-1]

    # 导出 Log Group 到 S3
    success, task_id = export_log_group_to_s3(
        region=REGION,
        env=ENV,
        log_group_name=LOG_GROUP_NAME,
        s3_bucket_name=s3_bucket_name,
        s3_prefix=s3_prefix,
        start_ts_ms=START_TS_MS,
        end_ts_ms=END_TS_MS,
    )
    if not success:
        return

    dir_key = f'{s3_prefix}/{task_id}'
    download_dir_from_s3(
        env=ENV,
        region=REGION,
        bucket_name=s3_bucket_name,
        dir_key=dir_key,
        output_path=f'{DATA_DOWNLOAD_DIR}/{dir_key}',
    )


if __name__ == '__main__':
    main()
