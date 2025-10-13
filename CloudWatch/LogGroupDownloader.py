import os
import sys
import logging
from datetime import datetime, timezone
import time
from typing import Optional

SCRIPT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
PROJ_PATH = os.path.dirname(SCRIPT_DIR_PATH)
if PROJ_PATH not in sys.path:
    sys.path.insert(0, PROJ_PATH)

from S3.s3_downloader import download_dir_from_s3  # noqa: E402
from S3.s3_helper import is_bucket_exists  # noqa: E402
from cloud_watch_helper import get_log_client  # noqa: E402
from utils.aws_consts import AllEnvs, Env  # noqa: E402
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
# endregion 配置项


def create_s3_export_bucket_if_not_exists(region: str, env: Env, bucket_name: str):
    policy_str = '''
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "{#service}"
            },
            "Action": "s3:GetBucketAcl",
            "Resource": "{#resource_root}"
        },
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "{#service}"
            },
            "Action": "s3:PutObject",
            "Resource": "{#resource_root}/*",
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "bucket-owner-full-control"
                }
            }
        }
    ]
}
'''
    if region.startswith('cn'):
        service = f'logs.{region}.amazonaws.com.cn'
        resource_root = f'arn:aws-cn:s3:::{bucket_name}'
    else:
        service = f'logs.{region}.amazonaws.com'
        resource_root = f'arn:aws:s3:::{bucket_name}'
    policy_str = policy_str.replace('{#service}', service)
    policy_str = policy_str.replace('{#resource_root}', resource_root)

    if not is_bucket_exists(region=region, env=env, bucket_name=bucket_name):
        logging.info(f'Createing bucket {bucket_name}')
        client = get_log_client(rgn=region, env=env)
        response = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region,
            },
            Policy=policy_str,
        )
        logging.debug(f'response={response}')


def export_log_group_to_s3(
        region: str, env: Env, log_group_name: str, s3_bucket_name: str, s3_prefix: Optional[str] = '',
        start_ts_ms: Optional[int] = None, end_ts_ms: Optional[int] = None
) -> bool:
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
        Tuple[bool, Optional[str], Optional[str]]: 导出任务是否成功, S3 Bucket名称, S3前缀
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
        return False

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
        return False

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
    return True


def main():
    logging.info(f'Start {__file__}')

    s3_bucket_name = f'{S3_BUCKET_NAME_PREFIX}-{REGION}'

    # 创建 S3 Bucket
    create_s3_export_bucket_if_not_exists(region=REGION, env=ENV, bucket_name=s3_bucket_name)

    s3_prefix = S3_PREFIX if S3_PREFIX else [tkn for tkn in LOG_GROUP_NAME.split('/') if tkn][-1]

    # 导出 Log Group 到 S3
    success = export_log_group_to_s3(
        region=REGION,
        env=ENV,
        log_group_name=LOG_GROUP_NAME,
        s3_bucket_name=s3_bucket_name,
        s3_prefix=s3_prefix,
    )
    if not success:
        return

    download_dir_from_s3(
        env=ENV,
        region=REGION,
        bucket_name=s3_bucket_name,
        dir_key=s3_prefix,
        output_path=f'{DATA_DOWNLOAD_DIR}/{s3_prefix}',
    )


if __name__ == '__main__':
    main()
