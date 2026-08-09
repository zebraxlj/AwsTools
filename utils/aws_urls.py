from typing import Optional
from urllib import parse


def gen_cloud_watch_log_stream_url(log_group: str, log_rgn: str, log_event: dict) -> str:
    """ 生成 LogStream 链接 """
    if log_rgn.startswith('cn'):
        host = 'console.amazonaws.cn'
    else:
        host = 'console.aws.amazon.com'
    param = {'start': log_event["timestamp"], 'refEventId': log_event["eventId"]}
    e_path = f'{log_event["logStreamName"]}?{parse.urlencode(param)}'
    return f'https://{log_rgn}.{host}/cloudwatch/home?region={log_rgn}#logsV2:log-groups/log-group/{mask_url_part(log_group)}/log-events/{mask_url_part(e_path)}'  # noqa


def gen_cloud_watch_log_stream_url1(
        log_group: str, log_rgn: str, log_stream: str,
        timestamp: Optional[int] = None, event_id: Optional[str] = None,
) -> str:
    """ 生成 LogStream 链接 """
    if log_rgn.startswith('cn'):
        host = 'console.amazonaws.cn'
    else:
        host = 'console.aws.amazon.com'
    param = {}
    if timestamp is not None:
        param['start'] = timestamp
    if event_id is not None:
        param['refEventId'] = event_id
    e_path = f'{log_stream}?{parse.urlencode(param)}'
    return f'https://{log_rgn}.{host}/cloudwatch/home?region={log_rgn}#logsV2:log-groups/log-group/{mask_url_part(log_group)}/log-events/{mask_url_part(e_path)}'  # noqa


def get_fleet_address(region: str, fleet_id: str) -> str:
    if region.startswith('cn'):
        return f'https://{region}.console.amazonaws.cn/gamelift/fleets/view/{fleet_id}?region={region}'
    else:
        return f'https://{region}.console.aws.amazon.com/gamelift/fleets/view/{fleet_id}?region={region}'


def get_iam_role_url(role_arn: str) -> str:
    """
    根据 Role ARN 生成 AWS IAM 控制台的 Role 页面 URL。
    ARN 格式: arn:aws(-cn):iam::ACCOUNT_ID:role/ROLE_NAME
    """
    # arn:aws-cn:iam::878457991216:role/AwsTools
    parts = role_arn.split(":")
    partition = parts[1]          # "aws" or "aws-cn"
    role_name = parts[5]          # "role/AwsTools"
    if not role_name.startswith("role/"):
        raise ValueError(f"Unexpected role ARN format: {role_arn}")
    role_name = role_name[len("role/"):]
    host = "console.amazonaws.cn" if partition == "aws-cn" else "console.aws.amazon.com"

    return f"https://{host}/iam/home#/roles/details/{role_name}?section=permissions"


def get_lambda_function_url(region: str, function_name):
    region = region
    if 'cn' in region:
        # https://cn-northwest-1.console.amazonaws.cn/lambda/home?region=cn-northwest-1#/functions/PartyAnimals-FeishuNotifier
        fn_url = f'https://{region}.console.amazonaws.cn/lambda/home?region={region}#/functions/{function_name}'
    else:
        # https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/PartyAnimals-EventTrackingFunction
        fn_url = f'https://{region}.console.aws.amazon.com/lambda/home?region={region}#/functions/{function_name}'
    return fn_url


def get_s3_bucket_url(region: str, bucket_name: str) -> str:
    if region.startswith('cn'):
        return f'https://{region}.console.amazonaws.cn/s3/buckets/{bucket_name}?region={region}'
    else:
        return f'https://{region}.console.aws.amazon.com/s3/buckets/{bucket_name}?region={region}'


def get_cloud_watch_log_group_url(region: str, log_group_name: str) -> str:
    """
    获取 CloudWatch 日志组的 URL
    :param region: AWS 区域
    :param log_group_name: 日志组名称
    :return: 日志组的 URL
    """
    log_group_name = mask_url_part(log_group_name)
    if region.startswith('cn'):
        return f'https://{region}.console.amazonaws.cn/cloudwatch/home?region={region}#logsV2:log-groups/log-group/{log_group_name}'  # noqa: E501
    else:
        return f'https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:log-groups/log-group/{log_group_name}'  # noqa: E501


def get_cloud_watch_log_group_all_events_url(
        region: str, log_group_name: str,
        ts_start_ms: Optional[int] = None, ts_end_ms: Optional[int] = None, pattern: Optional[str] = None,
) -> str:
    # ts_start_ms, ts_end_ms = 1751904000000, 1751990399000
    # pattern = 'asdf'
    # log_group_url = '''
    # https://cn-northwest-1.console.amazonaws.cn/cloudwatch/home?region=cn-northwest-1#logsV2:log-groups/log-group/$252Faws$252Flambda$252FStandalone--39669-PhoneCertFunction/
    # log-events$3Fstart$3D1751904000000$26end$3D1751990399000$26filterPattern$3Dasdf'''

    def validate_ts_ms(ts: int) -> None:
        if ts < 0:
            raise ValueError(f'Timestamp in milliseconds must be a positive integer, got {ts}')
        digits = len(str(ts))
        if digits < 13 or digits > 16:
            raise ValueError(f'Timestamp in milliseconds must be between 13 and 16 digits, got {ts}({digits} digits)')
    log_group_url = get_cloud_watch_log_group_url(region, log_group_name)
    params = {}
    if ts_start_ms is not None:
        validate_ts_ms(ts_start_ms)
        params['start'] = ts_start_ms
    if ts_end_ms is not None:
        validate_ts_ms(ts_end_ms)
        params['end'] = ts_end_ms
    if pattern is not None:
        params['filterPattern'] = pattern

    return f'{log_group_url}/log-events$3F' + mask_url_part(parse.urlencode(params))


def mask_url_part(part: str) -> str:
    """ 转换为 HTML 码 """
    mappings = {
        '$252F': '/',
        '$252C': ',',
        '$255B': '[',
        '$255D': ']',
        # '$253D': '=',
        '$2521': '!',
        '$2522': '"',
        # '$252F': '_',
        '$257C': '|',
        '$2B': '+',
        '$26': '&',
        '$3D': '=',
        '$3F': '?'
    }
    for k, v in mappings.items():
        part = part.replace(v, k)
    return part
