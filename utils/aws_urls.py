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


def get_fleet_address(region: str, fleet_id: str) -> str:
    if region.startswith('cn'):
        return f'https://{region}.console.amazonaws.cn/gamelift/fleets/view/{fleet_id}?region={region}'
    else:
        return f'https://{region}.console.aws.amazon.com/gamelift/fleets/view/{fleet_id}?region={region}'


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
