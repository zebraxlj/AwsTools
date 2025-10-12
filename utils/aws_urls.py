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
