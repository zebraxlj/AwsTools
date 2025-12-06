from typing import Dict

from aiobotocore.session import AioSession

from utils.aws_client_helper import get_aws_profile


_SESSION_CACHE: Dict[str, AioSession] = {}  # Cache for sessions


def get_cached_aiosession(region: str, is_prod: bool) -> AioSession:
    """ 获取一个缓存的异步 AWS Session

    Args:
        region (str): AWS 区域
        is_prod (bool): 是否是生产环境

    Returns:
        AioSession: 异步的 AWS Session
    """
    key = f"{region}:{is_prod}"
    if key not in _SESSION_CACHE:
        _SESSION_CACHE[key] = get_aiosession(region, is_prod)
    return _SESSION_CACHE[key]


def get_aiosession(region: str, is_prod: bool) -> AioSession:
    """ 获取一个异步 AWS Session

    Args:
        region (str): AWS 区域
        is_prod (bool): 是否是生产环境

    Returns:
        AioSession: 异步的 AWS Session
    """
    profile = get_aws_profile(region, is_prod)
    return AioSession(profile=profile)
