import platform
from typing import Optional

from .aws_consts_profile import PC_TO_PROFILE, AwsProfile


def get_aws_profile(region: str, is_prod_aws: bool) -> str:
    pc_name = platform.node()
    profile: Optional[AwsProfile] = PC_TO_PROFILE.get(pc_name, None)
    if profile is None:
        raise KeyError(f'Need to set up the profile for PC({pc_name})')

    is_cn = region.lower().startswith('cn')
    if is_cn and not is_prod_aws:
        return profile.dev_cn
    if not is_cn and not is_prod_aws:
        return profile.dev_us
    if is_cn and is_prod_aws:
        return profile.prod_cn
    if not is_cn and is_prod_aws:
        return profile.prod_us
    raise NotImplementedError(f'Unexpected region({region}) and is_prod_aws({is_prod_aws}) combination')
