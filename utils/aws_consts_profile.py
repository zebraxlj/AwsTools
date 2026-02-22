import platform
from dataclasses import dataclass


@dataclass
class AwsProfile:
    dev_cn: str = ''
    dev_us: str = ''
    prod_cn: str = ''
    prod_us: str = ''


PROFILE_Samson = AwsProfile(
    dev_cn='default', dev_us='aws-tools', prod_cn='nemo-prod-cn', prod_us='nemo-prod',
)

PC_TO_PROFILE = {
    'Source-XiaLijie': PROFILE_Samson,
    'DESKTOP-U6859CU': PROFILE_Samson,
    'SAMSON-G15': PROFILE_Samson,
}


def get_profiles_for_curr_pc():
    pc_name = platform.node()
    profile: AwsProfile = PC_TO_PROFILE.get(pc_name, None)
    if profile is None:
        raise KeyError(f'Need to set up the profile for your PC({pc_name})')
    return profile
