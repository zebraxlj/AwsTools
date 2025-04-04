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
