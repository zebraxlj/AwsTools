from dataclasses import dataclass

ACCT_DEV_CN = ''
ACCT_DEV_US = ''
ACCT_PROD_CN = ''
ACCT_PROD_US = ''


@dataclass
class Env:
    name: str
    is_prod_aws: bool = False  # 是否是生产环境 AWS


class AllEnvs:
    NemoDevCand = Env('NemoDev-cand')
    NemoDevMaprefine = Env('NemoDev-maprefine')
    NemoDevTrunk = Env('NemoDev-trunk')
    NemoTestComedy = Env('NemoTest-comedy')
    NemoTestTestLb = Env('NemoTest-test-lb')
    NemoTestValve = Env('NemoTest-valve')
    Standalone = Env('Standalone')
    StandaloneCand = Env('Standalone-cand')

    PartyAnimals = Env('PartyAnimals', True)
    PartyAnimalsInteral = Env('PartyAnimals-interal', True)

    @classmethod
    def get_env_by_name(cls, env_name) -> Env:
        for attr in cls.__dict__.values():
            if isinstance(attr, Env) and attr.name == env_name:
                return attr
        raise ValueError(f'Unknown environment={env_name}')


REGION_ABBR = {
    'BJ': 'cn-north-1',
    'NX': 'cn-northwest-1',
    'JP': 'ap-northeast-1',
    'AP': 'ap-northeast-1',
    'EU': 'eu-central-1',
    'US': 'us-east-1',
}

REGION_TO_ABBR = {
    v: k for k, v in REGION_ABBR.items()
}
