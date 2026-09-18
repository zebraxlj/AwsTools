from dataclasses import dataclass
from typing import Optional

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
            if isinstance(attr, Env) and attr.name.lower() == env_name.lower():
                return attr
        raise ValueError(f'Unknown environment={env_name}')


@dataclass(frozen=True)
class AwsRegion:
    abbr: str
    alias: str
    flag_utf8: str
    name: str
    name_cn: str


class AllRegions:
    BJ = AwsRegion(abbr='BJ', alias='北京', flag_utf8='🇨🇳', name='cn-north-1', name_cn='北京')
    NX = AwsRegion(abbr='NX', alias='宁夏', flag_utf8='🇨🇳', name='cn-northwest-1', name_cn='宁夏')
    JP = AwsRegion(abbr='JP', alias='东京', flag_utf8='🇯🇵', name='ap-northeast-1', name_cn='东京')
    EU = AwsRegion(abbr='EU', alias='欧洲', flag_utf8='🇪🇺', name='eu-central-1', name_cn='法兰克福')
    SP = AwsRegion(abbr='SP', alias='南美', flag_utf8='🇧🇷', name='sa-east-1', name_cn='圣保罗')
    US = AwsRegion(abbr='US', alias='北美', flag_utf8='🇺🇸', name='us-east-1', name_cn='弗吉尼亚')

    @classmethod
    def all_regions(cls) -> list[AwsRegion]:
        """
        获取所有区域对象，顺序与定义顺序一致
        :return: AwsRegion 对象列表
        """
        return [rgn for rgn in cls.__dict__.values() if isinstance(rgn, AwsRegion)]

    @classmethod
    def get_region_by_name(cls, name: str) -> AwsRegion:
        """
        根据区域名称获取 AwsRegion 对象，大小写敏感
        :param name: 区域名称，如 cn-north-1
        :return: AwsRegion 对象
        """
        for rgn in cls.all_regions():
            if rgn.name == name:
                return rgn
        raise ValueError(f'Unknown region name={name}')

    @classmethod
    def get_region_by_abbr(cls, abbr: str) -> AwsRegion:
        """
        根据区域缩写获取 AwsRegion 对象，缩写必须大写
        :param abbr: 区域缩写，如 BJ
        :return: AwsRegion 对象
        """
        for rgn in cls.all_regions():
            if rgn.abbr == abbr:
                return rgn
        raise ValueError(f'Unknown region abbr={abbr}')

    @classmethod
    def normalize_name(cls, value: str, default: Optional[str] = None) -> str:
        """
        把区域缩写或名称统一成区域名称，用于归一化命令行入参
        :param value: 区域缩写（必须大写）或区域名称
        :param default: 无法识别时的返回值，为 None 时原样返回 value
        :return: 区域名称，如 cn-north-1
        """
        for rgn in cls.all_regions():
            if value in (rgn.name, rgn.abbr):
                return rgn.name
        return value if default is None else default

    @classmethod
    def to_abbr(cls, name: str, default: Optional[str] = None) -> str:
        """
        把区域名称转成区域缩写，用于表格列、文件名等展示
        :param name: 区域名称，如 cn-north-1
        :param default: 无法识别时的返回值，为 None 时原样返回 name
        :return: 区域缩写，如 BJ
        """
        for rgn in cls.all_regions():
            if rgn.name == name:
                return rgn.abbr
        return name if default is None else default

    @classmethod
    def to_flag(cls, value: str, default: Optional[str] = None) -> str:
        """
        把区域缩写或名称转成国旗 emoji，用于表格列展示
        :param value: 区域缩写（必须大写）或区域名称
        :param default: 无法识别时的返回值，为 None 时原样返回 value
        :return: 国旗 emoji，如 🇨🇳
        """
        for rgn in cls.all_regions():
            if value in (rgn.name, rgn.abbr):
                return rgn.flag_utf8
        return value if default is None else default

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """
        判断区域是否已在本文件中定义，缩写（必须大写）和名称都算
        :param value: 区域缩写或区域名称
        :return: 是否已定义
        """
        return any(value in (rgn.name, rgn.abbr) for rgn in cls.all_regions())


REGION_NAME_TO_REGION = {rgn.name: rgn for rgn in AllRegions.all_regions()}

REGION_ABBR = {rgn.abbr: rgn.name for rgn in AllRegions.all_regions()}
REGION_ABBR['AP'] = AllRegions.JP.name  # 兼容旧缩写，Lambda/get_lambda_info_async.py 仍在用

REGION_TO_ABBR = {rgn.name: rgn.abbr for rgn in AllRegions.all_regions()}

REGION_TO_CHINESE = {rgn.name: rgn.alias for rgn in AllRegions.all_regions()}
