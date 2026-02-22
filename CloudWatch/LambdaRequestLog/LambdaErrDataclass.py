from enum import Enum


class LoginErrEnum(Enum):
    LoginQueue = '登录排队'
    SteamAppId999998 = 'FP DLC 不影响登录'


class LoginInfoErrEnum(Enum):
    Ps401 = 'PS 请求 401, 157330 之后的网关出现需 @马丁'
    Ps500 = 'PS 服务 internal server error'
    Ps503 = 'PS 请求 503'
    PsReqConnection = 'PS 请求连接断开'
    PsTimeout = 'PS 请求超时'
    PsTooManyReq = 'PS Too Many Requests'
    Unknown = '未知错误'


class MatchingErrEnum(Enum):
    DisbandLobbyLocalPlayer = '马丁：应该在抛异常前拦截下，等我节后处理，现在抛错的逻辑是对的'
    DBInternalServerErr = 'DB Internal Server Error'
    Ps503 = 'PS 请求 503'
    PsIndexOutOfRange = '1.10.x 马丁：1/24 合并的分线，1/25 发版会好'
    PsReqConnection = 'PS 请求连接断开'
    PsTimeout = 'PS 请求超时'
    PsWait1_25_Ver = '1.10.x PS 相关，等 1/25 发版'


class MissionErrEnum(Enum):
    DbTransactionOngoing = '数据库 Transaction is ongoing'
    TutorialIdempotent = '教1 结算 多次结算导致幂等问题（已知，RT）'
    Tutorial2Idempotent = '教2 结算 多次结算导致幂等问题（已知，RT）'
    Tutorial3Idempotent = '教3 结算 多次结算导致幂等问题（已知，RT）'


class StoreErrEnum(Enum):
    CdKeyBadKey = '玩家用不存在的 CdKey 兑换'
    CdKeyOldEnv = 'CdKey 客户端需要升级'
    CheckIdempotentError = '1.10.x 累登 CheckIdempotentError'
    CondCheckFailBpLoginReward = 'BpLoginReward ConditionalCheckFailed 等 20250220 版本网关'
    OrderNotExist = 'OrderModel.DoesNotExist'
    PsTimeout = 'PS 请求超时'
    SteamCode9 = 'Steam Code 9'
    SteamGetUserInfoTimeout = 'Steam GetUserInfo 超时'
    SteamTxnTimeout = 'Steam Transaction 服务超时'
    SteamInternalServerError = 'Steam Internal Server Error'
    Unknown = '未知错误'
