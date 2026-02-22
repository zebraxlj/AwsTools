import re
from collections import defaultdict
from enum import Enum
from typing import List, Dict

from AlertDataclass import LogDetail, RequestDetail
from LambdaErrDataclass import MissionErrEnum, StoreErrEnum, MatchingErrEnum, LoginErrEnum, LoginInfoErrEnum
from utils.aws_consts_profile import get_profiles_for_curr_pc, PROFILE_Samson


def is_msg_gql_head(log_msg: str) -> bool:
    return 'graphql_manager_base' in log_msg and 'error type = <class' in log_msg


def is_msg_gql_trace(log_msg: str) -> bool:
    return 'Errors Trace back' in log_msg


def is_gql_err(log_msg: str) -> bool:
    return 'GraphQLError: code is' in log_msg and 'message is' in log_msg


def is_pynamodb_trace(log_msg: str) -> bool:
    return 'pynamodb_utils._log_and_error_return' in log_msg and '|   ' in log_msg


def print_extra_analysis_header(name: str):
    print('\nExtra Analysis:', (f'{name} '+'='*40)[:40])


def print_extra_analysis_msg(msg: str, req_all: Dict[str, RequestDetail] = None, extra_dicts: List[Dict] = None):
    # 打印所有未知的 error 日志
    if req_all is not None:
        has_unknown = any([not req.is_err_known for req in req_all.values()])
        for req in req_all.values():
            if req.is_err_known:
                continue
            for log_detail in req.log_details:
                if '[ERROR]' in log_detail.message:
                    print(log_detail.message)
        print('\n' if has_unknown else '', end='')

    extra_dicts = extra_dicts if extra_dicts else []
    if msg or extra_dicts:
        print('report:','-'*40)
    if msg:
        print(msg)
    if get_profiles_for_curr_pc() == PROFILE_Samson:
        from utils.SystemTools.clipboard_writer import copy_to_clipboard
        copy_to_clipboard(msg)
    for d in extra_dicts:
        if d:
            print(d)
    with open('report.txt', 'w', encoding='utf8') as f_out:
        f_out.write(msg + '\n')
        for d in extra_dicts:
            if d:
                f_out.write(str(d) + '\n')


def get_req_details_from_log_details(log_details: List[LogDetail]) -> Dict[str, RequestDetail]:
    """ 日志拆分成事件 """
    # print('\n'.join(str(d) for d in log_details))

    req_all: Dict[str, RequestDetail] = dict()
    for d in log_details:
        if 'graphql_manager_base' in d.message and 'Errors Trace back' in d.message:
            continue
        if d.id not in req_all:
            req_all[d.id] = RequestDetail(dt_start=d.date_time, dt_end=d.date_time, id=d.id)
        if '__CallerId' in d.message:
            caller = re.findall(r".*?__CallerId': '(.*?)'", d.message)
            req_all[d.id].caller = caller[0] if len(caller) > 0 else ''
        req_all[d.id].dt_end = d.date_time if d.date_time > req_all[d.id].dt_end else req_all[d.id].dt_end
        req_all[d.id].log_details.append(d)
        req_body = re.findall(r'.*?START .*? LOGIC.*?body/event is (.*?)$', d.message, re.DOTALL|re.IGNORECASE)
        req_all[d.id].req_body = req_body[0] if len(req_body) > 0 else req_all[d.id].req_body
    return req_all


def check_account_info(log_details: List[LogDetail]):
    print_extra_analysis_header(check_account_info.__name__)
    req_all = get_req_details_from_log_details(log_details)
    # print('\n\n'.join(str(r) for r in req_all.values()))

    err_cnt: Dict[LoginInfoErrEnum, int] = defaultdict(int)
    db_err_detail: Dict[str,Dict[str, int]] = dict()
    for r in req_all.values():
        is_unknown = True
        for l in r.log_details:
            msg = l.message
            if "s2s.np.playstation.net', port=443): Read timed out" in msg:
                err_cnt[LoginInfoErrEnum.PsTimeout] += 1
                is_unknown = False
                break
            if "exceptions.ConnectionError" in msg and "Connection aborted" in msg:
                err_cnt[LoginInfoErrEnum.PsReqConnection] += 1
                is_unknown = False
                break
            if "play_station_manager" in msg and "Too Many Requests" in msg:
                err_cnt[LoginInfoErrEnum.PsTooManyReq] += 1
                is_unknown = False
                break
            if "play_station_manager" in msg and "failed! 401" in msg and "Invalid token" in msg:
                err_cnt[LoginInfoErrEnum.Ps401] += 1
                is_unknown = False
                break
            if "play_station_manager" in msg and "failed! 503" in msg:
                err_cnt[LoginInfoErrEnum.Ps503] += 1
                is_unknown = False
                break
            if "play_station_manager" in msg and "failed! 500" in msg:
                err_cnt[LoginInfoErrEnum.Ps500] += 1
                is_unknown = False
                break
            print(msg)
        if is_unknown:
            err_cnt[LoginInfoErrEnum.Unknown] += 1
    print_extra_analysis_msg(gen_report(err_cnt, len(req_all)), extra_dicts=[db_err_detail])


def check_account_info_old(log_details: List[LogDetail]):
    print_extra_analysis_header(check_account_info_old.__name__)
    for i in range(len(log_details)-1, -1, -1):
        log_msg = log_details[i].message
        if (
                is_msg_gql_head(log_msg)
                or is_msg_gql_trace(log_msg)
        ):
            log_details.pop(i)
    err_cnt = len([d for d in log_details if '[ERROR]' in d.message])
    err_cnt_lst = []
    # 没钱改名
    err_name_no_money = len([d for d in log_details if 'Do not have enough money to change display name' in d.message])
    err_cnt_lst.append(err_name_no_money)
    msg = ''
    msg += f'\n没钱改名 数量: {err_name_no_money}' if err_name_no_money else ''
    print_extra_analysis_msg(msg.lstrip())
    if sum(err_cnt_lst) != err_cnt:
        print('*'*10, '有其他错误', '*'*10)


def check_config_center_steam_stability(log_details: List[LogDetail]):
    print_extra_analysis_header(check_config_center_steam_stability.__name__)
    # ConfigCenter 统计 steam 服务不稳定 / 502 占比
    err_cnt = len([d for d in log_details if '[ERROR] ' in d.message])
    err_steam_service = len([d for d in log_details if (
            'GraphQLError: {"code": "9015"' in d.message and '"message": "Timeout:' in d.message
    )])
    err_steam_502 = len([d for d in log_details if (
            'SteamUnknownErr: code=502' in d.message or 'Bad Gateway' in d.message
    )])
    is_all_steam = err_cnt == err_steam_service + err_steam_502
    print(f'ConfigCenter: steam_err%={err_steam_service}/{err_cnt}, steam_502%={err_steam_502}/{err_cnt}, all steam: {is_all_steam}')
    msg = ('steam 鉴权 502' if err_steam_502 == err_cnt else
           'steam 鉴权不稳定' if err_steam_service == err_cnt else
           'steam 鉴权不稳定 + 502' if is_all_steam else
           'Unknown')
    print_extra_analysis_msg(msg)


def check_login_affected_user(log_details: List[LogDetail]):
    print_extra_analysis_header(check_login_affected_user.__name__)
    # 登录失败，统计影响玩家数量
    plat_ids = set()
    for d in log_details:
        if '---START LOGIN LOGIC, body/event' in d.message:
            plat_ids.add(re.findall(r'platformId[\D]*(\d+)', d.message)[0])
    print(plat_ids)
    print(f'affected players: {len(plat_ids)}')


def check_login(log_details: List[LogDetail]):
    print_extra_analysis_header(check_login.__name__)
    req_all = get_req_details_from_log_details(log_details)

    err_cnt: Dict[LoginErrEnum, int] = defaultdict(int)
    for r in req_all.values():
        for l in r.log_details:
            msg = l.message
            if 'Bad Steam AppId: 999998' in msg:  # FP DLC 功能没有上线，secret 没有生产环境 AppId，仅会报错
                err_cnt[LoginErrEnum.SteamAppId999998] += 1
                break
            if 'queue_manager.is_overload' in msg:
                err_cnt[LoginErrEnum.LoginQueue] += 1
    print_extra_analysis_msg(gen_report(err_cnt, len(req_all)))


def gen_report(err_cnt: Dict[Enum, int], req_cnt: int):
    report = (
        '' if sum(cnt for cnt in err_cnt.values()) == req_cnt
        else '*' * 10 + f' 有其他错误({req_cnt - sum(err_cnt.values())}) ' + '*' * 10 + f'\n总错误：{req_cnt}\t已知错误：{sum(err_cnt.values())}' + '\n'
    )
    report += '\t\n'.join(f'{e.value} 数量：{cnt}' for e, cnt in err_cnt.items())
    return report


def check_matching(log_details: List[LogDetail]):
    print_extra_analysis_header(check_matching.__name__)
    req_all = get_req_details_from_log_details(log_details)
    # print('\n\n'.join(str(r) for r in req_all.values()))

    err_cnt: Dict[MatchingErrEnum, int] = defaultdict(int)
    db_err_detail: Dict[str,Dict[str, int]] = dict()
    for r in req_all.values():
        for l in r.log_details:
            msg = l.message
            if 'No AccountStateModel found' in msg and re.match(r'.*?_P\d_D\d_J\d.*?', msg):
                err_cnt[MatchingErrEnum.DisbandLobbyLocalPlayer] += 1
                break
            pat_db_err = r'.*?table \((.*?)\).*?calling the (.*?) operation'
            if 'pynamodb.exception' in msg and 'Internal server error' in msg and re.match(pat_db_err, msg):
                err_cnt[MatchingErrEnum.DBInternalServerErr] += 1
                tbl, tbl_op = re.findall(pat_db_err, msg)[0]
                if tbl not in db_err_detail:
                    db_err_detail[tbl] = dict()
                if tbl_op not in db_err_detail[tbl]:
                    db_err_detail[tbl][tbl_op] = 0
                db_err_detail[tbl][tbl_op] += 1
            if "graphql_manager_base.py:36" in msg and "error msg:list index out of range" in msg:
                err_cnt[MatchingErrEnum.PsIndexOutOfRange] += 1
                break
            if "[ERROR]" in msg and (
                    "NoneType' object has no attribute 'items" in msg
                    or "play_station_manager.py:493 ⫸ leave_player_session: no access token" in msg
                    or "play_station_manager.py:325 ⫸ get_player_session: no access token" in msg
                    or "play_station_manager.py:387 ⫸ change_player_session_leader: no access token" in msg
                    or "play_station_manager.py:444 ⫸ put_player_sessions_non_psn_leader: no access token" in msg
            ):
                err_cnt[MatchingErrEnum.PsWait1_25_Ver] += 1
                break
            if 'exceptions.ReadTimeout' in msg and 's2s.np.playstation.net' in msg:
                err_cnt[MatchingErrEnum.PsTimeout] += 1
                break
            if 'Connection aborted' in msg and 'Connection reset by peer' in msg:
                err_cnt[MatchingErrEnum.PsReqConnection] += 1
                break
            if "play_station_manager" in msg and "failed! 503" in msg:
                err_cnt[MatchingErrEnum.Ps503] += 1
                break
            if '[ERROR]' in msg:
                print(msg)

    print_extra_analysis_msg(gen_report(err_cnt, len(req_all)), extra_dicts=[db_err_detail])


def check_mission_system(log_details: List[LogDetail]):
    print_extra_analysis_header(check_mission_system.__name__)
    req_all = get_req_details_from_log_details(log_details)
    # print('\n\n'.join(str(r) for r in req_all.values()))

    # 对请求进行分析
    err_cnt: Dict[MissionErrEnum, int] = defaultdict(int)
    txn_ongoing_detail: Dict = dict()
    for r in req_all.values():
        for l in r.log_details:
            # 教2 教3 多次结算导致的幂等报错
            l_msg = l.message
            if 'check_idempotent failed' in l_msg:
                if 'finishTutorial2 ' in r.req_body:
                    err_cnt[MissionErrEnum.Tutorial2Idempotent] += 1
                elif 'finishTutorial3 ' in r.req_body:
                    err_cnt[MissionErrEnum.Tutorial3Idempotent] += 1
                elif 'finishTutorial ' in r.req_body:
                    err_cnt[MissionErrEnum.TutorialIdempotent] += 1
                break
            txn_ongoing = re.findall(r'.*?on table \((.*?)\).*?calling the (.*?) operation: Transaction is ongoing',
                                     l_msg, re.DOTALL|re.IGNORECASE)
            if len(txn_ongoing) > 0:
                err_cnt[MissionErrEnum.DbTransactionOngoing] += 1
                tbl, tbl_op = txn_ongoing[0][0], txn_ongoing[0][1]
                if tbl not in txn_ongoing_detail:
                    txn_ongoing_detail[tbl] = dict()
                if tbl_op not in txn_ongoing_detail[tbl]:
                    txn_ongoing_detail[tbl][tbl_op] = 0
                txn_ongoing_detail[tbl][tbl_op] += 1
                break

    print_extra_analysis_msg(gen_report(err_cnt, len(req_all)), extra_dicts=[txn_ongoing_detail])


def check_store_old(log_details: List[LogDetail]):
    print_extra_analysis_header(check_store_old.__name__)
    for i in range(len(log_details)-1, -1, -1):
        log_msg = log_details[i].message
        if (
                is_msg_gql_head(log_msg)
                or is_msg_gql_trace(log_msg)
                or is_pynamodb_trace(log_msg)
        ):
            log_details.pop(i)
    err_cnt = len([d for d in log_details if '[ERROR]' in d.message])
    err_cnt_lst = []
    # steam 超时
    err_steam_user_info = len([d for d in log_details if ('Steam get user info' in d.message and 'timed out' in d.message)])
    err_cnt_lst.append(err_steam_user_info)
    err_steam_txn = len([d for d in log_details if 'Steam finalize transaction' in d.message and 'timed out' in d.message])
    err_cnt_lst.append(err_steam_txn)
    # DB Err
    db_err_detail = defaultdict(int)
    for d in log_details:
        pat_cond_err = r'.*?Failed to (.*?):.*?ConditionalCheckFailed.*?table \((.*?)\).*?'
        pat_tran_err = r'.*?⫸ (.*?) failed.*?Transaction is ongoing.*?'
        pat_cond_check_err = re.compile(pat_cond_err, re.IGNORECASE)
        if re.match(pat_cond_check_err, d.message):
            db_op, tbl = re.findall(pat_cond_check_err, d.message)[0]
            db_err_detail[f'{db_op}|{tbl}|ConditionalCheckFailed'] += 1
        elif re.match(pat_tran_err, d.message):
            db_op = re.findall(pat_tran_err, d.message)[0]
            db_err_detail[f'{db_op}|Transaction Ongoing'] += 1
    db_err_cnt = sum(v for v in db_err_detail.values())
    err_cnt_lst.append(db_err_cnt)
    db_err_msg = '\n'.join([f'{k} 数量: {v}' for k, v in db_err_detail.items()])
    # CdKey Err
    cd_key_err_detail = defaultdict(int)
    for d in log_details:
        pat_cd_key_gql = re.compile(r'.*?GraphQLError: code is (.*?), message is (.*?)$', re.DOTALL)
        if 'Bad CdKey Category=' in d.message:
            cate_id = d.message.split('Bad CdKey Category=')[1]
            cate_id = '沙石镇' if cate_id == '105@1.6' else cate_id
            cd_key_err_detail[f'CateId-{cate_id}'] += 1
        elif 'Cannot find the product_id: ' in d.message:
            prod_id = d.message.split('Cannot find the product_id: ')[1]
            prod_id = '沙石镇' if prod_id == 'P0540@1.6' else prod_id
            cd_key_err_detail[f'ProdId-{prod_id}'] += 1
        # elif re.match(pat_cd_key_gql, d.message):
        #     code, _msg = re.findall(pat_cd_key_gql, d.message)[0]
        #     cd_key_err_detail[f'gql-{code}-{_msg}'] += 1
    cd_key_err_cnt = sum(v for v in cd_key_err_detail.values())
    err_cnt_lst.append(cd_key_err_cnt)
    cd_key_err_msg = '\n'.join([f'{k} 数量: {v}' for k, v in cd_key_err_detail.items()])
    # 输出结果
    is_all_steam = err_cnt == err_steam_user_info + err_steam_txn
    if err_steam_user_info != 0 or err_steam_txn != 0:
        print(f'Store: steam_err%={err_steam_user_info}/{err_cnt} err_steam_txn%={err_steam_txn}/{err_cnt}, all steam: {is_all_steam}')
    is_all_db = err_cnt == db_err_cnt
    if db_err_cnt != 0:
        print(f'Store: db_err%={db_err_cnt}/{err_cnt}, all db: {is_all_db}')
    is_all_cd_key = err_cnt == cd_key_err_cnt
    if cd_key_err_cnt != 0:
        print(f'Store: cd_key_err%={cd_key_err_cnt}/{err_cnt}, all cd key: {is_all_cd_key}')
    msg = ''
    msg += f'\nSteam get user info 超时数量: {err_steam_user_info}' if err_steam_user_info else ''
    msg += f'\nSteam transaction 超时数量: {err_steam_txn}' if err_steam_txn else ''
    msg += ('\nDB report:\n' + db_err_msg) if db_err_msg else ''
    msg += ('\nCdKey report:\n' + cd_key_err_msg) if cd_key_err_msg else ''
    print_extra_analysis_msg(msg.lstrip())
    if sum(err_cnt_lst) != err_cnt:
        print('*'*10, '有其他错误', '*'*10)


def check_store(log_details: List[LogDetail]):
    print_extra_analysis_header(check_store.__name__)
    req_all: Dict[str, RequestDetail] = get_req_details_from_log_details(log_details)
    # print('\n\n'.join(str(r) for r in req_all.values()))
    err_cnt: Dict[StoreErrEnum, int] = defaultdict(int)
    bad_cd_key_detail: Dict[str, Dict[str, int]] = dict()
    cd_key_detail: []
    # 对请求进行分析
    for req_id, req in req_all.items():
        for l in req.log_details:
            l_msg = l.message
            if (
                    ('Steam InitTxn exception' in l_msg and 'read operation timed out' in l_msg)
                    or ('Steam finalize transaction' in l_msg and 'read operation timed out' in l_msg)
            ):
                err_cnt[StoreErrEnum.SteamTxnTimeout] += 1
                req_all[req_id].is_err_known = True
                break
            if 'Steam get user info : Exception is The read operation timed out' in l_msg:
                err_cnt[StoreErrEnum.SteamGetUserInfoTimeout] += 1
                req_all[req_id].is_err_known = True
                break
            if (
                ('cd_key_manager' in l_msg and '@1.6' in l_msg and '115570' in l.url)
                or ('VA0470@1.9 does not exist' and '154790' in l.url)  # 1.10 分线不存在时主线配表，后缀是 1.9
            ):
                err_cnt[StoreErrEnum.CdKeyOldEnv] += 1
                req_all[req_id].is_err_known = True
                break
            if 'No CdKey12Model found' in l_msg:
                err_cnt[StoreErrEnum.CdKeyBadKey] += 1
                key = l_msg.split("No CdKey12Model found for ('")[1].split("'")[0]
                if req.caller not in bad_cd_key_detail:
                    bad_cd_key_detail[req.caller] = {key: 0}
                elif key not in bad_cd_key_detail[req.caller]:
                    bad_cd_key_detail[req.caller][key] = 0
                bad_cd_key_detail[req.caller][key] += 1
                req_all[req_id].is_err_known = True
                break
            if 'orm.order_model.OrderModel.DoesNotExist' in l_msg:
                err_cnt[StoreErrEnum.OrderNotExist] += 1
                req_all[req_id].is_err_known = True
                break
            if 'CheckIdempotentError' in l_msg:
                err_cnt[StoreErrEnum.CheckIdempotentError] += 1
                req_all[req_id].is_err_known = True
                break
            if 'requests.exceptions.ReadTimeout' in l_msg and 's2s.np.playstation.net' in l_msg:
                err_cnt[StoreErrEnum.PsTimeout] += 1
                req_all[req_id].is_err_known = True
                break
            if 'steam_get_user_info(' in l_msg and ') Internal Server Error' in l_msg:
                err_cnt[StoreErrEnum.SteamInternalServerError] += 1
                req_all[req_id].is_err_known = True
                break
            if 'Steam code: 9' in l_msg:
                err_cnt[StoreErrEnum.SteamCode9] += 1
                req_all[req_id].is_err_known = True
                break
            if '(ConditionalCheckFailedException)' in l_msg and 'table (HeoPartyAnimalsBpLoginReward)' in l_msg:
                err_cnt[StoreErrEnum.CondCheckFailBpLoginReward] += 1
                req_all[req_id].is_err_known = True
                break
    print_extra_analysis_msg(gen_report(err_cnt, len(req_all)), req_all=req_all, extra_dicts=[bad_cd_key_detail])
