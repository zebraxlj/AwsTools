import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import List, Optional


os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from utils.aws_consts import AllEnvs, AllFunctions, Env, REGION_TO_CHINESE  # noqa: E402
from utils.aws_urls import get_cloud_watch_log_group_all_events_url  # noqa: E402

DATA_PATH = 'data'
DATA_PATH_INPUT = f'{DATA_PATH}/input'
DATA_PATH_OUTPUT = f'{DATA_PATH}/output'
DATA_PATH_COMPLETED = f'{DATA_PATH}/completed'

FMT_DT_FILE = '%Y-%m-%d_%H-%M-%S-%f'
FMT_DT_IN = '%Y-%m-%d %H:%M:%S:%f'
FMT_DT_OUT = '%Y-%m-%d %H:%M:%S.%f'

PAT_LOG_LINE_START = r'^(?P<dt_str>.*?) \[.*?\] \[.*?\] \[(?P<thread>\d*)\] \[.*?\]\s*?'
PAT_LOG_LINE1 = r'^(?P<dt_str>\d\d-\d\d \d\d:\d\d:\d\d:\d\d\d) \[.*?\] \[.*?\] \[(?P<thread>\d*)\].*?$'
PAT_GUID_URI = r'^(?P<dt_str>.*?) \[.*?\] \[.*?\] \[.*?\].*?Guid = (?P<guid>.*?),.*?uri = (?P<uri>.*?),.*?$'
PAT_GUID_RES = r'^(?P<dt_str>.*?) \[.*?\] \[.*?\] \[.*?\].*?Guid = (?P<guid>.*?),.*?code =.*?$'
PAT_GUID_TIMEOUT = r'^(?P<dt_str>.*?) \[.*?\] \[.*?\] \[.*?\].*?time out.*?Guid = (?P<guid>.*?)$'
PAT_RECREATE_INTR_REQUEST = r'^.*? \[.*?\] \[.*?\] \[(?P<thread>\d*)\] \[.*?\]\s*?\[MenuSceneLoader\] <RequestInterruption> (?P<interruption>.*?),.*?$'  # noqa: E501
PAT_RECREATE_INTR_DROP = r'^(?P<dt_str>.*?) \[.*?\] \[.*?\] \[(?P<thread>\d*)\] \[.*?\]\s*?\[MenuSceneLoader\] <DrawInterruption> (?P<interruption>.*?),.*?$'  # noqa: E501


@dataclass
class LogInfo:
    Msg: List[str] = field(default_factory=list)
    Trace: List[str] = field(default_factory=list)


@dataclass
class LogBlockInfo:
    Build: Optional[str] = ""
    LogDt: Optional[str] = ""
    # 阶段相关的基础信息
    StageName: Optional[str] = ""
    StageDesc: Optional[str] = ""
    DtStart: Optional[datetime] = None
    DtEnd: Optional[datetime] = None
    MsStart: Optional[Decimal] = None
    MsEnd: Optional[Decimal] = None
    MsDuration: Optional[Decimal] = None
    ThreadStart: Optional[int] = None
    ThreadEnd: Optional[int] = None

    # 网关请求
    GUID: Optional[str] = None
    AwsRegion: Optional[str] = None
    Lambda: Optional[str] = None
    LambdaEnv: Optional[Env] = None
    LambdaFunc: Optional[str] = None
    LambdaName: Optional[str] = None
    LambdaPath: Optional[str] = None
    LambdaResp: Optional[str] = None
    LambdaRespCode: Optional[str] = None
    LambdaRespMessage: Optional[str] = None
    LambdaRespResult: Optional[str] = None
    LambdaRgn: Optional[str] = None
    LambdaSubEnv: Optional[int] = None
    LogGroupUri: Optional[str] = None

    _RequestUri: Optional[str] = None
    _RequestResp: Optional[str] = None

    @property
    def RequestUri(self) -> Optional[str]:
        return self._RequestUri

    @RequestUri.setter
    def RequestUri(self, value: str):
        if value is not None:
            value = value.strip()
        self._RequestUri = value
        self.parse_request_uri()

    @property
    def RequestResp(self) -> Optional[str]:
        return self._RequestResp

    @RequestResp.setter
    def RequestResp(self, value: str):
        if value is not None:
            value = value.strip()
        self._RequestResp = value
        self.parse_request_resp()

    def is_lambda_request(self) -> bool:
        DOMAIN_CN, DOMAIN_US = 'api.recreategames.com.cn', 'api.partyanimalsgame.com'
        return self._RequestUri is not None and (DOMAIN_CN in self._RequestUri or DOMAIN_US in self._RequestUri)

    def parse_request_uri(self) -> None:
        """ 解析 RequestUri，提取 AWS Region、Lambda 名称、Lambda 环境等信息 """
        if self._RequestUri and self.is_lambda_request():
            tmp_url = self._RequestUri.strip().replace('https://', '').replace('http://', '').replace('cdn.', '')
            domains = tmp_url.split('/')[0]
            domain_tkns = domains.split('.')
            lambda_env = AllEnvs.get_env_by_name(domain_tkns[1])
            lambda_path = self._RequestUri.split('/')[-1]
            lambda_sub_env_str = self._RequestUri.split('/')[-2]
            lambda_sub_env = int(lambda_sub_env_str) if lambda_sub_env_str.isdigit() else None

            lambda_func = AllFunctions.get_func_by_path(lambda_path)

            self.AwsRegion = domain_tkns[0]
            self.Lambda = lambda_path
            self.LambdaEnv = lambda_env
            self.LambdaFunc = lambda_func.get_full_name(lambda_env, lambda_sub_env)
            self.LambdaName = self.Lambda
            self.LambdaPath = lambda_path
            self.LambdaRgn = REGION_TO_CHINESE.get(self.AwsRegion, '??')
            self.StageDesc = f'{self.Lambda}_{self.LambdaRgn}'

            self.LogGroupUri = get_cloud_watch_log_group_all_events_url(
                self.AwsRegion, lambda_func.get_log_group_name(lambda_env, lambda_sub_env),
                ts_start_ms=int(self.DtStart.timestamp() * 1000) if self.DtStart else None,
                ts_end_ms=(
                    int(self.DtEnd.timestamp() * 1000) if self.DtEnd
                    else int((self.DtStart + timedelta(seconds=15)).timestamp() * 1000) if self.DtStart
                    else None
                ),
            )
        else:
            self.StageDesc = self._RequestUri

    def parse_request_resp(self) -> None:
        """ 解析 RequestResp，提取 Lambda 执行信息 """
        if self._RequestResp and self.is_lambda_request():
            self.LambdaResp = self._RequestResp
            self.LambdaRespCode = '' if self.LambdaRespCode is None else self.LambdaRespCode
            self.LambdaRespMessage = '' if self.LambdaRespMessage is None else self.LambdaRespMessage
            self.LambdaRespResult = '' if self.LambdaRespResult is None else self.LambdaRespResult

            try:
                lambda_resp_dict = json.loads(self._RequestResp)
                self.LambdaRespCode = lambda_resp_dict.get('code', None)
                self.LambdaRespMessage = lambda_resp_dict.get('message', None)
                # self.LambdaRespResult = f"{lambda_resp_dict.get('result', None)}"
                self.LambdaRespResult = lambda_resp_dict.get('result', None)

                key_content_null = 'content null?'
                if key_content_null in self._RequestResp:
                    self.LambdaRespMessage = f"{key_content_null} {self._RequestResp.split(key_content_null)[-1]}"

                if not self.LambdaRespCode and not self.LambdaRespMessage and not self.LambdaRespResult:
                    self.LambdaRespMessage = self._RequestResp
            except Exception:
                key_code = 'code ='
                if key_code in self._RequestResp:
                    self.LambdaRespCode = self._RequestResp.split(key_code)[-1].split(',')[0]
                key_content_null = 'content null?'
                if key_content_null in self._RequestResp:
                    self.LambdaRespMessage = f"{key_content_null} {self._RequestResp.split(key_content_null)[-1]}"
                else:
                    self.LambdaRespMessage = self._RequestResp[:20]
        else:
            if self.StageDesc and '幻数' in self.StageDesc:
                self.LambdaRespMessage = self._RequestResp


@dataclass
class LambdaExecInfo:
    GUID: Optional[str] = None
    DurationLambdaInit: Optional[float] = None
    DurationLambdaExec: Optional[float] = None


def parse_log_file(log_path) -> List[LogInfo]:
    """ 读取客户端日志文件，解析成 LogInfo 格式

    Args:
        file_path (str): 文件路径

    Returns:
        List[LogInfo]: 解析后的日志信息
    """
    print(f'开始解析 {log_path}')
    log_all: List[LogInfo] = []
    with open(log_path, 'r', encoding='utf-8-sig') as f_in:
        lines = f_in.readlines()
        for i in range(len(lines)):
            line = lines[i].strip()
            if not line:
                # 空行不处理
                continue
            elif not log_all and not re.match(PAT_LOG_LINE1, line):
                # 处于任何原因，日志第一行不是 log message，跳过
                print('日志文件不以 log message 开始', line)
            elif re.match(PAT_LOG_LINE1, line):
                # print('是 行1')
                log_all.append(LogInfo(
                    Msg=[line]
                ))
                # if not build and '(Build-' in line and line.endswith(')'):
                #     build = 'Build-' + line.split('(Build-')[1][:-1]
            elif re.match(r'^\s*?at .*?', line):
                # print('是 trace')
                log_all[-1].Trace.append(line)
            elif not log_all[-1].Trace:
                # 多行日志文本的情况，还没到 traceback
                # print('是 多行')
                log_all[-1].Msg.append(line)
            elif line.strip() == 'Log stream is closed':
                pass
            else:
                print('未处理的日志行', line)
        print(f'总处理的行数：{len(lines)}')
        print(f'总日志数量：{len(log_all)}')
        print(f'总首行日志：{len([line for line in lines if re.match(PAT_LOG_LINE1, line)])}')
    return log_all


def parse_log_line_dt(year: int, line_dt: str) -> datetime:
    return datetime.strptime(f'{year}-{line_dt}', FMT_DT_IN).replace(tzinfo=timezone.utc)


def move_input_file(file_path_src: str) -> None:
    if DATA_PATH_INPUT not in file_path_src:
        print('')
    if not os.path.exists(file_path_src):
        pass
    file_path_dest = file_path_src.replace(DATA_PATH_INPUT, DATA_PATH_COMPLETED)
    os.rename(file_path_src, file_path_dest)


def write_output_file_GUID(file_path_out: str, log_blocks: List[LogBlockInfo]) -> None:
    # 输出结果
    with open(file_path_out, 'w', encoding='utf8', newline='') as f_out:
        field_names = [
            'DtStart',
            'MsDuration',
            'GUID',
            'StageDesc',
            'LambdaRgn',
            'LambdaRespCode', "LambdaRespMessage", 'LambdaRespResult',
            'LogGroupUri', 'LambdaResp',
        ]
        writer = csv.DictWriter(f_out, delimiter='\t', fieldnames=field_names)
        writer.writeheader()
        rows = [asdict(log_block) for log_block in log_blocks if log_block.GUID]
        rows = [{k: v for k, v in row.items() if k in field_names} for row in rows]
        writer.writerows(rows)


def write_output_file_stage(file_path_out: str, log_blocks: List[LogBlockInfo]) -> None:
    # 输出结果
    with open(file_path_out, 'w', encoding='utf8', newline='') as f_out:
        field_names = [
            'Build', 'LogDt',
            'StageName', 'StageDesc',
            'DtStart', 'DtEnd',
            'MsStart', 'MsEnd', 'MsDuration',
            'GapSec',
            'ThreadStart', 'ThreadEnd',
            'GUID',
            'Lambda', 'LambdaRgn',
        ]
        writer = csv.DictWriter(f_out, delimiter='\t', fieldnames=field_names)
        writer.writeheader()
        rows = [asdict(stage) for stage in log_blocks]
        rows = [{k: v for k, v in row.items() if k in field_names} for row in rows]
        row_prev = None
        for i, row in enumerate(rows):
            row_ori = row.copy()
            row['DtStart'] = datetime.strftime(row_ori['DtStart'], FMT_DT_OUT)[:-3]
            row['DtEnd'] = datetime.strftime(row_ori['DtEnd'], FMT_DT_OUT)[:-3] if row_ori['DtEnd'] else ''
            # 对额外列加默认值
            additional_cols = {'GapSec': 0}
            if i == 0:
                row_prev = row_ori
                rows[i] = {**row, **additional_cols}
                continue

            additional_cols = {'GapSec': (row_ori['DtStart'] - row_prev['DtStart']).total_seconds() if row_prev else 0}
            rows[i] = {**row, **additional_cols}
            row_prev = row_ori
        writer.writerows(rows)


def handle_1_file(file_path: str):
    file_name = os.path.basename(file_path)

    # 日志文件名中获取日志生成时间，用于计算不同步骤距开始第几毫秒开始、结束
    assert len(file_name) > 23, '文件名可能不包含日期 年-月-日_时-分-秒-毫秒，例如：2025-01-16_03-44-23-143-RECREATE06.log'
    dt_log = datetime.strptime(file_name[:23], FMT_DT_FILE).replace(tzinfo=timezone.utc)
    # 日志行没有年份信息，只能从日志文件名中获取
    year_file, year_log = dt_log.year, dt_log.year

    log_all: List[LogInfo] = parse_log_file(file_path)

    log_blocks: List[LogBlockInfo] = [LogBlockInfo(StageName='生成日志文件', DtStart=dt_log)]

    build = None  # 客户端版本号
    dt_prev_str = None
    for log_idx, log in enumerate(log_all):
        log_line_1 = log.Msg[0]
        match_log_line = re.search(PAT_LOG_LINE1, log_line_1)

        # 检查日志首行是否有时间
        if not match_log_line:
            print('日志 message 提取不到时间', PAT_LOG_LINE1, log_line_1)
            continue

        thread = int(match_log_line.group('thread'))

        # 解析日志时间
        dt_curr_str = match_log_line.group('dt_str')
        if dt_prev_str and dt_prev_str.startswith('12') and dt_curr_str.startswith('01') and year_log == year_file:
            # 日志 message 时间只有“月日时间”，如果是跨年日志，会出现上一条日志是12月，下一条是1月，这种情况年份+1
            year_file += 1
            print('跨年日志')
        dt_prev_str = dt_curr_str
        dt_curr = parse_log_line_dt(year_file, dt_curr_str)

        # region 解析日志内容
        # 解析日志内容 ======================================================================================
        # 客户端 build 版本号
        if not build and '(Build-' in log_line_1 and log_line_1.endswith(')'):
            build = 'Build-' + log_line_1.split('(Build-')[1][:-1]
            continue
        # 开始客户端初始化
        if '[MenuSceneLoader] <StartAsync> frameCount' in log_line_1:
            log_blocks.append(LogBlockInfo(StageName='开始初始化客户端', DtStart=dt_curr))
            continue
        # 开始登录流程
        if '[TrailBlazer] <StartAsync> manualSelectWorld=' in log_line_1:
            log_blocks.append(LogBlockInfo(StageName='登录开始', DtStart=dt_curr))
            continue
        # recreate 弹出弹窗
        match_interruption_request = re.search(PAT_RECREATE_INTR_REQUEST, log_line_1)
        if match_interruption_request:
            inter = match_interruption_request.group('interruption')
            log_blocks.append(LogBlockInfo(StageName='登录弹窗', StageDesc=inter, DtStart=dt_curr, ThreadStart=thread))
            continue
        # recreate 关闭弹窗
        match_interruption_drop = re.search(PAT_RECREATE_INTR_DROP, log_line_1)
        if match_interruption_drop:
            inter = match_interruption_drop.group('interruption')
            for i, block in enumerate(log_blocks):
                if block.StageName != '登录弹窗' or block.StageDesc != inter or block.DtEnd:
                    continue
                block.DtEnd = dt_curr
                block.ThreadEnd = thread
                log_blocks[i] = block
                break
            continue
        # 进入主菜单
        if '[StaticGameInfo] <ActiveSceneChanged>' in log_line_1 and 'new=HallScene' in log_line_1:
            log_blocks.append(LogBlockInfo(StageName='进入主菜单', DtStart=dt_curr))
            continue

        # 收到幻数返回
        if '[MagicCodeRetriever](GetEncryptedMagicCode) s is null:' in log_line_1:
            log_blocks.append(LogBlockInfo(StageName='登录请求', StageDesc='取得幻数', DtStart=dt_log, DtEnd=dt_curr))
            continue

        # 获取上次登录地区 开始
        if '[LoginRegionManager] <GetLastLoginRgnFromDbNoOverseaData> start' in log_line_1:
            log_blocks.append(LogBlockInfo(StageName='登录', StageDesc='获取地区', DtStart=dt_curr, ThreadStart=thread))
            continue
        # 获取上次登录地区 结束
        if '[LoginRegionManager] <GetLastLoginRgnFromDbNoOverseaData> cn: ' in log_line_1:
            for idx, block in enumerate(log_blocks):
                if block.StageName == '登录' and block.StageDesc == '获取地区':
                    block.DtEnd = dt_curr
                    block.ThreadEnd = thread
                    log_blocks[idx] = block
                    break
            continue

        # 网络请求
        is_guid = 'Guid =' in log_line_1

        # 网络请求 发起
        match_guid_uri = re.search(PAT_GUID_URI, log_line_1)
        if match_guid_uri:
            guid = match_guid_uri.group('guid')
            uri = match_guid_uri.group('uri')
            uri = (
                '外区_幻数' if '/init_code' in uri or 'mageUs' in uri
                else '国区_幻数' if 'mage' in uri or 'mageCn' in uri
                else uri
            )

            stage = ''
            if (
                '幻数' in uri
                or uri.endswith('/login-rec') or uri.endswith('/config-center')
                or uri.endswith('/login') or uri.endswith('/login-alert')
                or uri.endswith('/mystery')
            ):
                stage = '登录请求'
            elif uri.endswith('/account-info'):
                for line in log.Trace:
                    if '.LoginManagerAsync.' in line:
                        stage = '登录请求'
                        break
            stage = stage if stage else '网络请求'

            log_block_info = LogBlockInfo(
                StageName=stage,
                DtStart=dt_curr,
                ThreadStart=thread,
                GUID=guid,
            )
            log_block_info.RequestUri = uri
            log_blocks.append(log_block_info)
            continue
        elif is_guid and 'uri =' in log_line_1:
            print('日志包含 uri 关键词，但是无法被解析', PAT_GUID_URI, log.Msg, sep='\n\t')

        # 网络请求 返回
        match_guid_code = re.search(PAT_GUID_RES, log_line_1)
        if match_guid_code:
            guid = match_guid_code.group('guid')
            for i, block in enumerate(log_blocks):
                # 不检查在日志生成前发起的网络请求
                if block.GUID != guid:
                    continue
                block.DtEnd = dt_curr
                block.ThreadEnd = thread
                if 'full content is : ' in log_line_1:
                    block.RequestResp = log_line_1.split('full content is : ')[-1].strip()
                    # print(f'has full content {log_line_1}')
                elif 'Request finished Successfully, but the server sent an error' in log_line_1:
                    block.RequestResp = log_line_1.split('message = ')[-1].strip()
                    block.LambdaRespCode = log_line_1.split('http code = ')[-1].split(',')[0]
                    block.LambdaRespMessage = log_line_1.split('message = ')[-1]
                else:
                    block.RequestResp = log_line_1
                log_blocks[i] = block
                break
            continue
        elif is_guid and 'code =' in log_line_1:
            print('日志包含 code 关键词，但是无法被解析', PAT_GUID_RES, log.Msg, sep='\n\t')

        # 网络请求 超时
        match_guid_timeout = re.search(PAT_GUID_TIMEOUT, log_line_1)
        if match_guid_timeout:
            guid = match_guid_timeout.group('guid')
            for i, block in enumerate(log_blocks):
                block = log_blocks[i]
                if block.GUID != guid:
                    continue
                block.DtEnd = dt_curr
                block.ThreadEnd = thread
                log_blocks[i] = block
                break
            continue
        elif is_guid and 'time out' in log_line_1:
            print('日志包含 time out 关键词，但是无法被解析', PAT_GUID_RES, log.Msg, sep='\n\t')

        # 网络请求 未知情况
        if is_guid:
            print('未处理的网络请求', log.Msg)

    # 补全日志块需要计算的信息
    for i, block in enumerate(log_blocks):
        block.Build = build
        block.LogDt = dt_log.isoformat()[:-7]
        # block.MsStart = int((block.DtStart - dt_log).total_seconds() * 1000) if block.DtStart else block.MsStart
        # block.MsEnd = int((block.DtEnd - dt_log).total_seconds() * 1000) if block.DtEnd else block.MsEnd
        # block.MsDuration = block.MsEnd - block.MsStart if block.DtStart and block.DtEnd else 1
        block.MsStart = round(Decimal((block.DtStart - dt_log).total_seconds()), 3) if block.DtStart else block.MsStart
        block.MsEnd = round(Decimal((block.DtEnd - dt_log).total_seconds()), 3) if block.DtEnd else block.MsEnd
        block.MsDuration = block.MsEnd - block.MsStart if block.MsStart and block.MsEnd else Decimal('0.001')
        log_blocks[i] = block

    # file_path_out = f'{DATA_PATH_OUTPUT}/{build}_{file_name.split(".")[0]}.csv'
    # write_output_file_stage(file_path_out, log_blocks)
    file_path_out = f'{DATA_PATH_OUTPUT}/{build}_{file_name.split(".")[0]}_GUID.csv'
    if DATA_PATH not in file_path:
        file_path_out = f'{os.path.dirname(file_path)}/{build}_{file_name.split(".")[0]}_GUID.csv'
    write_output_file_GUID(file_path_out, log_blocks)


def handle_input_dir():
    input_files = os.listdir('./data/input')
    print('处理文件列表：', '\n'.join(input_files), sep='\n')
    for file in input_files:
        path = f'{DATA_PATH_INPUT}/{file}'
        handle_1_file(path)
        move_input_file(path)


def main():
    os.chdir(os.path.dirname(__file__))
    if not os.path.exists(DATA_PATH_COMPLETED):
        os.mkdir(DATA_PATH_COMPLETED)
    if not os.path.exists(DATA_PATH_INPUT):
        os.mkdir(DATA_PATH_INPUT)
    if not os.path.exists(DATA_PATH_OUTPUT):
        os.mkdir(DATA_PATH_OUTPUT)

    handle_1_file(r'')

    # files = [
    #     line.strip() for line in r'''
    #     '''.split('\n') if line.strip()
    # ]
    # # handle_1_file(f'{DATA_PATH_INPUT}/{files[0]}')
    # for file in files:
    #     handle_1_file(file)
    # return

    # handle_input_dir()
    return


if __name__ == '__main__':
    main()
