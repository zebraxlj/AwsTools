import csv
import logging
import os
import platform
import re
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from utils.aws_urls import gen_cloud_watch_log_stream_url1
from utils.logging_helper import setup_logging

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
__PROJ_DIR = os.path.dirname(__SCRIPT_DIR)
if __PROJ_DIR not in sys.path:
    sys.path.insert(0, __PROJ_DIR)

from CloudWatch.cloud_watch_dataclass import FilterLogEventsResp, CloudWatchRequest, GetLogEventsResp
from CloudWatch.cloud_watch_helper import filter_log_events, get_log_client, get_log_events
from utils.aws_consts import AllEnvs, Env

PAT_TIMEOUT_SPAN = re.compile(r'Task timed out after (?P<requestSpan>\d+(?:\.\d+)?) seconds')
PAT_REQUEST_ID = re.compile(r'\s(?P<requestId>[0-9a-fA-F-]{8,})\s+Task timed out after')

AWS_REGION = 'cn-northwest-1'
DT_START = datetime(2026, 2, 25, 15, 0)
DT_END = datetime(2026, 2, 25, 15, 5)
LOG_GROUP_NAME = '/aws/lambda/PartyAnimals--205890-GameAdminFunction'

# Data output directory (default to script directory)
DATA_DIR = __SCRIPT_DIR
pc_name = platform.node()
if pc_name in {'Source-XiaLijie'}:
    DATA_DIR = os.path.join(__PROJ_DIR, 'CloudWatch', 'Data', 'TimeoutRequestLog')


def _ensure_utc(dt: datetime, label: str) -> datetime:
    if dt.tzinfo is None:
        logging.warning('%s is naive; assuming UTC', label)
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def download_timeout_requests(
        aws_rgn: str, log_group: str, dt_end: datetime, dt_start: Optional[datetime] = None
) -> None:
    dt_end = _ensure_utc(dt_end, 'dt_end')
    dt_start = dt_end - timedelta(minutes=5) if dt_start is None else _ensure_utc(dt_start, 'dt_start')
    timeout_requests = get_timeout_requests(
        aws_rgn=aws_rgn, log_group=log_group,
        dt_start=dt_start,
        dt_end=dt_end,
    )
    fn_name = log_group.split('/')[-1]
    file_path = os.path.join(DATA_DIR, f'TimeoutRequests_{fn_name}_{dt_start.timestamp()}_{dt_end.timestamp()}.csv')
    save_to_csv(file_path=file_path, cloud_watch_requests=timeout_requests)


def get_timeout_requests(
        aws_rgn: str, log_group: str, dt_start: datetime, dt_end: datetime
) -> List[CloudWatchRequest]:
    dt_start = _ensure_utc(dt_start, 'dt_start')
    dt_end = _ensure_utc(dt_end, 'dt_end')
    env_name: str = log_group.split('/')[-1].split('-')[0]
    env: Env = AllEnvs.get_env_by_name(env_name)
    client = get_log_client(aws_rgn, env)

    # Fetch all timeout events in the window
    timeout_events_resp = filter_log_events(
        aws_region=aws_rgn, log_group_name=log_group,
        pattern='Task timed out after',
        dt_start=dt_start, dt_end=dt_end,
        is_stop_on_match=False,
        client=client,
    )
    timeout_event_objs: List[FilterLogEventsResp] = [FilterLogEventsResp(**e) for e in timeout_events_resp]

    timeout_requests: List[CloudWatchRequest] = []

    # Parse timeout events into request windows
    for e in timeout_event_objs:
        message = e.message.strip()
        span_match = PAT_TIMEOUT_SPAN.search(message)
        if not span_match:
            logging.warning('Skip event without timeout span: %s', message)
            continue

        request_span = float(span_match.group('requestSpan'))
        req_id_match = PAT_REQUEST_ID.search(message)
        request_id = req_id_match.group('requestId') if req_id_match else None
        if request_id is None:
            logging.warning('RequestId not found in timeout line: %s', message)

        dt_end_req = datetime.fromtimestamp(e.timestamp / 1000, tz=timezone.utc)
        dt_start_req = dt_end_req - timedelta(seconds=request_span)
        timeout_requests.append(CloudWatchRequest(
            log_stream=e.logStreamName,
            dt_start=dt_start_req, dt_end=dt_end_req, ts_end=e.timestamp,
            req_id=request_id, req_span=request_span,
            event_id=e.eventId,
            url=gen_cloud_watch_log_stream_url1(
                log_group=log_group, log_rgn=aws_rgn, log_stream=e.logStreamName,
                timestamp=int(dt_start_req.timestamp() * 1000), event_id=e.eventId,
            ),
        ))
    timeout_requests.sort(key=lambda e: e.ts_end)

    # Fetch full request logs by LogStream, RequestID, DtStart, DtEnd
    for i in range(len(timeout_requests)):
        req = timeout_requests[i]
        events_resp = get_log_events(
            client=client,
            logStreamName=req.log_stream,
            logGroupName=log_group,
            startTime=req.dt_start,
            endTime=req.dt_end + timedelta(milliseconds=1),  # get_log_events excludes endTime; add 1 ms
            startFromHead=True,
        )
        timeout_requests[i].req_logs = [GetLogEventsResp(**e) for e in events_resp]

    return timeout_requests


def save_to_csv(file_path: str, cloud_watch_requests: List[CloudWatchRequest]) -> None:
    rows = [
        {
            'timestamp': datetime.fromtimestamp(log_event.timestamp / 1000, tz=timezone.utc).isoformat(),
            'message': log_event.message.strip().replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' '),
            'logStream': req.log_stream,
            'url': req.url if 'Task timed out after' in log_event.message else '',
        }
        for req in cloud_watch_requests
        for log_event in req.req_logs
    ]
    if not rows:
        logging.warning('No data to save')
        return

    dir_path = os.path.dirname(file_path)
    if dir_path and not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    with open(file_path, 'w', newline='', encoding='utf8') as f_out:
        writer = csv.DictWriter(f_out, quotechar='"', fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    logging.info(f'File saved at {file_path}')


def main():
    setup_logging()
    download_timeout_requests(aws_rgn=AWS_REGION, log_group=LOG_GROUP_NAME, dt_end=DT_END, dt_start=DT_START)


if __name__ == '__main__':
    main()
