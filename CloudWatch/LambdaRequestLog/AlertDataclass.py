from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from datetime import datetime
from typing import List


@dataclass_json
@dataclass
class LogDetail:
    """ 一行 Cloudwatch 日志 """
    date_time: datetime
    message: str
    url: str
    event_resp: dict = None
    id: str = None


@dataclass_json
@dataclass
class RequestDetail:
    """ 一个请求所包含的所有日志 """
    caller: str = None
    dt_start: datetime = None
    dt_end: datetime = None
    id: str = None
    req_body: str = None
    log_details: List[LogDetail] = field(default_factory=lambda: list())
    is_err_known: bool = None

    def __repr__(self):
        logs = '\n'.join(f'{d.date_time}|{d.message}' for d in self.log_details)
        return (f'{self.id} {self.dt_start} {self.dt_end}\n'
                f'{self.req_body}\n'
                f'{logs}')
