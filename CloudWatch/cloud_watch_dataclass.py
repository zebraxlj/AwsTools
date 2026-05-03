import dataclasses
from datetime import datetime
from enum import Enum
from typing import Optional, List


class StopReason(Enum):
    EMPTY_RESPONSE = 'empty_response'
    TOKEN_EXHAUSTED = 'token_exhausted'
    STOP_EVENT = 'stop_event'
    COMPLETED = 'completed'


@dataclasses.dataclass(init=False)
class Boto3CloudWatchResp:
    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


@dataclasses.dataclass(init=False)
class FilterLogEventsResp(Boto3CloudWatchResp):
    logStreamName: str
    timestamp: int
    message: str
    eventId: str


@dataclasses.dataclass(init=False)
class GetLogEventsResp(Boto3CloudWatchResp):
    timestamp: int
    message: str


@dataclasses.dataclass
class CloudWatchRequest:
    log_stream: str
    dt_start: Optional[datetime] = None
    dt_end: Optional[datetime] = None
    req_id: Optional[str] = None
    req_span: Optional[float] = None
    req_logs: Optional[List[GetLogEventsResp]] = None
    event_id: Optional[str] = None
    ts_end: Optional[int] = None
    url: Optional[str] = None


@dataclasses.dataclass
class FetchStats:
    """Statistics collected during a paginated log-fetching loop."""
    iterations: int = 0
    total_events: int = 0
    total_duration_ms: float = 0.0
    avg_iteration_ms: float = 0.0
    events_per_iteration: List[int] = dataclasses.field(default_factory=list)
    stopped_by: StopReason = StopReason.COMPLETED
