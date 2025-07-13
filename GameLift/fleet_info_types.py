from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar, List, Optional

from dataclasses_json import DataClassJsonMixin, dataclass_json

from GameLift.fleet_info_consts import DT_FMT_S
from utils.TablePrinter.table_printer import BaseRow, BaseTable, ColumnConfig, CondFmtContain, CondFmtExactMatch


@dataclass_json
@dataclass
class FleetAttribute(DataClassJsonMixin):
    FleetId: str
    FleetType: str
    Name: str
    CreationTime: datetime
    TerminationTime: datetime
    Status: str
    Region: Optional[str] = None


@dataclass_json
@dataclass
class FleetCapacity(DataClassJsonMixin):
    FleetId: str
    InstanceType: str
    InstanceCounts: dict
    Location: str
    LastCheckedDt: Optional[datetime] = None


@dataclass_json
@dataclass
class FleetLocationAttribute(DataClassJsonMixin):
    @dataclass_json
    @dataclass
    class FleetLocationState:
        Location: str = 'Unknown'
        Status: str = 'Unknown'

    LocationState: FleetLocationState = field(default_factory=FleetLocationState)
    StoppedActions: List = field(default_factory=list)


@dataclass_json
@dataclass
class FleetLocationCapacity(DataClassJsonMixin):
    FleetId: str
    InstanceType: str = 'Unknown'
    InstanceCounts: dict = field(default_factory=dict)
    Location: str = 'Unknown'


@dataclass
class EnvFleetStatusRow(BaseRow):
    SubEnv: int = -1
    __SubEnv_config: ClassVar[ColumnConfig] = ColumnConfig(alias='子环境', hide=True)
    Region: str = 'NA'
    __Region_config: ClassVar[ColumnConfig] = ColumnConfig(alias='地区')
    Name: str = field(default_factory=lambda: '无战斗服')
    __Name_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='Fleet名', conditional_format=CondFmtContain(contain_target='AWS MFA Expired')
    )
    CreateTime: str = 'NA'
    __CreateTime_config: ClassVar[ColumnConfig] = ColumnConfig(alias='Fleet创建时间')
    Status: str = 'NA'
    __Status_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='Fleet状态', conditional_format=CondFmtExactMatch(match_target='ERROR')
    )
    FleetType: str = 'NA'
    __FleetType_config: ClassVar[ColumnConfig] = ColumnConfig(alias='机群类型')
    InstanceType: str = 'NA'
    Minimum: int = -1
    __Minimum_config: ClassVar[ColumnConfig] = ColumnConfig(alias='Min')
    Maximum: int = -1
    __Maximum_config: ClassVar[ColumnConfig] = ColumnConfig(alias='Max')
    Desired: int = -1
    __Desired_config: ClassVar[ColumnConfig] = ColumnConfig(alias='所需')
    Pending: int = -1
    __Pending_config: ClassVar[ColumnConfig] = ColumnConfig(hide=True)
    Active: int = -1
    __Active_config: ClassVar[ColumnConfig] = ColumnConfig(alias='活跃')
    Idle: int = -1
    __Idle_config: ClassVar[ColumnConfig] = ColumnConfig(alias='空闲')
    Terminating: int = -1
    __Terminating_config: ClassVar[ColumnConfig] = ColumnConfig(hide=True)
    InstanceLocation: str = 'NA'
    __InstanceLocation_config: ClassVar[ColumnConfig] = ColumnConfig(alias='实例地区')
    LocationStatus: str = 'NA'
    __LocationStatus_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='地区状态', conditional_format=CondFmtExactMatch(match_target='ERROR')
    )
    FleetId: str = 'NA'
    __FleetId_config: ClassVar[ColumnConfig] = ColumnConfig(alias='FleetId(末段)')
    LastCheckedTime: Optional[str] = None  # 自动赋值
    __LastCheckedTime_config: ClassVar[ColumnConfig] = ColumnConfig(hide=True)
    LastCheckedDt: Optional[datetime] = None  # 自动赋值
    __LastCheckedDt_config: ClassVar[ColumnConfig] = ColumnConfig(alias='拉取时间', format='%H:%M:%S')
    Name_href: Optional[str] = None

    def __post_init__(self):
        if self.LastCheckedDt is not None:
            raise ValueError('不要给 EnvFleetStatusRow.LastCheckedDt 手动赋值')
        if self.LastCheckedTime is not None:
            raise ValueError('不要给 EnvFleetStatusRow.LastCheckedTime 手动赋值')
        dt_now = datetime.now()
        self.LastCheckedDt = dt_now
        self.LastCheckedTime = dt_now.strftime(DT_FMT_S)


class EnvFleetStatusTbl(BaseTable):
    row_type = EnvFleetStatusRow
