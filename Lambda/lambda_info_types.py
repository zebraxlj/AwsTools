from dataclasses import dataclass, fields
from typing import ClassVar, Optional

from utils.TablePrinter.table_printer import BaseRow, BaseTable, ColumnAlignment, ColumnConfig, CondFmtExactMatch


@dataclass
class Function:
    """
    用于解析 list_functions 返回的 Functions 字段，返回结构见文档：
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/list_functions.html
    """
    FunctionName: str
    FunctionArn: str
    Runtime: Optional[str] = None
    Role: Optional[str] = None
    Handler: Optional[str] = None
    CodeSize: Optional[int] = None
    Description: Optional[str] = None
    Timeout: Optional[int] = None
    MemorySize: Optional[int] = None
    LastModified: Optional[str] = None
    CodeSha256: Optional[str] = None
    Version: Optional[str] = None
    Environment: Optional[dict] = None
    Layers: Optional[dict] = None
    PackageType: Optional[str] = None
    Architectures: Optional[list] = None
    EphemeralStorage: Optional[dict] = None

    @classmethod
    def from_dict(cls, dict_data: dict):
        valid_fields = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in dict_data.items() if k in valid_fields}
        return cls(**filtered_data)

    def get_region(self) -> str:
        """
        从 FunctionArn 中提取地区信息
        :return: 地区字符串
        """
        if not self.FunctionArn:
            return 'NA'
        return self.FunctionArn.split(':lambda:')[1].split(':')[0]


@dataclass
class FunctionRow(BaseRow):
    FunctionName: str = 'NA'
    __FunctionName_config: ClassVar[ColumnConfig] = ColumnConfig(align=ColumnAlignment.LEFT)
    FunctionName_href: str = 'NA'
    Region: str = 'NA'
    __Region_config: ClassVar[ColumnConfig] = ColumnConfig(alias='地区')
    Timeout: Optional[int] = None
    __Timeout_config: ClassVar[ColumnConfig] = ColumnConfig(alias='超时')
    MemorySize: Optional[int] = None
    __MemorySize_config: ClassVar[ColumnConfig] = ColumnConfig(alias='内存')
    ConcurrencySetting: str = 'NA'
    __ConcurrencySetting_config: ClassVar[ColumnConfig] = ColumnConfig(
        alias='并发设置', conditional_format=CondFmtExactMatch(match_target='Throttled')
    )
    __Throttled_config: ClassVar[ColumnConfig] = ColumnConfig(conditional_format=CondFmtExactMatch(match_target=True))
    LastDeployDt: str = 'NA'
    __LastDeployDt_config: ClassVar[ColumnConfig] = ColumnConfig(alias='最后部署时间')


class FunctionTable(BaseTable):
    row_type = FunctionRow
