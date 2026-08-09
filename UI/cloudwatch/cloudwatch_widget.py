"""
CloudWatch Log Groups 标签页 UI。

布局：
  第一行工具栏：标题 | 账号下拉 | 地区下拉 | 刷新按钮
  搜索框
  表格（LogGroupName（超链接） | 创建时间 | LastIngestionTime）
  状态栏

缓存策略：
  有缓存（profile + region 为 key）→ 直接展示，状态栏显示上次拉取时间
  无缓存 → 自动触发拉取
  点"刷新" → 强制重新拉取
"""

from __future__ import annotations

import webbrowser
from datetime import datetime, timezone
from typing import Optional

from PyQt5.QtCore import (
    QEvent,
    QModelIndex,
    QSortFilterProxyModel,
    QThreadPool,
    Qt,
    QTimer,
)
from PyQt5.QtGui import (
    QColor,
    QFont,
    QPalette,
    QStandardItem,
    QStandardItemModel,
)
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from services.cloudwatch.log_groups_cache import load_cache, open_cache_dir, save_cache
from services.cloudwatch.log_groups_service import LogGroupInfo
from services.mfa.profile_parser import MfaProfile, ProfileStatus, RegionGroup, parse_profiles
from UI.cloudwatch.cloudwatch_worker import FetchIngestionTimeWorker, FetchLogGroupsWorker

# 列索引常量
COL_NAME = 0
COL_CREATED = 1
COL_LAST_INGESTION = 2

# 名称列中存储 URL 的 role
URL_ROLE = Qt.UserRole + 1

_HEADERS = ["LogGroup 名称", "创建时间 (UTC)", "最后写入 (UTC)"]

# 单击延迟（ms）：等待双击是否触发，防止单击+双击同时生效
_CLICK_DELAY_MS = 250

# 按 region group 划分的候选地区列表，格式：(显示名, region id)
_CN_REGIONS = [
    ("宁夏 (cn-northwest-1)", "cn-northwest-1"),
    ("北京 (cn-north-1)",     "cn-north-1"),
]
_US_REGIONS = [
    ("美东 (us-east-1)",        "us-east-1"),
    ("美西 (us-west-2)",        "us-west-2"),
    ("欧洲 (eu-central-1)",     "eu-central-1"),
    ("亚太东京 (ap-northeast-1)", "ap-northeast-1"),
    ("亚太首尔 (ap-northeast-2)", "ap-northeast-2"),
    ("亚太新加坡 (ap-southeast-1)", "ap-southeast-1"),
    ("亚太悉尼 (ap-southeast-2)", "ap-southeast-2"),
    ("南美 (sa-east-1)",        "sa-east-1"),
]


def _region_choices(profile: MfaProfile) -> list[tuple[str, str]]:
    """根据 profile 所属区域组，返回对应的地区列表。"""
    return _CN_REGIONS if profile.region_group == RegionGroup.CN else _US_REGIONS


def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _fmt_age(fetched_at: datetime) -> str:
    delta = int((datetime.now(timezone.utc) - fetched_at).total_seconds())
    if delta < 60:
        return "刚刚"
    if delta < 3600:
        return f"{delta // 60} 分钟前"
    if delta < 86400:
        return f"{delta // 3600} 小时前"
    return f"{delta // 86400} 天前"


class _LinkDelegate(QStyledItemDelegate):
    """为名称列渲染超链接样式（CloudWatch 红 + 下划线）"""

    _LINK_COLOR = QColor("#e8371b")

    def initStyleOption(self, option: QStyleOptionViewItem, index: QModelIndex):
        super().initStyleOption(option, index)
        if index.column() == COL_NAME:
            option.palette.setColor(QPalette.Text, self._LINK_COLOR)
            f = QFont(option.font)
            f.setUnderline(True)
            option.font = f


class CloudWatchWidget(QWidget):
    """CloudWatch Log Groups 查询标签页"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._all_groups: list[LogGroupInfo] = []
        self._group_map: dict[str, LogGroupInfo] = {}  # name → LogGroupInfo 索引
        self._fetched_at: Optional[datetime] = None
        self._thread_pool = QThreadPool()

        # generation counter：每次切换账号/地区/刷新时 +1，worker 回调时核对
        self._generation = 0

        # profile_name → MfaProfile，用于填地区列表
        self._profile_map: dict[str, MfaProfile] = {}
        # log_group_name → QStandardItem (最后写入列)，供第二阶段逐条回填
        self._ingestion_items: dict[str, QStandardItem] = {}

        # 单击延迟定时器（防止单击+双击同时触发）
        self._click_timer = QTimer(self)
        self._click_timer.setSingleShot(True)
        self._click_timer.setInterval(_CLICK_DELAY_MS)
        self._pending_click_url: Optional[str] = None

        # ── 构建界面 ──────────────────────────────────────
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        # 顶部工具栏：标题 | 账号 | 地区 | 刷新
        toolbar = QHBoxLayout()
        toolbar.setSpacing(10)

        title = QLabel("CloudWatch Log Groups")
        title.setObjectName("cwHeaderTitle")

        self._account_combo = QComboBox()
        self._account_combo.setObjectName("cwAccountCombo")
        self._account_combo.setMinimumWidth(200)
        self._account_combo.setToolTip("选择已激活 MFA 的账号")

        self._region_combo = QComboBox()
        self._region_combo.setObjectName("cwRegionCombo")
        self._region_combo.setMinimumWidth(210)
        self._region_combo.setToolTip("选择要查询的 AWS 地区")

        self._refresh_btn = QPushButton("刷新")
        self._refresh_btn.setObjectName("cwRefreshBtn")
        self._refresh_btn.setToolTip("强制重新从 AWS 拉取最新数据（忽略缓存）")

        toolbar.addWidget(title)
        toolbar.addStretch()
        toolbar.addWidget(QLabel("账号:"))
        toolbar.addWidget(self._account_combo)
        toolbar.addWidget(QLabel("地区:"))
        toolbar.addWidget(self._region_combo)
        toolbar.addWidget(self._refresh_btn)
        root.addLayout(toolbar)

        # 搜索框
        search_row = QHBoxLayout()
        search_row.setSpacing(8)
        self._search_edit = QLineEdit()
        self._search_edit.setObjectName("cwSearchEdit")
        self._search_edit.setPlaceholderText("输入关键字过滤日志组名称...")
        self._search_edit.setClearButtonEnabled(True)
        search_row.addWidget(QLabel("搜索:"))
        search_row.addWidget(self._search_edit)
        root.addLayout(search_row)

        # 表格 — 用 QFrame 卡片包裹，实现 MD3 Data Table 风格
        self._model = QStandardItemModel(0, len(_HEADERS), self)
        self._model.setHorizontalHeaderLabels(_HEADERS)

        self._proxy = QSortFilterProxyModel(self)
        self._proxy.setSourceModel(self._model)
        self._proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self._proxy.setFilterKeyColumn(COL_NAME)

        table_card = QFrame()
        table_card.setObjectName("cwTableCard")
        card_layout = QVBoxLayout(table_card)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(0)

        self._table = QTableView()
        self._table.setObjectName("cwTable")
        self._table.setModel(self._proxy)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.SingleSelection)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.setAlternatingRowColors(False)
        self._table.setShowGrid(False)
        self._table.setSortingEnabled(True)
        self._table.verticalHeader().setVisible(False)
        self._table.setWordWrap(False)
        self._table.setMouseTracking(True)
        self._table.setFrameShape(QFrame.NoFrame)

        self._table.verticalHeader().setDefaultSectionSize(48)

        self._table.setItemDelegateForColumn(COL_NAME, _LinkDelegate(self._table))

        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(COL_NAME, QHeaderView.Stretch)
        hdr.setSectionResizeMode(COL_CREATED, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(COL_LAST_INGESTION, QHeaderView.ResizeToContents)
        hdr.setHighlightSections(False)

        self._proxy.sort(COL_NAME, Qt.AscendingOrder)

        card_layout.addWidget(self._table)
        root.addWidget(table_card, stretch=1)

        # 状态栏
        bottom_row = QHBoxLayout()
        bottom_row.setSpacing(8)
        self._status_label = QLabel("")
        self._status_label.setObjectName("cwStatusLabel")
        self._open_cache_btn = QPushButton("打开缓存目录")
        self._open_cache_btn.setObjectName("cwOpenCacheBtn")
        self._open_cache_btn.setToolTip("在文件管理器中打开本地缓存所在文件夹")
        bottom_row.addWidget(self._status_label, stretch=1)
        bottom_row.addWidget(self._open_cache_btn)
        root.addLayout(bottom_row)

        # 每分钟刷新"N分钟前"文字
        self._age_timer = QTimer(self)
        self._age_timer.setInterval(60_000)
        self._age_timer.timeout.connect(self._refresh_status_count)

        # ── 信号连接 ──────────────────────────────────────
        self._account_combo.currentIndexChanged.connect(self._on_account_changed)
        self._region_combo.currentIndexChanged.connect(self._on_region_changed)
        self._refresh_btn.clicked.connect(self._on_refresh_clicked)
        self._open_cache_btn.clicked.connect(lambda: open_cache_dir())
        self._search_edit.textChanged.connect(self._proxy.setFilterFixedString)
        self._table.clicked.connect(self._on_table_clicked)
        self._table.doubleClicked.connect(self._on_table_double_clicked)
        self._click_timer.timeout.connect(self._on_click_timer_fired)
        self._table.viewport().installEventFilter(self)

        # ── 初始化 ────────────────────────────────────────
        self._populate_accounts()

    # ── #17: tab 切换时刷新账号列表 ───────────────────────

    def showEvent(self, event):
        super().showEvent(event)
        self._populate_accounts()

    # ── 事件过滤（鼠标悬停光标） ─────────────────────────

    def eventFilter(self, obj, event):
        if obj is self._table.viewport() and event.type() == QEvent.MouseMove:
            index = self._table.indexAt(event.pos())
            if index.isValid() and index.column() == COL_NAME:
                self._table.viewport().setCursor(Qt.PointingHandCursor)
            else:
                self._table.viewport().setCursor(Qt.ArrowCursor)
        return super().eventFilter(obj, event)

    # ── 账号下拉 ─────────────────────────────────────────

    def _populate_accounts(self):
        """解析本地 AWS profile，将已激活（ACTIVE）账号填入下拉框"""
        self._account_combo.blockSignals(True)
        self._region_combo.blockSignals(True)

        prev_selection = self._account_combo.currentData()
        self._account_combo.clear()

        try:
            profiles = parse_profiles()
        except Exception as exc:
            self._set_status(f"读取 AWS profile 失败: {exc}", error=True)
            self._account_combo.blockSignals(False)
            self._region_combo.blockSignals(False)
            return

        active_profiles = [p for p in profiles if p.status == ProfileStatus.ACTIVE]

        if not active_profiles:
            self._account_combo.addItem("（无已激活账号）")
            self._account_combo.setEnabled(False)
            self._region_combo.setEnabled(False)
            self._refresh_btn.setEnabled(False)
            self._set_status("没有已激活的 MFA 账号，请先在 MFA 管理 Tab 中激活。", error=True)
            self._account_combo.blockSignals(False)
            self._region_combo.blockSignals(False)
            return

        self._profile_map = {p.session_name: p for p in active_profiles}
        self._account_combo.setEnabled(True)
        self._region_combo.setEnabled(True)
        self._refresh_btn.setEnabled(True)

        restore_idx = 0
        for i, p in enumerate(active_profiles):
            self._account_combo.addItem(p.session_name, userData=p.session_name)
            if p.session_name == prev_selection:
                restore_idx = i

        self._account_combo.setCurrentIndex(restore_idx)
        self._account_combo.blockSignals(False)
        self._region_combo.blockSignals(False)

        # 触发地区列表填充，再触发数据加载
        self._refresh_region_combo()

    # ── 地区下拉 ─────────────────────────────────────────

    def _refresh_region_combo(self):
        """根据当前选中账号，重新填充地区下拉框，默认选中 profile 自带的 region。"""
        profile = self._current_profile()
        if profile is None:
            return

        self._region_combo.blockSignals(True)
        self._region_combo.clear()

        choices = _region_choices(profile)
        default_region = profile.region

        default_idx = 0
        for i, (label, region_id) in enumerate(choices):
            self._region_combo.addItem(label, userData=region_id)
            if region_id == default_region:
                default_idx = i

        self._region_combo.setCurrentIndex(default_idx)
        self._region_combo.blockSignals(False)

        self._load_for_current()

    # ── 当前选中值 ───────────────────────────────────────

    def _current_profile(self) -> Optional[MfaProfile]:
        name = self._account_combo.currentData()
        return self._profile_map.get(name) if name else None

    def _current_profile_name(self) -> Optional[str]:
        return self._account_combo.currentData()

    def _current_region(self) -> Optional[str]:
        return self._region_combo.currentData()

    # ── 缓存 + 拉取入口 ──────────────────────────────────

    def _load_for_current(self, force_fetch: bool = False):
        profile_name = self._current_profile_name()
        region = self._current_region()
        if not profile_name or not region:
            return

        # 递增 generation，使旧 worker 的回调失效
        self._generation += 1

        if not force_fetch:
            groups, fetched_at = load_cache(profile_name, region)
            if groups:
                self._show_data(groups, fetched_at)
                return
            self._set_status("无本地缓存，正在从 AWS 拉取...")
        else:
            self._set_status("正在从 AWS 拉取...")

        self._model.removeRows(0, self._model.rowCount())
        self._refresh_btn.setEnabled(False)
        self._account_combo.setEnabled(False)
        self._region_combo.setEnabled(False)

        gen = self._generation
        worker = FetchLogGroupsWorker(profile_name, region)
        worker.signals.finished.connect(
            lambda groups, ts, err, _g=gen: self._on_fetch_finished(groups, ts, err, _g)
        )
        self._thread_pool.start(worker)

    # ── 事件处理 ─────────────────────────────────────────

    def _on_account_changed(self, _index: int):
        self._refresh_region_combo()

    def _on_region_changed(self, _index: int):
        self._load_for_current(force_fetch=False)

    def _on_refresh_clicked(self):
        self._load_for_current(force_fetch=True)

    # ── #9: 单击/双击互斥 ─────────────────────────────────
    # 单击名称列 → 延迟后打开浏览器；双击名称列 → 取消单击、复制名称

    def _on_table_clicked(self, proxy_index: QModelIndex):
        if proxy_index.column() != COL_NAME:
            return
        source_index = self._proxy.mapToSource(proxy_index)
        item = self._model.item(source_index.row(), COL_NAME)
        if item:
            url = item.data(URL_ROLE)
            if url:
                self._pending_click_url = url
                self._click_timer.start()

    def _on_table_double_clicked(self, proxy_index: QModelIndex):
        # 取消待执行的单击（打开浏览器）
        self._click_timer.stop()
        self._pending_click_url = None

        if proxy_index.column() != COL_NAME:
            return
        source_index = self._proxy.mapToSource(proxy_index)
        item = self._model.item(source_index.row(), COL_NAME)
        if item:
            QApplication.clipboard().setText(item.text())
            self._set_status(f"已复制: {item.text()}")
            QTimer.singleShot(3000, self._refresh_status_count)

    def _on_click_timer_fired(self):
        """单击延迟到期，确认不是双击，执行打开浏览器"""
        url = self._pending_click_url
        self._pending_click_url = None
        if url:
            webbrowser.open(url)

    # ── 第一阶段：日志组列表 ────────────────────────────────

    def _on_fetch_finished(
        self, groups: list[LogGroupInfo], fetched_at: Optional[datetime], error: str,
        gen: int,
    ):
        # 检查 generation：若不匹配说明用户已切换，丢弃旧结果
        if gen != self._generation:
            return

        self._refresh_btn.setEnabled(True)
        self._account_combo.setEnabled(True)
        self._region_combo.setEnabled(True)

        if error:
            self._set_status(f"拉取失败: {error}", error=True)
            return

        self._show_data(groups, fetched_at)

    def _show_data(self, groups: list[LogGroupInfo], fetched_at: Optional[datetime]):
        self._all_groups = groups
        self._group_map = {g.log_group_name: g for g in groups}
        self._fetched_at = fetched_at
        self._populate_table(groups)
        self._refresh_status_count()
        self._age_timer.start()
        # 启动第二阶段：后台补充 lastIngestionTime
        self._start_ingestion_fetch(groups)

    def _populate_table(self, groups: list[LogGroupInfo]):
        self._model.removeRows(0, self._model.rowCount())
        self._ingestion_items.clear()

        for g in groups:
            name_item = QStandardItem(g.log_group_name)
            name_item.setData(g.console_url, URL_ROLE)
            name_item.setToolTip(f"{g.log_group_name}\n{g.console_url}")

            created_item = QStandardItem(_fmt_dt(g.creation_time))
            created_item.setTextAlignment(Qt.AlignCenter)

            ingestion_text = _fmt_dt(g.last_ingestion_time) if g.last_ingestion_time else ""
            ingestion_item = QStandardItem(ingestion_text)
            ingestion_item.setTextAlignment(Qt.AlignCenter)

            self._ingestion_items[g.log_group_name] = ingestion_item
            self._model.appendRow([name_item, created_item, ingestion_item])

    # ── 第二阶段：后台补充 lastIngestionTime ──────────────

    def _start_ingestion_fetch(self, groups: list[LogGroupInfo]):
        missing = [g.log_group_name for g in groups if g.last_ingestion_time is None]
        if not missing:
            return

        profile_name = self._current_profile_name()
        region = self._current_region()
        if not profile_name or not region:
            return

        gen = self._generation
        worker = FetchIngestionTimeWorker(profile_name, region, missing)
        worker.signals.item_ready.connect(
            lambda name, ts, _g=gen: self._on_ingestion_item_ready(name, ts, _g)
        )
        worker.signals.all_done.connect(
            lambda _g=gen: self._on_ingestion_all_done(_g)
        )
        self._thread_pool.start(worker)

    def _on_ingestion_item_ready(self, log_group_name: str, ts: Optional[datetime], gen: int):
        if gen != self._generation:
            return

        item = self._ingestion_items.get(log_group_name)
        if item is None:
            return
        item.setText(_fmt_dt(ts) if ts is not None else "无")

        # O(1) 更新内存数据
        g = self._group_map.get(log_group_name)
        if g is not None:
            g.last_ingestion_time = ts

    def _on_ingestion_all_done(self, gen: int):
        if gen != self._generation:
            return
        profile_name = self._current_profile_name()
        region = self._current_region()
        if profile_name and region and self._all_groups:
            save_cache(profile_name, region, self._all_groups)

    # ── 状态栏 ───────────────────────────────────────────

    def _set_status(self, text: str, error: bool = False):
        self._status_label.setText(text)
        self._status_label.setProperty("error", "true" if error else "false")
        self._status_label.style().polish(self._status_label)

    def _refresh_status_count(self):
        count = self._proxy.rowCount()
        total = self._model.rowCount()
        count_part = f"共 {total} 个" if count == total else f"显示 {count} / {total} 个"
        if self._fetched_at:
            self._set_status(f"{count_part} 日志组   |   上次拉取: {_fmt_age(self._fetched_at)}")
        else:
            self._set_status(f"{count_part} 日志组")
