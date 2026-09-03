"""CloudWatch Alarms 页面 UI。

布局：
  工具栏：标题 | 账号 | 地区 | 状态过滤 | 刷新
  搜索框
  表格（告警名 | 当前状态 | 更新时间 | Metric | Namespace）
  操作栏：状态选择 + 原因输入 + 设置按钮
  状态栏
"""

from __future__ import annotations

import logging
import threading
import webbrowser
from datetime import datetime, timezone
from typing import Optional

from PyQt5.QtCore import (
    QEvent,
    QModelIndex,
    QObject,
    QRunnable,
    QSortFilterProxyModel,
    QThreadPool,
    Qt,
    QTimer,
    pyqtSignal,
    pyqtSlot,
)
from PyQt5.QtGui import QColor, QFont, QPalette, QStandardItem, QStandardItemModel
from PyQt5.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QTableView,
    QTextBrowser,
    QVBoxLayout,
    QWidget,
)

from CloudWatch.cloud_watch_alarm_dataclass import AlarmInfo, AlarmStateValue
from CloudWatch.cloud_watch_alarm_helper import fetch_alarms, set_alarm_states_batch
from services.mfa.profile_parser import MfaProfile, ProfileStatus, RegionGroup, parse_profiles
from utils.rate_limiter import Cancelled

_log = logging.getLogger(__name__)

# 列索引
COL_NAME = 0
COL_STATE = 1
COL_UPDATED = 2
COL_METRIC = 3
COL_NAMESPACE = 4

URL_ROLE = Qt.UserRole + 1

_HEADERS = ["告警名", "当前状态", "更新时间 (UTC)", "Metric", "Namespace"]

_CLICK_DELAY_MS = 250

_STATE_COLORS = {
    AlarmStateValue.OK: QColor("#22c55e"),
    AlarmStateValue.ALARM: QColor("#ef4444"),
    AlarmStateValue.INSUFFICIENT_DATA: QColor("#9ca3af"),
}

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

# ── 状态过滤下拉：显示名 → AlarmStateValue|None ────────────
_STATE_FILTER_CHOICES: list[tuple[str, Optional[AlarmStateValue]]] = [
    ("全部", None),
    ("ALARM", AlarmStateValue.ALARM),
    ("OK", AlarmStateValue.OK),
    ("INSUFFICIENT_DATA", AlarmStateValue.INSUFFICIENT_DATA),
]


def _region_choices(profile: MfaProfile) -> list[tuple[str, str]]:
    return _CN_REGIONS if profile.region_group == RegionGroup.CN else _US_REGIONS


def _fmt_dt(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_age(fetched_at: datetime) -> str:
    delta = int((datetime.now(timezone.utc) - fetched_at).total_seconds())
    if delta < 60:
        return "刚刚"
    if delta < 3600:
        return f"{delta // 60} 分钟前"
    if delta < 86400:
        return f"{delta // 3600} 小时前"
    return f"{delta // 86400} 天前"


# ── Delegates ────────────────────────────────────────────

class _LinkDelegate(QStyledItemDelegate):
    """告警名列 —— 超链接样式（红色 + 下划线）。"""
    _LINK_COLOR = QColor("#e8371b")

    def initStyleOption(self, option: QStyleOptionViewItem, index: QModelIndex):
        super().initStyleOption(option, index)
        if index.column() == COL_NAME:
            option.palette.setColor(QPalette.Text, self._LINK_COLOR)
            f = QFont(option.font)
            f.setUnderline(True)
            option.font = f


# ── 后台 workers ─────────────────────────────────────────

class _CancellableWorker(QRunnable):
    def __init__(self):
        super().__init__()
        self._stop = threading.Event()

    def cancel(self):
        self._stop.set()

    def _emit(self, emitter):
        if self._stop.is_set():
            return
        try:
            emitter()
        except RuntimeError:
            _log.debug("signal target destroyed, emit skipped")


class _ListSignals(QObject):
    # (alarms, error_message)
    finished = pyqtSignal(list, str)


class FetchAlarmsWorker(_CancellableWorker):
    def __init__(self, profile_name: str, region: str,
                 state_filter: Optional[AlarmStateValue]):
        super().__init__()
        self._profile_name = profile_name
        self._region = region
        self._state_filter = state_filter
        self.signals = _ListSignals()

    @pyqtSlot()
    def run(self):
        try:
            alarms = fetch_alarms(
                self._profile_name, self._region,
                state_value=self._state_filter,
                cancel=self._stop,
            )
        except Cancelled:
            return
        except Exception as exc:
            self._emit(lambda: self.signals.finished.emit([], str(exc)))
            return
        if self._stop.is_set():
            return
        self._emit(lambda: self.signals.finished.emit(alarms, ""))


class _SetStateSignals(QObject):
    # [(alarm_name, error_or_None), ...]
    finished = pyqtSignal(list)


class SetAlarmStateWorker(_CancellableWorker):
    def __init__(self, profile_name: str, region: str,
                 alarm_names: list[str], state: AlarmStateValue, reason: str):
        super().__init__()
        self._profile_name = profile_name
        self._region = region
        self._names = alarm_names
        self._state = state
        self._reason = reason
        self.signals = _SetStateSignals()

    @pyqtSlot()
    def run(self):
        try:
            results = set_alarm_states_batch(
                self._profile_name, self._region,
                self._names, self._state, self._reason,
                cancel=self._stop,
            )
        except Exception as exc:
            results = [(n, str(exc)) for n in self._names]
        if self._stop.is_set():
            return
        self._emit(lambda: self.signals.finished.emit(results))


# ── Widget ───────────────────────────────────────────────

class CloudWatchAlarmWidget(QWidget):
    """CloudWatch Alarms 列出与修改状态。"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._all_alarms: list[AlarmInfo] = []
        self._alarm_map: dict[str, AlarmInfo] = {}
        self._fetched_at: Optional[datetime] = None
        self._thread_pool = QThreadPool()

        self._generation = 0
        self._list_worker: Optional[FetchAlarmsWorker] = None
        self._set_worker: Optional[SetAlarmStateWorker] = None

        self._profile_map: dict[str, MfaProfile] = {}

        self._click_timer = QTimer(self)
        self._click_timer.setSingleShot(True)
        self._click_timer.setInterval(_CLICK_DELAY_MS)
        self._pending_click_url: Optional[str] = None

        # ── UI 构建 ─────────────────────────────────────
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        # 顶部工具栏
        toolbar = QHBoxLayout()
        toolbar.setSpacing(10)

        title = QLabel("CloudWatch Alarms")
        title.setObjectName("cwHeaderTitle")

        self._info_btn = QPushButton("\U0001F6C8")  # 🛈
        self._info_btn.setObjectName("cwInfoBtn")
        self._info_btn.setFlat(True)
        self._info_btn.setStyleSheet(
            "QPushButton#cwInfoBtn { border: none; background: transparent; "
            "padding: 0; font-size: 24px; color: #6b7280; }"
            "QPushButton#cwInfoBtn:hover { color: #111827; }"
        )
        self._info_btn.setFixedSize(32, 32)
        self._info_btn.setCursor(Qt.PointingHandCursor)
        self._info_btn.setToolTip("查看操作说明")

        self._account_combo = QComboBox()
        self._account_combo.setObjectName("cwAccountCombo")
        self._account_combo.setMinimumWidth(200)
        self._account_combo.setToolTip("选择已激活 MFA 的账号")

        self._region_combo = QComboBox()
        self._region_combo.setObjectName("cwRegionCombo")
        self._region_combo.setMinimumWidth(210)

        self._state_filter_combo = QComboBox()
        self._state_filter_combo.setObjectName("cwStateFilterCombo")
        self._state_filter_combo.setMinimumWidth(160)
        self._state_filter_combo.setToolTip("按告警当前状态过滤")
        for label, val in _STATE_FILTER_CHOICES:
            self._state_filter_combo.addItem(label, userData=val)

        self._refresh_btn = QPushButton("刷新")
        self._refresh_btn.setObjectName("cwRefreshBtn")

        toolbar.addWidget(title)
        toolbar.addWidget(self._info_btn)
        toolbar.addStretch()
        toolbar.addWidget(QLabel("账号:"))
        toolbar.addWidget(self._account_combo)
        toolbar.addWidget(QLabel("地区:"))
        toolbar.addWidget(self._region_combo)
        toolbar.addWidget(QLabel("状态:"))
        toolbar.addWidget(self._state_filter_combo)
        toolbar.addWidget(self._refresh_btn)
        root.addLayout(toolbar)

        # 搜索框
        search_row = QHBoxLayout()
        search_row.setSpacing(8)
        self._search_edit = QLineEdit()
        self._search_edit.setObjectName("cwSearchEdit")
        self._search_edit.setPlaceholderText("输入关键字过滤告警名 / Metric / Namespace...")
        self._search_edit.setClearButtonEnabled(True)
        search_row.addWidget(QLabel("搜索:"))
        search_row.addWidget(self._search_edit)
        root.addLayout(search_row)

        # 表格
        self._model = QStandardItemModel(0, len(_HEADERS), self)
        self._model.setHorizontalHeaderLabels(_HEADERS)

        self._proxy = QSortFilterProxyModel(self)
        self._proxy.setSourceModel(self._model)
        self._proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)
        # -1 表示对所有列做过滤，命中任一列即保留
        self._proxy.setFilterKeyColumn(-1)

        table_card = QFrame()
        table_card.setObjectName("cwTableCard")
        card_layout = QVBoxLayout(table_card)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(0)

        self._table = QTableView()
        self._table.setObjectName("cwTable")
        self._table.setModel(self._proxy)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._table.setAlternatingRowColors(False)
        self._table.setShowGrid(False)
        self._table.setSortingEnabled(True)
        self._table.verticalHeader().setVisible(False)
        self._table.setWordWrap(False)
        self._table.setMouseTracking(True)
        self._table.setFrameShape(QFrame.NoFrame)
        self._table.verticalHeader().setDefaultSectionSize(44)
        self._table.setItemDelegateForColumn(COL_NAME, _LinkDelegate(self._table))

        hdr = self._table.horizontalHeader()
        hdr.setSectionResizeMode(COL_NAME, QHeaderView.Stretch)
        hdr.setSectionResizeMode(COL_STATE, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(COL_UPDATED, QHeaderView.ResizeToContents)
        # Metric / Namespace 用固定初始宽度 + 可拖，避免长命名空间挤扁名字列
        hdr.setSectionResizeMode(COL_METRIC, QHeaderView.Interactive)
        hdr.setSectionResizeMode(COL_NAMESPACE, QHeaderView.Interactive)
        self._table.setColumnWidth(COL_METRIC, 200)
        self._table.setColumnWidth(COL_NAMESPACE, 200)
        # 保证名字列总能拿到足够空间
        hdr.setMinimumSectionSize(80)
        hdr.setStretchLastSection(False)
        self._table.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        hdr.setHighlightSections(False)

        self._proxy.sort(COL_NAME, Qt.AscendingOrder)

        card_layout.addWidget(self._table)
        root.addWidget(table_card, stretch=1)

        # 操作栏：目标状态 + Reason + 设置按钮
        action_row = QHBoxLayout()
        action_row.setSpacing(8)
        self._target_state_combo = QComboBox()
        self._target_state_combo.setObjectName("cwTargetStateCombo")
        self._target_state_combo.setMinimumWidth(180)
        for s in (AlarmStateValue.OK, AlarmStateValue.ALARM, AlarmStateValue.INSUFFICIENT_DATA):
            self._target_state_combo.addItem(s.value, userData=s)

        self._reason_edit = QLineEdit()
        self._reason_edit.setObjectName("cwReasonEdit")
        self._reason_edit.setPlaceholderText("StateReason（AWS 要求 1~1023 字符）")
        self._reason_edit.setText("Manually set from AwsTools")

        self._set_state_btn = QPushButton("设置状态")
        self._set_state_btn.setObjectName("cwSetStateBtn")
        self._set_state_btn.setToolTip("将所有已选中的告警设为上方状态")

        action_row.addWidget(QLabel("目标状态:"))
        action_row.addWidget(self._target_state_combo)
        action_row.addWidget(QLabel("原因:"))
        action_row.addWidget(self._reason_edit, 1)
        action_row.addWidget(self._set_state_btn)
        root.addLayout(action_row)

        # 状态栏
        self._status_label = QLabel("")
        self._status_label.setObjectName("cwStatusLabel")
        root.addWidget(self._status_label)

        # 定时器：刷新"N分钟前"
        self._age_timer = QTimer(self)
        self._age_timer.setInterval(60_000)
        self._age_timer.timeout.connect(self._refresh_status_count)

        # ── 信号连接 ──────────────────────────────────
        self._account_combo.currentIndexChanged.connect(self._on_account_changed)
        self._region_combo.currentIndexChanged.connect(self._on_region_changed)
        self._state_filter_combo.currentIndexChanged.connect(self._on_state_filter_changed)
        self._refresh_btn.clicked.connect(self._on_refresh_clicked)
        self._info_btn.clicked.connect(self._on_info_clicked)
        self._set_state_btn.clicked.connect(self._on_set_state_clicked)
        self._search_edit.textChanged.connect(self._proxy.setFilterFixedString)
        self._table.clicked.connect(self._on_table_clicked)
        self._table.doubleClicked.connect(self._on_table_double_clicked)
        self._click_timer.timeout.connect(self._on_click_timer_fired)
        self._table.viewport().installEventFilter(self)

        # 初始化
        self._populate_accounts()

    # ── 生命周期 ─────────────────────────────────────

    def showEvent(self, event):
        super().showEvent(event)
        self._populate_accounts()

    def shutdown(self):
        """由 MainWindow.closeEvent 调用，取消后台任务再等待线程池。"""
        self._cancel_pending()
        self._age_timer.stop()
        self._click_timer.stop()
        self._thread_pool.waitForDone(3000)

    def _cancel_pending(self):
        for w in (self._list_worker, self._set_worker):
            if w is not None:
                w.cancel()
        self._list_worker = None
        self._set_worker = None

    # ── 事件过滤（鼠标悬停光标） ────────────────────

    def eventFilter(self, obj, event):
        if obj is self._table.viewport() and event.type() == QEvent.MouseMove:
            index = self._table.indexAt(event.pos())
            if index.isValid() and index.column() == COL_NAME:
                self._table.viewport().setCursor(Qt.PointingHandCursor)
            else:
                self._table.viewport().setCursor(Qt.ArrowCursor)
        return super().eventFilter(obj, event)

    # ── 账号 / 地区下拉 ─────────────────────────────

    def _populate_accounts(self):
        self._account_combo.blockSignals(True)
        self._region_combo.blockSignals(True)

        prev = self._account_combo.currentData()
        self._account_combo.clear()

        try:
            profiles = parse_profiles()
        except Exception as exc:
            self._set_status(f"读取 AWS profile 失败: {exc}", error=True)
            self._account_combo.blockSignals(False)
            self._region_combo.blockSignals(False)
            return

        active = [p for p in profiles if p.status == ProfileStatus.ACTIVE]
        if not active:
            self._account_combo.addItem("（无已激活账号）")
            self._account_combo.setEnabled(False)
            self._region_combo.setEnabled(False)
            self._refresh_btn.setEnabled(False)
            self._set_state_btn.setEnabled(False)
            self._set_status("没有已激活的 MFA 账号，请先在 MFA 管理 Tab 中激活。", error=True)
            self._account_combo.blockSignals(False)
            self._region_combo.blockSignals(False)
            return

        self._profile_map = {p.session_name: p for p in active}
        self._account_combo.setEnabled(True)
        self._region_combo.setEnabled(True)
        self._refresh_btn.setEnabled(True)
        self._set_state_btn.setEnabled(True)

        restore_idx = 0
        for i, p in enumerate(active):
            self._account_combo.addItem(p.session_name, userData=p.session_name)
            if p.session_name == prev:
                restore_idx = i
        self._account_combo.setCurrentIndex(restore_idx)
        self._account_combo.blockSignals(False)
        self._region_combo.blockSignals(False)

        self._refresh_region_combo()

    def _refresh_region_combo(self):
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

    # ── 当前选中值 ──────────────────────────────────

    def _current_profile(self) -> Optional[MfaProfile]:
        name = self._account_combo.currentData()
        return self._profile_map.get(name) if name else None

    def _current_profile_name(self) -> Optional[str]:
        return self._account_combo.currentData()

    def _current_region(self) -> Optional[str]:
        return self._region_combo.currentData()

    def _current_state_filter(self) -> Optional[AlarmStateValue]:
        return self._state_filter_combo.currentData()

    # ── 拉取入口 ────────────────────────────────────

    def _load_for_current(self):
        profile_name = self._current_profile_name()
        region = self._current_region()
        if not profile_name or not region:
            return

        self._generation += 1
        self._cancel_pending()

        self._set_status("正在从 AWS 拉取告警...")
        self._model.removeRows(0, self._model.rowCount())
        self._refresh_btn.setEnabled(False)
        self._set_state_btn.setEnabled(False)

        gen = self._generation
        worker = FetchAlarmsWorker(profile_name, region, self._current_state_filter())
        worker.signals.finished.connect(
            lambda alarms, err, _g=gen: self._on_fetch_finished(alarms, err, _g)
        )
        self._list_worker = worker
        self._thread_pool.start(worker)

    # ── 事件处理 ────────────────────────────────────

    def _on_account_changed(self, _i: int):
        self._refresh_region_combo()

    def _on_region_changed(self, _i: int):
        self._load_for_current()

    def _on_state_filter_changed(self, _i: int):
        self._load_for_current()

    def _on_refresh_clicked(self):
        self._load_for_current()

    def _on_info_clicked(self):
        """展示操作说明。"""
        html = (
            "<h3 style='margin-top:0'>如何获取告警状态</h3>"
            "<ul>"
            "<li>顶部选择 <b>账号</b>、<b>地区</b> 与 <b>状态</b>，表格会自动拉取符合筛选的 Metric Alarms</li>"
            "<li>点击 <b>刷新</b> 按钮可按照当前筛选将表格内容更新至最新</li>"
            "<li>可用 <b>搜索框</b> 按名字 / Metric / Namespace 进行本地过滤</li>"
            "</ul>"
            "<h3 style='margin-top:0'>如何设置告警状态</h3>"
            "<ol>"
            "<li>在表格中选中目标告警行（支持 <b>Ctrl / Shift 多选</b>）</li>"
            "<li>底部操作栏：<b>目标状态</b> 选 OK / ALARM / INSUFFICIENT_DATA</li>"
            "<li>底部操作栏：<b>原因</b> 填 <code>StateReason</code>（AWS 必填，1~1023 字符，默认预填了一段）</li>"
            "<li>点 <b>设置状态</b> → 确认框确认后后台批量调用 <code>cloudwatch:SetAlarmState</code>，完成后自动刷新列表</li>"
            "</ol>"
            '<p><b>顶部&quot;状态&quot;与底部&quot;目标状态&quot;的区别：</b></p>'
            "<ul>"
            "<li>顶部 <b>状态</b> = <i>看谁</i>——服务端过滤 <code>describe_alarms</code> 结果</li>"
            "<li>底部 <b>目标状态</b> = <i>改成谁</i>——给选中告警下发的新状态</li>"
            "</ul>"
            "<p style='color:#6b7280'>注意：<code>SetAlarmState</code> 是<b>临时</b>改状态，"
            "下一次 metric 评估到来时告警会按规则重新计算，短时间内看到状态“弹回”是正常的。</p>"
        )
        dlg = QDialog(self)
        dlg.setWindowTitle("操作说明")
        dlg.resize(740, 460)

        layout = QVBoxLayout(dlg)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        browser = QTextBrowser(dlg)
        browser.setOpenExternalLinks(True)
        browser.setStyleSheet(
            "QTextBrowser { font-size: 15px; line-height: 1.6; background: transparent; border: 0px; padding: 12px; }"
        )
        browser.setHtml(html)
        layout.addWidget(browser, 1)

        # 复刻 cwOpenCacheBtn 的已知能生效的 Text Button 配方（无 border）
        close_btn = QPushButton("关闭", dlg)
        close_btn.setObjectName("cwDialogCloseBtn")
        close_btn.setCursor(Qt.PointingHandCursor)
        close_btn.clicked.connect(dlg.reject)

        buttons_row = QHBoxLayout()
        buttons_row.setContentsMargins(0, 0, 20, 20)
        buttons_row.addStretch()
        buttons_row.addWidget(close_btn)
        layout.addLayout(buttons_row)

        dlg.exec_()

    # ── 单击/双击 ────────────────────────────────────

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
        url = self._pending_click_url
        self._pending_click_url = None
        if url:
            webbrowser.open(url)

    # ── 拉取结果 ────────────────────────────────────

    def _on_fetch_finished(self, alarms: list[AlarmInfo], error: str, gen: int):
        if gen != self._generation:
            return

        self._list_worker = None
        self._refresh_btn.setEnabled(True)
        self._set_state_btn.setEnabled(True)

        if error:
            self._set_status(f"拉取失败: {error}", error=True)
            return

        self._show_data(alarms)

    def _show_data(self, alarms: list[AlarmInfo]):
        self._all_alarms = alarms
        self._alarm_map = {a.alarm_name: a for a in alarms}
        self._fetched_at = datetime.now(timezone.utc)
        self._populate_table(alarms)
        self._refresh_status_count()
        self._age_timer.start()

    def _populate_table(self, alarms: list[AlarmInfo]):
        self._model.removeRows(0, self._model.rowCount())

        for a in alarms:
            name_item = QStandardItem(a.alarm_name)
            name_item.setData(a.console_url, URL_ROLE)
            tip = a.alarm_name
            if a.state_reason:
                tip += f"\n\nStateReason: {a.state_reason}"
            name_item.setToolTip(tip)

            state_item = QStandardItem(a.state_value.value)
            state_item.setTextAlignment(Qt.AlignCenter)
            state_item.setForeground(_STATE_COLORS.get(a.state_value, QColor("#111827")))
            f = QFont(state_item.font())
            f.setBold(True)
            state_item.setFont(f)

            updated_item = QStandardItem(_fmt_dt(a.state_updated_timestamp))
            updated_item.setTextAlignment(Qt.AlignCenter)

            metric_item = QStandardItem(a.metric_name or "")
            namespace_item = QStandardItem(a.namespace or "")

            self._model.appendRow([name_item, state_item, updated_item, metric_item, namespace_item])

    # ── 设置状态 ────────────────────────────────────

    def _selected_alarm_names(self) -> list[str]:
        rows = self._table.selectionModel().selectedRows()
        names: list[str] = []
        for proxy_idx in rows:
            src = self._proxy.mapToSource(proxy_idx)
            item = self._model.item(src.row(), COL_NAME)
            if item is not None:
                names.append(item.text())
        return names

    def _on_set_state_clicked(self):
        profile_name = self._current_profile_name()
        region = self._current_region()
        if not profile_name or not region:
            return

        names = self._selected_alarm_names()
        if not names:
            QMessageBox.information(self, "未选择", "请先在表格中选择至少一个告警。")
            return

        target = self._target_state_combo.currentData()
        if target is None:
            return

        reason = self._reason_edit.text().strip()
        if not reason:
            # 弹窗要一个
            reason, ok = QInputDialog.getText(
                self, "State Reason",
                "StateReason 必填（1~1023 字符）:",
                text="Manually set from AwsTools",
            )
            reason = (reason or "").strip()
            if not ok or not reason:
                return
            self._reason_edit.setText(reason)

        # 二次确认
        confirm = QMessageBox.question(
            self, "确认",
            f"将 {len(names)} 个告警设为 {target.value}？\n\n"
            f"（示例）{names[0]}"
            + ("\n..." if len(names) > 1 else ""),
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return

        self._generation += 1
        if self._set_worker is not None:
            self._set_worker.cancel()
        self._set_state_btn.setEnabled(False)
        self._set_status(f"正在将 {len(names)} 个告警设为 {target.value}...")

        gen = self._generation
        worker = SetAlarmStateWorker(profile_name, region, names, target, reason)
        worker.signals.finished.connect(
            lambda results, _g=gen: self._on_set_state_finished(results, _g)
        )
        self._set_worker = worker
        self._thread_pool.start(worker)

    def _on_set_state_finished(self, results: list[tuple[str, Optional[str]]], gen: int):
        if gen != self._generation:
            return
        self._set_worker = None
        self._set_state_btn.setEnabled(True)

        ok = [n for n, err in results if err is None]
        failed = [(n, err) for n, err in results if err is not None]

        if not failed:
            self._set_status(f"已成功设置 {len(ok)} 个告警状态，正在刷新...")
        else:
            preview = "\n".join(f"- {n}: {err}" for n, err in failed[:5])
            more = f"\n... 还有 {len(failed) - 5} 个" if len(failed) > 5 else ""
            QMessageBox.warning(
                self, "部分失败",
                f"成功 {len(ok)} 个，失败 {len(failed)} 个：\n{preview}{more}",
            )
            self._set_status(f"完成：成功 {len(ok)} / 失败 {len(failed)}，正在刷新...", error=True)

        # 状态改完后刷新列表以看到最新状态（AWS 内部会重新评估，稍后可能又回到 OK/ALARM）
        self._load_for_current()

    # ── 状态栏 ──────────────────────────────────────

    def _set_status(self, text: str, error: bool = False):
        self._status_label.setText(text)
        self._status_label.setProperty("error", "true" if error else "false")
        self._status_label.style().polish(self._status_label)

    def _refresh_status_count(self):
        count = self._proxy.rowCount()
        total = self._model.rowCount()
        count_part = f"共 {total} 个" if count == total else f"显示 {count} / {total} 个"
        if self._fetched_at:
            self._set_status(f"{count_part} 告警   |   上次拉取: {_fmt_age(self._fetched_at)}")
        else:
            self._set_status(f"{count_part} 告警")
