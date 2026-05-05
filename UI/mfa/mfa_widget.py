"""
MFA 标签页 Widget：纯 UI 布局和渲染，不包含业务逻辑。
所有数据操作通过 MfaController 完成。
"""

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QScrollArea, QFrame,
)

from UI.mfa.mfa_controller import MfaController, GroupedProfiles
from UI.mfa.profile_card import ProfileCard
from UI.styles import (
    HEADER_STYLE, RELOAD_BTN_STYLE,
    TOGGLE_BTN_ON_STYLE, TOGGLE_BTN_OFF_STYLE,
    GROUP_HEADER_STYLE, SEPARATOR_STYLE,
)


class MfaWidget(QWidget):
    """MFA 管理标签页，可嵌入 QTabWidget 或作为独立 dialog"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._cards: dict[str, ProfileCard] = {}  # session_name -> ProfileCard
        self._controller = MfaController(self)

        self._build_ui()
        self._connect_signals()
        self._controller.load_profiles()
        self._start_timer()

    # ── UI 构建 ──────────────────────────────────────────

    def _build_ui(self):
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(16, 16, 16, 16)
        root_layout.setSpacing(12)

        # 顶部栏
        header_layout = QHBoxLayout()

        title = QLabel("AWS MFA Manager")
        title.setStyleSheet(HEADER_STYLE)
        header_layout.addWidget(title)

        header_layout.addStretch()

        self.reload_btn = QPushButton("↻ 重新加载")
        self.reload_btn.setStyleSheet(RELOAD_BTN_STYLE)
        self.reload_btn.setCursor(Qt.PointingHandCursor)
        header_layout.addWidget(self.reload_btn)

        self.toggle_btn = QPushButton("☆ 仅收藏")
        self.toggle_btn.setCursor(Qt.PointingHandCursor)
        header_layout.addWidget(self.toggle_btn)

        root_layout.addLayout(header_layout)

        # 滚动区域
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        self.scroll_content = QWidget()
        self.scroll_layout = QVBoxLayout(self.scroll_content)
        self.scroll_layout.setContentsMargins(0, 0, 0, 0)
        self.scroll_layout.setSpacing(8)
        self.scroll_layout.addStretch()

        scroll.setWidget(self.scroll_content)
        root_layout.addWidget(scroll)

    def _connect_signals(self):
        """连接 controller 信号和 UI 事件"""
        # controller → widget
        self._controller.profiles_changed.connect(self._on_profiles_changed)
        self._controller.mfa_result.connect(self._on_mfa_result)

        # UI 事件 → controller
        self.reload_btn.clicked.connect(self._controller.load_profiles)
        self.toggle_btn.clicked.connect(self._controller.toggle_starred_filter)

    # ── 渲染 ─────────────────────────────────────────────

    def _on_profiles_changed(self, data: GroupedProfiles, show_starred_only: bool):
        """controller 通知数据变化，重建卡片列表"""
        self._update_toggle_btn(show_starred_only)
        self._rebuild_cards(data)

    def _rebuild_cards(self, data: GroupedProfiles):
        """根据分组数据重建所有卡片"""
        self._clear_scroll_layout()
        self._cards.clear()

        for group_label, items in data.groups:
            # 分组标题
            label = QLabel(group_label)
            label.setStyleSheet(GROUP_HEADER_STYLE)
            self.scroll_layout.addWidget(label)

            for profile, is_starred in items:
                card = ProfileCard(profile, is_starred)
                card.star_toggled.connect(self._controller.toggle_profile_star)
                card.mfa_submitted.connect(self._controller.submit_mfa)
                self.scroll_layout.addWidget(card)
                self._cards[profile.session_name] = card

            # 分隔线
            sep = QFrame()
            sep.setFrameShape(QFrame.HLine)
            sep.setStyleSheet(SEPARATOR_STYLE)
            self.scroll_layout.addWidget(sep)

        self.scroll_layout.addStretch()

    def _clear_scroll_layout(self):
        while self.scroll_layout.count() > 0:
            item = self.scroll_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

    def _update_toggle_btn(self, show_starred_only: bool):
        if show_starred_only:
            self.toggle_btn.setText("⭐ 仅收藏")
            self.toggle_btn.setStyleSheet(TOGGLE_BTN_ON_STYLE)
        else:
            self.toggle_btn.setText("☆ 仅收藏")
            self.toggle_btn.setStyleSheet(TOGGLE_BTN_OFF_STYLE)

    # ── MFA 结果 ─────────────────────────────────────────

    def _on_mfa_result(self, profile_name: str, success: bool, error_message: str):
        """controller 通知 MFA 验证结果，更新对应卡片"""
        card = self._cards.get(profile_name)
        if card is None:
            return

        if success:
            # 数据已被 controller 更新，刷新卡片显示
            profile = self._controller.get_profile(profile_name)
            if profile:
                card.update_profile(profile)

        card.on_mfa_result(success, error_message)

    # ── 定时器 ───────────────────────────────────────────

    def _start_timer(self):
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._on_tick)
        self._timer.start(1000)

    def _on_tick(self):
        for card in self._cards.values():
            card.tick()
