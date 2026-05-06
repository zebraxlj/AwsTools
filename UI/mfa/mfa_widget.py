"""
MFA 标签页 Widget：Material Design 风格。
区域（CN/US）为大卡片容器，Profile 为其中的子卡片。

本文件不调用任何 setStyleSheet —— 所有样式由 app.py 加载的 theme.qss 统一控制。
"""

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QScrollArea, QFrame,
    QGraphicsDropShadowEffect,
)
from PyQt5.QtGui import QColor

from UI.mfa.mfa_controller import MfaController, GroupedProfiles
from UI.mfa.profile_card import ProfileCard


def _make_elevation_shadow(elevation: int = 2) -> QGraphicsDropShadowEffect:
    """创建 Material Design elevation 阴影效果"""
    shadow = QGraphicsDropShadowEffect()
    shadow.setBlurRadius(elevation * 6)
    shadow.setOffset(0, elevation * 1.5)
    shadow.setColor(QColor(0, 0, 0, 30 + elevation * 8))
    return shadow


class RegionCard(QFrame):
    """区域大卡片容器（Surface）"""

    def __init__(self, label: str, parent=None):
        super().__init__(parent)
        self.setObjectName("regionCard")
        self.setGraphicsEffect(_make_elevation_shadow(2))

        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(20, 16, 20, 12)
        self._layout.setSpacing(0)

        header = QLabel(label)
        header.setObjectName("regionHeader")
        self._layout.addWidget(header)
        self._layout.addSpacing(8)

    def add_profile_card(self, card: QFrame, is_first: bool):
        if not is_first:
            divider = QFrame()
            divider.setObjectName("profileDivider")
            divider.setFrameShape(QFrame.HLine)
            divider.setFixedHeight(1)
            self._layout.addWidget(divider)
        self._layout.addWidget(card)


class MfaWidget(QWidget):
    """MFA 管理标签页"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._cards: dict[str, ProfileCard] = {}
        self._controller = MfaController(self)

        self._build_ui()
        self._connect_signals()
        self._controller.load_profiles()
        self._start_timer()

    # ── UI 构建 ──────────────────────────────────────────

    def _build_ui(self):
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(20, 16, 20, 16)
        root_layout.setSpacing(12)

        # 顶部工具栏（左右 4px 与滚动区域内的卡片对齐）
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(4, 0, 4, 0)
        header_layout.setSpacing(12)

        title = QLabel("AWS MFA Manager")
        title.setObjectName("headerTitle")
        header_layout.addWidget(title)

        header_layout.addStretch()

        self.reload_btn = QPushButton("↻ 重新加载")
        self.reload_btn.setObjectName("reloadBtn")
        self.reload_btn.setCursor(Qt.PointingHandCursor)
        header_layout.addWidget(self.reload_btn)

        self.toggle_btn = QPushButton("☆ 仅收藏")
        self.toggle_btn.setObjectName("toggleBtn")
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
        self.scroll_layout.setContentsMargins(4, 4, 4, 4)
        self.scroll_layout.setSpacing(16)
        self.scroll_layout.addStretch()

        scroll.setWidget(self.scroll_content)
        root_layout.addWidget(scroll)

    def _connect_signals(self):
        self._controller.profiles_changed.connect(self._on_profiles_changed)
        self._controller.mfa_result.connect(self._on_mfa_result)
        self.reload_btn.clicked.connect(self._controller.load_profiles)
        self.toggle_btn.clicked.connect(self._controller.toggle_starred_filter)

    # ── 渲染 ─────────────────────────────────────────────

    def _on_profiles_changed(self, data: GroupedProfiles, show_starred_only: bool):
        self._update_toggle_btn(show_starred_only)
        self._rebuild_cards(data)

    def _rebuild_cards(self, data: GroupedProfiles):
        self._clear_scroll_layout()
        self._cards.clear()

        for group_label, items in data.groups:
            region_card = RegionCard(group_label)

            for i, (profile, is_starred) in enumerate(items):
                card = ProfileCard(profile, is_starred)
                card.star_toggled.connect(self._controller.toggle_profile_star)
                card.mfa_submitted.connect(self._controller.submit_mfa)
                region_card.add_profile_card(card, is_first=(i == 0))
                self._cards[profile.session_name] = card

            self.scroll_layout.addWidget(region_card)

        self.scroll_layout.addStretch()

    def _clear_scroll_layout(self):
        while self.scroll_layout.count() > 0:
            item = self.scroll_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

    def _update_toggle_btn(self, show_starred_only: bool):
        """通过 Qt dynamic property 切换样式，不调用 setStyleSheet"""
        if show_starred_only:
            self.toggle_btn.setText("⭐ 仅收藏")
            self.toggle_btn.setProperty("active", True)
        else:
            self.toggle_btn.setText("☆ 仅收藏")
            self.toggle_btn.setProperty("active", False)

        # 通知 Qt 重新匹配样式规则
        self.toggle_btn.style().unpolish(self.toggle_btn)
        self.toggle_btn.style().polish(self.toggle_btn)
        self.toggle_btn.update()

    # ── MFA 结果 ─────────────────────────────────────────

    def _on_mfa_result(self, profile_name: str, success: bool, error_message: str):
        card = self._cards.get(profile_name)
        if card is None:
            return

        if success:
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
