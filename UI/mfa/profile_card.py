"""
Profile 卡片组件：展示单个 MFA profile 的状态和交互控件。
"""

from PyQt5.QtCore import Qt, pyqtSignal, QRegExp
from PyQt5.QtGui import QRegExpValidator
from PyQt5.QtWidgets import (
    QFrame, QHBoxLayout, QVBoxLayout, QLabel, QPushButton,
    QLineEdit, QWidget,
)

from services.mfa.profile_parser import MfaProfile, ProfileStatus
from UI.styles import (
    CARD_STYLE_ACTIVE, CARD_STYLE_EXPIRED,
    STAR_BTN_STYLE, PROFILE_NAME_STYLE, DETAIL_LABEL_STYLE,
    TIMER_LABEL_STYLE_ACTIVE, TIMER_LABEL_STYLE_WARNING,
    STATUS_EXPIRED_STYLE, REFRESH_BTN_STYLE,
    MFA_LABEL_STYLE, MFA_INPUT_STYLE, SUBMIT_BTN_STYLE,
    TOAST_SUCCESS_STYLE, TOAST_ERROR_STYLE,
)


def _format_remaining(seconds: int) -> str:
    """将秒数格式化为 Xh Xm Xs"""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    elif m > 0:
        return f"{m}m {s:02d}s"
    else:
        return f"{s}s"


class ProfileCard(QFrame):
    """单个 Profile 的卡片组件"""

    # 信号
    star_toggled = pyqtSignal(str)          # profile_name
    mfa_submitted = pyqtSignal(str, str)    # (profile_name, mfa_code)

    def __init__(self, profile: MfaProfile, is_starred: bool, parent=None):
        super().__init__(parent)
        self.profile = profile
        self._is_starred = is_starred
        self._is_loading = False
        self._mfa_input_visible = False

        self.setObjectName("profileCard")
        self._build_ui()
        self._apply_status()

    # ── UI 构建 ──────────────────────────────────────────

    def _build_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(12, 10, 12, 10)
        main_layout.setSpacing(6)

        self._build_top_row(main_layout)
        self._build_detail_row(main_layout)
        self._build_action_row(main_layout)
        self._build_mfa_row(main_layout)

    def _build_top_row(self, parent_layout: QVBoxLayout):
        """第一行：星标 + Profile 名称 + 倒计时/过期状态"""
        top_row = QHBoxLayout()
        top_row.setSpacing(8)

        self.star_btn = QPushButton("⭐" if self._is_starred else "☆")
        self.star_btn.setStyleSheet(STAR_BTN_STYLE)
        self.star_btn.setCursor(Qt.PointingHandCursor)
        self.star_btn.setFixedSize(32, 32)
        self.star_btn.clicked.connect(self._on_star_clicked)
        top_row.addWidget(self.star_btn)

        self.name_label = QLabel(self.profile.display_name)
        self.name_label.setStyleSheet(PROFILE_NAME_STYLE)
        top_row.addWidget(self.name_label)

        top_row.addStretch()

        self.timer_label = QLabel("")
        self.timer_label.setStyleSheet(TIMER_LABEL_STYLE_ACTIVE)
        top_row.addWidget(self.timer_label)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet(STATUS_EXPIRED_STYLE)
        top_row.addWidget(self.status_label)

        parent_layout.addLayout(top_row)

    def _build_detail_row(self, parent_layout: QVBoxLayout):
        """第二行：详情（Region, Role, MFA device）"""
        detail_parts = []
        if self.profile.region:
            detail_parts.append(f"Region: {self.profile.region}")
        if self.profile.role_arn:
            role_short = self.profile.role_arn.rsplit("/", 1)[-1]
            detail_parts.append(f"Role: {role_short}")
        if self.profile.mfa_device:
            mfa_short = self.profile.mfa_device.rsplit("/", 1)[-1]
            detail_parts.append(f"MFA: {mfa_short}")

        if detail_parts:
            self.detail_label = QLabel("  |  ".join(detail_parts))
            self.detail_label.setStyleSheet(DETAIL_LABEL_STYLE)
            parent_layout.addWidget(self.detail_label)

    def _build_action_row(self, parent_layout: QVBoxLayout):
        """第三行：强制刷新按钮（仅未过期时显示）"""
        self.action_widget = QWidget()
        action_layout = QHBoxLayout(self.action_widget)
        action_layout.setContentsMargins(0, 4, 0, 0)
        action_layout.setSpacing(8)

        self.force_refresh_btn = QPushButton("强制刷新")
        self.force_refresh_btn.setStyleSheet(REFRESH_BTN_STYLE)
        self.force_refresh_btn.setCursor(Qt.PointingHandCursor)
        self.force_refresh_btn.clicked.connect(self._on_force_refresh)
        action_layout.addWidget(self.force_refresh_btn)

        action_layout.addStretch()
        parent_layout.addWidget(self.action_widget)

    def _build_mfa_row(self, parent_layout: QVBoxLayout):
        """MFA 输入行：标签 + 输入框 + 验证按钮 + toast"""
        self.mfa_widget = QWidget()
        mfa_layout = QHBoxLayout(self.mfa_widget)
        mfa_layout.setContentsMargins(0, 4, 0, 0)
        mfa_layout.setSpacing(8)

        mfa_label = QLabel("MFA Code:")
        mfa_label.setStyleSheet(MFA_LABEL_STYLE)
        mfa_layout.addWidget(mfa_label)

        self.mfa_input = QLineEdit()
        self.mfa_input.setStyleSheet(MFA_INPUT_STYLE)
        self.mfa_input.setPlaceholderText("000000")
        self.mfa_input.setMaxLength(6)
        self.mfa_input.setFixedWidth(120)
        self.mfa_input.setValidator(QRegExpValidator(QRegExp(r"\d{0,6}")))
        self.mfa_input.returnPressed.connect(self._on_submit)
        mfa_layout.addWidget(self.mfa_input)

        self.submit_btn = QPushButton("验证")
        self.submit_btn.setStyleSheet(SUBMIT_BTN_STYLE)
        self.submit_btn.setCursor(Qt.PointingHandCursor)
        self.submit_btn.clicked.connect(self._on_submit)
        mfa_layout.addWidget(self.submit_btn)

        self.toast_label = QLabel("")
        self.toast_label.setVisible(False)
        mfa_layout.addWidget(self.toast_label)

        mfa_layout.addStretch()
        parent_layout.addWidget(self.mfa_widget)

    # ── 状态更新 ─────────────────────────────────────────

    def _apply_status(self):
        """根据 profile 当前状态设置卡片外观和控件可见性"""
        status, remaining = self.profile.get_status_and_remaining()

        if status == ProfileStatus.ACTIVE:
            self.setStyleSheet(CARD_STYLE_ACTIVE)
            self.status_label.setVisible(False)
            self.timer_label.setVisible(True)
            self.action_widget.setVisible(True)
            self.mfa_widget.setVisible(self._mfa_input_visible)
            self._set_timer_text(remaining)
        else:
            self.setStyleSheet(CARD_STYLE_EXPIRED)
            self.timer_label.setVisible(False)
            self.status_label.setVisible(True)
            self.status_label.setText(
                "已过期" if status == ProfileStatus.EXPIRED else "未初始化"
            )
            self.action_widget.setVisible(False)
            self._mfa_input_visible = True
            self.mfa_widget.setVisible(True)

    def _set_timer_text(self, remaining: int):
        """设置倒计时文本和对应样式"""
        text = _format_remaining(remaining)
        style = TIMER_LABEL_STYLE_WARNING if remaining < 600 else TIMER_LABEL_STYLE_ACTIVE
        self.timer_label.setStyleSheet(style)
        self.timer_label.setText(f"⏱ {text}")

    def tick(self):
        """定时器每秒调用一次，更新倒计时"""
        status, remaining = self.profile.get_status_and_remaining()

        if status == ProfileStatus.ACTIVE:
            self._set_timer_text(remaining)
        elif not self._mfa_input_visible:
            self._apply_status()

    # ── 事件处理 ─────────────────────────────────────────

    def _on_star_clicked(self):
        self._is_starred = not self._is_starred
        self.star_btn.setText("⭐" if self._is_starred else "☆")
        self.star_toggled.emit(self.profile.session_name)

    def _on_force_refresh(self):
        self._mfa_input_visible = True
        self.mfa_widget.setVisible(True)
        self.mfa_input.setFocus()
        self.mfa_input.clear()
        self._clear_toast()

    def _on_submit(self):
        code = self.mfa_input.text().strip()
        if len(code) != 6 or not code.isdigit():
            self._show_toast("请输入6位数字验证码", is_error=True)
            return

        self._set_loading(True)
        self._clear_toast()
        self.mfa_submitted.emit(self.profile.session_name, code)

    def on_mfa_result(self, success: bool, error_message: str = ""):
        """STS 调用完成后的回调"""
        self._set_loading(False)

        if success:
            self._show_toast("验证成功！凭证已更新", is_error=False)
            self._mfa_input_visible = False
            self.mfa_input.clear()
            self._apply_status()
        else:
            self._show_toast(f"验证失败: {error_message}", is_error=True)

    # ── 内部辅助 ─────────────────────────────────────────

    def _set_loading(self, loading: bool):
        self._is_loading = loading
        self.submit_btn.setEnabled(not loading)
        self.mfa_input.setEnabled(not loading)
        self.submit_btn.setText("验证中..." if loading else "验证")

    def _show_toast(self, message: str, is_error: bool):
        self.toast_label.setText(message)
        self.toast_label.setStyleSheet(TOAST_ERROR_STYLE if is_error else TOAST_SUCCESS_STYLE)
        self.toast_label.setVisible(True)

    def _clear_toast(self):
        self.toast_label.setText("")
        self.toast_label.setVisible(False)

    @property
    def is_starred(self) -> bool:
        return self._is_starred

    def update_profile(self, profile: MfaProfile):
        """更新 profile 数据并刷新显示"""
        self.profile = profile
        self._apply_status()
