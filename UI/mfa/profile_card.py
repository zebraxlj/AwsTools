"""
Profile 子卡片组件：嵌入区域大卡片中的行项，Material Design 风格。

本文件不调用任何 setStyleSheet —— 所有样式由 app.py 加载的 theme.qss 统一控制。
动态样式切换通过修改 objectName + style().polish() 实现。
"""

import webbrowser

from PyQt5.QtCore import Qt, pyqtSignal, QRegExp
from PyQt5.QtGui import QRegExpValidator
from PyQt5.QtWidgets import (
    QFrame, QHBoxLayout, QVBoxLayout, QLabel, QPushButton,
    QLineEdit, QWidget, QApplication,
)

from services.mfa.profile_parser import MfaProfile, ProfileStatus
from utils.aws_urls import get_iam_role_url


def _format_remaining(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m:02d}m {s:02d}s"
    elif m > 0:
        return f"{m}m {s:02d}s"
    else:
        return f"{s}s"


def _swap_object_name(widget, new_name: str):
    """切换 objectName 并强制 Qt 重新匹配样式"""
    if widget.objectName() != new_name:
        widget.setObjectName(new_name)
        widget.style().unpolish(widget)
        widget.style().polish(widget)
        widget.update()


class ProfileCard(QFrame):
    """单个 Profile 子卡片，嵌入 RegionCard 中"""

    star_toggled = pyqtSignal(str)
    mfa_submitted = pyqtSignal(str, str)

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
        outer = QHBoxLayout(self)
        outer.setContentsMargins(4, 6, 8, 6)
        outer.setSpacing(12)

        # 左侧色条
        self.indicator = QFrame()
        self.indicator.setObjectName("indicatorActive")
        outer.addWidget(self.indicator)

        # 右侧内容
        content = QVBoxLayout()
        content.setContentsMargins(0, 0, 0, 0)
        content.setSpacing(4)
        self._build_top_row(content)
        self._build_detail_row(content)
        self._build_action_row(content)
        self._build_mfa_row(content)
        outer.addLayout(content, 1)

    def _build_top_row(self, layout: QVBoxLayout):
        row = QHBoxLayout()
        row.setSpacing(6)

        self.star_btn = QPushButton("⭐" if self._is_starred else "☆")
        self.star_btn.setObjectName("starBtn")
        self.star_btn.setCursor(Qt.PointingHandCursor)
        self.star_btn.setFixedSize(32, 32)
        self.star_btn.clicked.connect(self._on_star_clicked)
        row.addWidget(self.star_btn)

        self.name_label = QLabel(self.profile.display_name)
        self.name_label.setObjectName("profileName")
        row.addWidget(self.name_label)

        # IAM Role 链接按钮
        if self.profile.role_arn:
            self.link_btn = QPushButton("🔗")
            self.link_btn.setObjectName("linkBtn")
            self.link_btn.setCursor(Qt.PointingHandCursor)
            self.link_btn.setFixedSize(28, 28)
            self.link_btn.setToolTip(f"打开 IAM Role: {self.profile.role_arn.rsplit('/', 1)[-1]}")
            self.link_btn.clicked.connect(self._on_link_clicked)
            row.addWidget(self.link_btn)

        # 终端命令按钮（点击复制命令到剪贴板）
        self.cmd_btn = QPushButton(">_")
        self.cmd_btn.setObjectName("cmdBtn")
        self.cmd_btn.setCursor(Qt.PointingHandCursor)
        self.cmd_btn.setFixedSize(28, 28)
        self.cmd_btn.setToolTip(self._build_mfa_command())
        self.cmd_btn.clicked.connect(self._on_cmd_clicked)
        row.addWidget(self.cmd_btn)

        row.addStretch()

        self.timer_label = QLabel("")
        self.timer_label.setObjectName("timerLabelActive")
        row.addWidget(self.timer_label)

        self.status_label = QLabel("")
        self.status_label.setObjectName("statusLabel")
        row.addWidget(self.status_label)

        layout.addLayout(row)

    def _build_detail_row(self, layout: QVBoxLayout):
        parts = []
        if self.profile.region:
            parts.append(f"Region: {self.profile.region}")
        if self.profile.role_arn:
            parts.append(f"Role: {self.profile.role_arn.rsplit('/', 1)[-1]}")
        if self.profile.mfa_device:
            parts.append(f"MFA: {self.profile.mfa_device.rsplit('/', 1)[-1]}")
        if parts:
            lbl = QLabel("  ·  ".join(parts))
            lbl.setObjectName("detailLabel")
            layout.addWidget(lbl)

    def _build_action_row(self, layout: QVBoxLayout):
        self.action_widget = QWidget()
        row = QHBoxLayout(self.action_widget)
        row.setContentsMargins(0, 2, 0, 0)
        row.setSpacing(8)

        self.force_refresh_btn = QPushButton("强制刷新")
        self.force_refresh_btn.setObjectName("refreshBtn")
        self.force_refresh_btn.setCursor(Qt.PointingHandCursor)
        self.force_refresh_btn.clicked.connect(self._on_force_refresh)
        row.addWidget(self.force_refresh_btn)
        row.addStretch()

        layout.addWidget(self.action_widget)

    def _build_mfa_row(self, layout: QVBoxLayout):
        self.mfa_widget = QWidget()
        row = QHBoxLayout(self.mfa_widget)
        row.setContentsMargins(0, 2, 0, 0)
        row.setSpacing(8)

        lbl = QLabel("MFA Code:")
        lbl.setObjectName("mfaLabel")
        row.addWidget(lbl)

        self.mfa_input = QLineEdit()
        self.mfa_input.setObjectName("mfaInput")
        self.mfa_input.setPlaceholderText("000000")
        self.mfa_input.setMaxLength(6)
        self.mfa_input.setFixedWidth(130)
        self.mfa_input.setValidator(QRegExpValidator(QRegExp(r"\d{0,6}")))
        self.mfa_input.returnPressed.connect(self._on_submit)
        row.addWidget(self.mfa_input)

        self.submit_btn = QPushButton("验证")
        self.submit_btn.setObjectName("submitBtn")
        self.submit_btn.setCursor(Qt.PointingHandCursor)
        self.submit_btn.clicked.connect(self._on_submit)
        row.addWidget(self.submit_btn)

        self.toast_label = QLabel("")
        self.toast_label.setObjectName("toastSuccess")
        self.toast_label.setVisible(False)
        row.addWidget(self.toast_label)

        row.addStretch()
        layout.addWidget(self.mfa_widget)

    # ── 状态更新 ─────────────────────────────────────────

    def _apply_status(self):
        status, remaining = self.profile.get_status_and_remaining()

        if status == ProfileStatus.ACTIVE:
            _swap_object_name(self.indicator, "indicatorActive")
            self.status_label.setVisible(False)
            self.timer_label.setVisible(True)
            self.action_widget.setVisible(True)
            self.mfa_widget.setVisible(self._mfa_input_visible)
            self._set_timer_text(remaining)
        else:
            _swap_object_name(self.indicator, "indicatorExpired")
            self.timer_label.setVisible(False)
            self.status_label.setVisible(True)
            self.status_label.setText(
                "已过期" if status == ProfileStatus.EXPIRED else "未初始化"
            )
            self.action_widget.setVisible(False)
            self._mfa_input_visible = True
            self.mfa_widget.setVisible(True)

    def _set_timer_text(self, remaining: int):
        new_name = "timerLabelWarning" if remaining < 600 else "timerLabelActive"
        _swap_object_name(self.timer_label, new_name)
        self.timer_label.setText(f"⏱ {_format_remaining(remaining)}")

    def tick(self):
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

    def _on_link_clicked(self):
        """打开 AWS IAM Role 控制台页面"""
        if self.profile.role_arn:
            url = get_iam_role_url(self.profile.role_arn)
            webbrowser.open(url)

    def _on_cmd_clicked(self):
        """复制 MFA 终端命令到剪贴板"""
        cmd = self._build_mfa_command()
        clipboard = QApplication.clipboard()
        clipboard.setText(cmd)
        self._show_toast("命令已复制到剪贴板", is_error=False)

    def _build_mfa_command(self) -> str:
        """生成 aws-mfa 终端命令"""
        parts = []
        if self.profile.region:
            parts.append(f"export AWS_DEFAULT_REGION={self.profile.region}")
        parts.append(f"AWS_PROFILE={self.profile.session_name}")
        duration = self.profile.duration_seconds or 3600
        parts.append(f"; aws-mfa --duration {duration}")
        return " ".join(parts)

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
        _swap_object_name(self.toast_label, "toastError" if is_error else "toastSuccess")
        self.toast_label.setText(message)
        self.toast_label.setVisible(True)

    def _clear_toast(self):
        self.toast_label.setText("")
        self.toast_label.setVisible(False)

    @property
    def is_starred(self) -> bool:
        return self._is_starred

    def update_profile(self, profile: MfaProfile):
        self.profile = profile
        self._apply_status()
