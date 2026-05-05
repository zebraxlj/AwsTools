"""
AWS Tools 主应用：QApplication + 主窗口 Tab 骨架。
新功能只需在此注册一个 tab 即可。
"""

import sys

from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QApplication, QMainWindow, QTabWidget

from UI.mfa.mfa_widget import MfaWidget
from UI.styles import WINDOW_STYLE


class MainWindow(QMainWindow):
    """主窗口：Tab 容器，每个功能模块一个 tab"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("AWS Tools")
        self.setMinimumSize(620, 500)
        self.resize(660, 750)
        self.setStyleSheet(WINDOW_STYLE)

        # Tab 容器
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # ── 注册功能标签页 ──
        self.tabs.addTab(MfaWidget(), "MFA 管理")
        # 以后新增功能只需加一行：
        # self.tabs.addTab(CloudWatchWidget(), "CloudWatch")


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    font = QFont("Microsoft YaHei", 11)
    app.setFont(font)

    window = MainWindow()
    window.show()

    sys.exit(app.exec_())
