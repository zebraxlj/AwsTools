"""
AWS Tools 主应用：QApplication + 主窗口 Tab 骨架。
新功能只需在此注册一个 tab 即可。
"""

import logging
import sys
from pathlib import Path

from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QApplication, QMainWindow, QTabWidget, QWidget

from UI.mfa.mfa_widget import MfaWidget


def _load_qss() -> str:
    """加载 theme.qss 样式文件"""
    qss_path = Path(__file__).parent / "theme.qss"
    logging.debug(f'loading qss file: {qss_path}')
    return qss_path.read_text(encoding="utf-8")


def create_app() -> QApplication:
    """创建并配置 QApplication（字体、主题、样式表），所有入口共享此函数。"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Microsoft YaHei", 12))
    app.setStyleSheet(_load_qss())
    return app


class MainWindow(QMainWindow):
    """主窗口：Tab 容器，每个功能模块一个 tab"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("AWS Tools")
        self.setMinimumSize(660, 550)
        self.resize(720, 800)

        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # ── 注册功能标签页 ──
        self.tabs.addTab(MfaWidget(), "MFA 管理")
        # 以后新增功能只需加一行：
        # self.tabs.addTab(CloudWatchWidget(), "CloudWatch")


def run_app():
    """正式入口：启动完整的多 Tab 应用"""
    app = create_app()
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


def run_single_widget(widget_factory, title: str = "AWS Tools — Dev",
                      width: int = 700, height: int = 750):
    """
    开发入口：启动单个 Widget 作为独立窗口，用于模块开发调试。

    widget_factory: 可调用对象（class 或 lambda），在 QApplication 创建之后才实例化。
    """
    app = create_app()
    widget = widget_factory()
    window = QMainWindow()
    window.setWindowTitle(title)
    window.setCentralWidget(widget)
    window.resize(width, height)
    window.show()
    sys.exit(app.exec_())
