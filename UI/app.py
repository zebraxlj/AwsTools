"""
AWS Tools 主应用：QApplication + 主窗口骨架。
新功能只需在 MainWindow.__init__ 中调用 add_page() 注册即可。
"""

import logging
import sys
from pathlib import Path

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont, QIcon
from PyQt5.QtWidgets import (QApplication, QHBoxLayout, QMainWindow,
                              QPushButton, QStackedWidget, QVBoxLayout,
                              QWidget)

from UI.mfa.mfa_widget import MfaWidget
from UI.cloudwatch.cloudwatch_widget import CloudWatchWidget


def _load_qss() -> str:
    """加载 theme.qss 样式文件"""
    qss_path = Path(__file__).parent / "theme.qss"
    logging.debug(f'loading qss file: {qss_path}')
    return qss_path.read_text(encoding="utf-8")


def _load_icon() -> QIcon:
    """加载应用图标"""
    icon_path = Path(__file__).parent / "assets" / "aws_tools_icon_310x310.png"
    return QIcon(str(icon_path))


def create_app() -> QApplication:
    """创建并配置 QApplication（字体、主题、样式表），所有入口共享此函数。"""
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setFont(QFont("Microsoft YaHei", 12))
    app.setStyleSheet(_load_qss())
    app.setWindowIcon(_load_icon())
    return app


def _shutdown_page(page: QWidget) -> None:
    """
    调用页面可选实现的 shutdown()，让它取消后台 worker。

    不做这一步的话，页面内自建的 QThreadPool 析构时会 waitForDone()，
    一直等到限速队列跑完（几百个日志组 @5TPS 可达数分钟）；且那时 worker
    的 signals 已随 widget 销毁，emit 会抛
    RuntimeError: wrapped C/C++ object ... has been deleted。
    """
    shutdown = getattr(page, "shutdown", None)
    if not callable(shutdown):
        return
    try:
        shutdown()
    except Exception:
        logging.exception("page shutdown failed: %s", type(page).__name__)


class MainWindow(QMainWindow):
    """主窗口：左侧导航栏 + 右侧页面栈"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("AWS Tools")
        self.setMinimumSize(660, 550)
        self.resize(1200, 768)

        # ── 中心布局：左侧导航 | 右侧内容 ──
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QHBoxLayout(central)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        # 左侧导航栏
        self._nav_bar = QWidget()
        self._nav_bar.setObjectName("navBar")
        self._nav_bar.setFixedWidth(130)
        self._nav_layout = QVBoxLayout(self._nav_bar)
        self._nav_layout.setContentsMargins(6, 10, 6, 10)
        self._nav_layout.setSpacing(4)
        self._nav_layout.addStretch()          # 按钮靠上，底部弹性留白

        # 右侧页面栈
        self._pages = QStackedWidget()
        self._pages.setObjectName("pageStack")

        root_layout.addWidget(self._nav_bar)
        root_layout.addWidget(self._pages, 1)  # stretch=1 让页面栈占满剩余空间

        # 内部记录
        self._nav_buttons: list[QPushButton] = []

        # ── 注册功能页面 ──
        self.add_page("MFA 管理", MfaWidget())
        self.add_page("CloudWatch", CloudWatchWidget())

        # 默认选中第一个
        if self._nav_buttons:
            self._switch_page(0)

    # ── public API ──

    def add_page(self, name: str, widget: QWidget):
        """注册一个页面：在左侧导航栏添加按钮，在右侧添加对应 widget。"""
        index = self._pages.count()
        self._pages.addWidget(widget)

        btn = QPushButton(name)
        btn.setObjectName("navBtn")
        btn.setCursor(Qt.PointingHandCursor)
        btn.setCheckable(True)
        btn.clicked.connect(lambda checked, i=index: self._switch_page(i))

        # 插入到 stretch 之前
        self._nav_layout.insertWidget(self._nav_layout.count() - 1, btn)
        self._nav_buttons.append(btn)

    # ── private ──

    def _switch_page(self, index: int):
        """切换到第 index 个页面，并更新按钮选中态。"""
        self._pages.setCurrentIndex(index)
        for i, btn in enumerate(self._nav_buttons):
            btn.setChecked(i == index)

    def closeEvent(self, event):
        """关闭前依次通知各页面清理（取消后台 worker），再走默认关闭流程。"""
        for i in range(self._pages.count()):
            _shutdown_page(self._pages.widget(i))
        super().closeEvent(event)


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
    # 独立窗口没有 MainWindow 的 closeEvent，改挂 aboutToQuit 做同样的清理
    app.aboutToQuit.connect(lambda: _shutdown_page(widget))
    window = QMainWindow()
    window.setWindowTitle(title)
    window.setCentralWidget(widget)
    window.resize(width, height)
    window.show()
    sys.exit(app.exec_())
