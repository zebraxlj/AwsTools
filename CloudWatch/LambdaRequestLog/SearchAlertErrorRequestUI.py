import datetime
import logging
import os
import shutil
import subprocess
import sys
import threading

from PyQt5 import QtCore, QtGui, QtWidgets

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
__PROJ_DIR = os.path.dirname(os.path.dirname(__SCRIPT_DIR))
if __PROJ_DIR not in sys.path:
    sys.path.insert(0, __PROJ_DIR)

from CloudWatch.cloud_watch_ui_helper import load_app_style
from CloudWatch.LambdaRequestLog.SearchAlertErrorRequest import (  # noqa: E402
    AlertDetail, CancelledError, DEFAULT_DATA_DIR, handle_alert, parse_alert_detail, HandleAlertResult
)
from utils.logging_helper import setup_logging  # noqa: E402

SEARCH_QSS_PATH = os.path.join(__SCRIPT_DIR, "SearchAlertErrorRequest.qss")


def load_search_style() -> str:
    try:
        with open(SEARCH_QSS_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except OSError as exc:
        logging.error("Failed to load search style file: %s", exc)
        return ""


class _StreamEmitter(QtCore.QObject):
    """把 write() 调用转成 Qt 信号，跨线程投递到 UI。"""

    text_written = QtCore.pyqtSignal(str)

    def write(self, text: str) -> int:
        if text:
            self.text_written.emit(text)
        return len(text or "")

    def flush(self) -> None:
        pass


class _AlertWorker(QtCore.QThread):
    """后台跑 handle_alert；期间把 stdout/stderr/logging 重定向到 emitter。
    取消通过 cancel_token（threading.Event）传给 handle_alert，
    handle_alert 在下一个检查点抛 CancelledError；粒度 = 一次 AWS 调用。"""

    finished_ok = QtCore.pyqtSignal(object)
    failed = QtCore.pyqtSignal(str)

    def __init__(self, alert_detail, dt_start, dt_end, emitter, parent=None):
        super().__init__(parent)
        self._alert_detail = alert_detail
        self._dt_start = dt_start
        self._dt_end = dt_end
        self._emitter = emitter
        self._cancel_token = threading.Event()

    def request_cancel(self) -> None:
        self._cancel_token.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel_token.is_set()

    def run(self) -> None:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        root_logger = logging.getLogger()
        log_handler = logging.StreamHandler(self._emitter)
        log_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.addHandler(log_handler)
        sys.stdout = self._emitter
        sys.stderr = self._emitter
        try:
            result = handle_alert(
                self._alert_detail,
                dt_start=self._dt_start,
                dt_end=self._dt_end,
                cancel_token=self._cancel_token,
            )
            self.finished_ok.emit(result)
        except CancelledError:
            # 已取消：不发结果、不发错误；_on_run_thread_finished 会看到 cancelled=True
            pass
        except Exception as exc:
            logging.exception("handle_alert failed")
            self.failed.emit(str(exc))
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            root_logger.removeHandler(log_handler)


class SearchAlertErrorWidget(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("CloudWatch 错误告警日志搜索")

        self.setStyleSheet(load_search_style())
        self.alert_detail = None

        # region 部件: 输入
        self.input_text = QtWidgets.QTextEdit()
        self.input_text.setPlaceholderText("飞书告警内容...")
        self.input_text.setMinimumHeight(140)
        self.parse_button = QtWidgets.QPushButton("解析")
        self.parse_button.setObjectName('secondaryButton')
        # endregion

        # region layout: 输入
        input_layout = QtWidgets.QVBoxLayout()
        input_layout.setSpacing(8)
        input_layout.addWidget(self.input_text)
        input_layout.addWidget(self.parse_button)
        # endregion

        # region 部件: 运行
        # 开始时间
        self.start_datetime_edit = QtWidgets.QDateTimeEdit(QtCore.QDateTime.currentDateTime())
        self.start_datetime_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.start_datetime_edit.setCalendarPopup(True)
        # 结束时间
        self.end_datetime_edit = QtWidgets.QDateTimeEdit(QtCore.QDateTime.currentDateTime())
        self.end_datetime_edit.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.end_datetime_edit.setCalendarPopup(True)
        # 时长长度
        self.offset_input = QtWidgets.QLineEdit()
        self.offset_input.setText('5')
        self.offset_input.setPlaceholderText("数字")
        self.offset_input.setValidator(QtGui.QIntValidator(0, 1_000_000))
        # 时长单位
        self.offset_unit_combo = QtWidgets.QComboBox()
        self.offset_unit_combo.addItems(["分钟", "小时", "天", "周", "月"])
        self.update_start_datetime_edit()
        # 运行按钮
        self.run_button = QtWidgets.QPushButton("运行")
        self.run_button.setObjectName('primaryButton')
        # 进度条（贴在运行按钮下方，跑动时才显示）
        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setRange(0, 0)  # busy indicator
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setFormat("运行中...")
        self.progress_bar.hide()
        # endregion

        # region layout: 运行
        run_layout = QtWidgets.QHBoxLayout()
        run_layout.addWidget(QtWidgets.QLabel("开始时间"))
        run_layout.addWidget(self.start_datetime_edit)
        run_layout.addWidget(QtWidgets.QLabel("结束时间"))
        run_layout.addWidget(self.end_datetime_edit)
        run_layout.addWidget(self.offset_input)
        run_layout.addWidget(self.offset_unit_combo)

        run_group_layout = QtWidgets.QVBoxLayout()
        run_group_layout.setSpacing(8)
        run_group_layout.addLayout(run_layout)
        run_group_layout.addWidget(self.run_button)
        run_group_layout.addWidget(self.progress_bar)
        # endregion

        # region 部件: 结果
        self.error_csv_input = QtWidgets.QLineEdit()
        self.error_csv_input.setPlaceholderText("error_csv")
        self.error_csv_input.setReadOnly(True)
        self.open_error_button = QtWidgets.QPushButton("打开错误日志 csv")
        self.open_error_button.setObjectName('secondaryButton')

        self.full_csv_input = QtWidgets.QLineEdit()
        self.full_csv_input.setPlaceholderText("full_csv")
        self.full_csv_input.setReadOnly(True)
        self.open_full_button = QtWidgets.QPushButton("打开全量日志 csv")
        self.open_full_button.setObjectName('secondaryButton')

        self.open_folder_button = QtWidgets.QPushButton("打开下载文件夹")
        self.open_folder_button.setObjectName('secondaryButton')
        self.output_dir = ""
        # endregion

        # region layout: 结果
        csv_grid_layout = QtWidgets.QGridLayout()
        csv_grid_layout.addWidget(self.error_csv_input, 0, 0)
        csv_grid_layout.addWidget(self.open_error_button, 0, 1)
        csv_grid_layout.addWidget(self.full_csv_input, 1, 0)
        csv_grid_layout.addWidget(self.open_full_button, 1, 1)
        csv_grid_layout.addWidget(self.open_folder_button, 2, 0, 1, 2)
        csv_grid_layout.setSpacing(8)
        csv_grid_layout.setColumnStretch(0, 1)
        # endregion

        # region 部件: 输出
        self.output_text = QtWidgets.QTextEdit()
        self.output_text.setPlaceholderText("输出...")
        self.output_text.setReadOnly(True)
        self.output_text.setMinimumHeight(180)
        self.output_text.setFont(QtGui.QFont('Consolas'))
        # endregion

        # region layout: 输出
        output_layout = QtWidgets.QVBoxLayout()
        output_layout.addWidget(self.output_text)
        # endregion

        self._worker: "_AlertWorker | None" = None
        self._original_run_text = self.run_button.text()
        self._output_emitter = _StreamEmitter()
        self._output_emitter.text_written.connect(self._append_output)

        input_group = QtWidgets.QGroupBox("1. 输入")
        input_group.setLayout(input_layout)
        run_group = QtWidgets.QGroupBox("2. 时间设置")
        run_group.setLayout(run_group_layout)
        result_group = QtWidgets.QGroupBox("3. 下载结果")
        result_group.setLayout(csv_grid_layout)
        output_group = QtWidgets.QGroupBox("输出")
        output_group.setLayout(output_layout)

        layout = QtWidgets.QVBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.addWidget(input_group)
        layout.addWidget(run_group)
        layout.addWidget(result_group)
        layout.addWidget(output_group)
        self.setLayout(layout)

        self.parse_button.clicked.connect(self.on_parse_clicked)
        self.offset_input.textChanged.connect(self.update_start_datetime_edit)
        self.offset_unit_combo.currentIndexChanged.connect(self.update_start_datetime_edit)
        self.end_datetime_edit.dateTimeChanged.connect(self.update_start_datetime_edit)
        self.run_button.clicked.connect(self.on_run_clicked)
        self.open_error_button.clicked.connect(self.on_open_error_csv)
        self.open_full_button.clicked.connect(self.on_open_full_csv)
        self.open_folder_button.clicked.connect(self.on_open_folder)

    def get_start_datetime(self) -> datetime.datetime:
        return self.start_datetime_edit.dateTime().toPyDateTime()

    def get_end_datetime(self) -> datetime.datetime:
        return self.end_datetime_edit.dateTime().toPyDateTime()

    def on_parse_clicked(self) -> None:
        start_dt = self.get_start_datetime()
        end_dt = self.get_end_datetime()
        content = self.input_text.toPlainText()
        try:
            alarm_info: AlertDetail = parse_alert_detail(content)
            self.output_text.setPlainText(str(alarm_info))
        except Exception as e:
            self.output_text.setPlainText(str(e))
            logging.error(e)
            return
        self.alert_detail = alarm_info
        if alarm_info.alarm_dt is not None:
            alarm_qdt = QtCore.QDateTime.fromSecsSinceEpoch(int(alarm_info.alarm_dt.timestamp()))
            self.end_datetime_edit.setDateTime(alarm_qdt)
            self.update_start_datetime_edit()
        logging.debug("Start time: %s; End time: %s", start_dt, end_dt)
        logging.debug(str(alarm_info))

    def on_run_clicked(self) -> None:
        if self._worker is not None and self._worker.isRunning():
            # 运行中再点 = 取消
            self._cancel_running()
            return
        content = self.input_text.toPlainText()
        try:
            self.alert_detail = parse_alert_detail(content)
        except Exception as e:
            self.output_text.setPlainText(str(e))
            return

        self.output_text.clear()
        self.error_csv_input.clear()
        self.full_csv_input.clear()
        self._set_running(True)

        worker = _AlertWorker(
            alert_detail=self.alert_detail,
            dt_start=self.get_start_datetime(),
            dt_end=self.get_end_datetime(),
            emitter=self._output_emitter,
            parent=self,
        )
        worker.finished_ok.connect(self._on_run_finished)
        worker.failed.connect(self._on_run_failed)
        worker.finished.connect(self._on_run_thread_finished)
        self._worker = worker
        worker.start()

    def _cancel_running(self) -> None:
        if self._worker is None or not self._worker.isRunning():
            return
        self._worker.request_cancel()
        self.run_button.setEnabled(False)
        self.run_button.setText("取消中...")
        self.progress_bar.setFormat("取消中，等待下一个检查点...")
        self._append_output("\n[INFO] 已请求取消，等待当前 AWS 调用返回后在下一个检查点停止。\n")

    def _set_running(self, running: bool) -> None:
        self.run_button.setEnabled(True)
        self.run_button.setText("取消" if running else self._original_run_text)
        self.run_button.setObjectName('cancelButton' if running else 'primaryButton')
        # 重新走一遍 QSS，让 objectName 变化生效
        self.run_button.style().unpolish(self.run_button)
        self.run_button.style().polish(self.run_button)
        self.parse_button.setEnabled(not running)
        if running:
            self.progress_bar.setFormat("运行中...")
            self.progress_bar.show()
        else:
            self.progress_bar.hide()

    def _append_output(self, text: str) -> None:
        cursor = self.output_text.textCursor()
        cursor.movePosition(QtGui.QTextCursor.End)
        cursor.insertText(text)
        self.output_text.setTextCursor(cursor)
        self.output_text.ensureCursorVisible()

    def _on_run_thread_finished(self) -> None:
        cancelled = bool(self._worker and self._worker.cancelled)
        self._set_running(False)
        if cancelled:
            self._append_output("\n[INFO] 已取消。\n")

    def _on_run_finished(self, result: object) -> None:
        if self._worker is not None and self._worker.cancelled:
            return
        if isinstance(result, HandleAlertResult):
            self.error_csv_input.setText(result.error_csv)
            self.full_csv_input.setText(result.full_csv)
            self.output_dir = os.path.dirname(result.error_csv or result.full_csv or "")
            self._append_output(
                f"\nERROR CSV: {result.error_csv}\n"
                f"FULL CSV: {result.full_csv}\n"
                f"COUNTS: error={result.error_cnt} full={result.full_cnt}\n"
            )
        else:
            self._append_output("\nDone\n")

    def _on_run_failed(self, message: str) -> None:
        if self._worker is not None and self._worker.cancelled:
            return
        self._append_output(f"\n[ERROR] {message}\n")

    def on_open_error_csv(self) -> None:
        self.open_file_path(self.error_csv_input.text())

    def on_open_full_csv(self) -> None:
        self.open_file_path(self.full_csv_input.text())

    def on_open_folder(self) -> None:
        error_csv = self.error_csv_input.text().strip()
        if error_csv and os.path.isfile(error_csv):
            self.reveal_in_explorer(error_csv)
            return
        target = self.output_dir or DEFAULT_DATA_DIR
        if target and not os.path.isdir(target):
            os.makedirs(target, exist_ok=True)
        self.open_folder(target)

    def open_file_path(self, path: str) -> None:
        target = path.strip()
        if not target:
            self.output_text.setPlainText("Path is empty.")
            return
        if not os.path.exists(target):
            self.output_text.setPlainText(f"File not found: {target}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(target))

    def open_folder(self, path: str) -> None:
        target = (path or "").strip()
        if not target:
            self.output_text.setPlainText("Folder path is empty.")
            return
        if not os.path.isdir(target):
            self.output_text.setPlainText(f"Folder not found: {target}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(target))

    def reveal_in_explorer(self, file_path: str) -> None:
        target = os.path.normpath(os.path.abspath(file_path))
        try:
            if sys.platform == 'win32':
                # explorer 即使成功也常返回非 0，故不检查返回码；/select, 后必须紧跟路径。
                subprocess.run(['explorer', f'/select,{target}'])
                return
            if sys.platform == 'darwin' and shutil.which('open'):
                subprocess.run(['open', '-R', target], check=False)
                return
        except Exception as exc:
            logging.error("Failed to reveal file in explorer: %s", exc)
        # 其它平台或失败：退回到打开所在目录。
        self.open_folder(os.path.dirname(target))

    def update_start_datetime_edit(self) -> None:
        text = self.offset_input.text().strip()
        if not text:
            return
        try:
            value = int(text)
        except ValueError:
            return

        end_dt = self.end_datetime_edit.dateTime()
        unit = self.offset_unit_combo.currentText()
        if unit == "分钟":
            start_dt = end_dt.addSecs(-value * 60)
        elif unit == "小时":
            start_dt = end_dt.addSecs(-value * 3600)
        elif unit == "天":
            start_dt = end_dt.addDays(-value)
        elif unit == "周":
            start_dt = end_dt.addDays(-value * 7)
        elif unit == "月":
            start_dt = end_dt.addMonths(-value)
        else:
            return

        self.start_datetime_edit.setDateTime(start_dt)


def main() -> int:
    setup_logging(is_show_logger_name=False)
    app = QtWidgets.QApplication(sys.argv)
    font = app.font()
    font.setPointSize(font.pointSize() + 4)
    app.setFont(font)
    app.setStyleSheet(load_app_style())
    widget = SearchAlertErrorWidget()
    widget.resize(1366, 1080)
    widget.show()
    return app.exec_()


if __name__ == "__main__":
    raise SystemExit(main())
