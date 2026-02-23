import datetime
import logging
import os
import sys

from PyQt5 import QtCore, QtGui, QtWidgets

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJ_PATH = os.path.dirname(os.path.dirname(__SCRIPT_DIR))
if PROJ_PATH not in sys.path:
    sys.path.insert(0, PROJ_PATH)

from CloudWatch.LambdaRequestLog.SearchAlertErrorRequest import AlertDetail, handle_alert, parse_alert_detail, \
    HandleAlertResult
from utils.logging_helper import setup_logging


class MainWindow(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("PaLogSearcher")

        self.alert_detail = None

        self.input_text = QtWidgets.QTextEdit()
        self.input_text.setPlaceholderText("飞书告警内容...")

        self.parse_button = QtWidgets.QPushButton("解析")

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

        time_layout = QtWidgets.QHBoxLayout()
        time_layout.addWidget(QtWidgets.QLabel("开始时间"))
        time_layout.addWidget(self.start_datetime_edit)
        time_layout.addWidget(QtWidgets.QLabel("结束时间"))
        time_layout.addWidget(self.end_datetime_edit)
        time_layout.addWidget(self.offset_input)
        time_layout.addWidget(self.offset_unit_combo)

        self.run_button = QtWidgets.QPushButton("运行")

        self.error_csv_input = QtWidgets.QLineEdit()
        self.error_csv_input.setPlaceholderText("error_csv")
        self.error_csv_input.setReadOnly(True)
        self.open_error_button = QtWidgets.QPushButton("打开 error_csv")

        self.full_csv_input = QtWidgets.QLineEdit()
        self.full_csv_input.setPlaceholderText("full_csv")
        self.full_csv_input.setReadOnly(True)
        self.open_full_button = QtWidgets.QPushButton("打开 full_csv")

        csv_grid_layout = QtWidgets.QGridLayout()
        csv_grid_layout.addWidget(QtWidgets.QLabel("error_csv"), 0, 0)
        csv_grid_layout.addWidget(self.error_csv_input, 0, 1)
        csv_grid_layout.addWidget(self.open_error_button, 0, 2)
        csv_grid_layout.addWidget(QtWidgets.QLabel("full_csv"), 1, 0)
        csv_grid_layout.addWidget(self.full_csv_input, 1, 1)
        csv_grid_layout.addWidget(self.open_full_button, 1, 2)

        self.output_text = QtWidgets.QTextEdit()
        self.output_text.setPlaceholderText("输出...")
        self.output_text.setReadOnly(True)

        layout = QtWidgets.QVBoxLayout()
        layout.setSpacing(10)
        layout.addWidget(self.input_text)
        layout.addWidget(self.parse_button)
        layout.addLayout(time_layout)
        layout.addWidget(self.run_button)
        layout.addLayout(csv_grid_layout)
        layout.addWidget(self.output_text)
        self.setLayout(layout)

        self.parse_button.clicked.connect(self.on_parse_clicked)
        self.offset_input.textChanged.connect(self.update_start_datetime_edit)
        self.offset_unit_combo.currentIndexChanged.connect(self.update_start_datetime_edit)
        self.end_datetime_edit.dateTimeChanged.connect(self.update_start_datetime_edit)
        self.run_button.clicked.connect(self.on_run_clicked)
        self.open_error_button.clicked.connect(self.on_open_error_csv)
        self.open_full_button.clicked.connect(self.on_open_full_csv)

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
        content = self.input_text.toPlainText()
        try:
            if self.alert_detail is None:
                self.alert_detail = parse_alert_detail(content)
            result: HandleAlertResult = handle_alert(
                self.alert_detail,
                dt_start=self.get_start_datetime(),
                dt_end=self.get_end_datetime(),
            )
        except Exception as e:
            self.output_text.setPlainText(str(e))
            return
        if isinstance(result, HandleAlertResult):
            self.error_csv_input.setText(result.error_csv)
            self.full_csv_input.setText(result.full_csv)
            self.output_text.setPlainText(
                f"ERROR CSV: {result.error_csv}\n"
                f"FULL CSV: {result.full_csv}\n"
                f"COUNTS: error={result.error_cnt} full={result.full_cnt}"
            )
        else:
            self.output_text.setPlainText("Done")

    def on_open_error_csv(self) -> None:
        self.open_file_path(self.error_csv_input.text())

    def on_open_full_csv(self) -> None:
        self.open_file_path(self.full_csv_input.text())

    def open_file_path(self, path: str) -> None:
        target = path.strip()
        if not target:
            self.output_text.setPlainText("Path is empty.")
            return
        if not os.path.exists(target):
            self.output_text.setPlainText(f"File not found: {target}")
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(target))

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
    window = MainWindow()
    window.resize(1366, 1080)
    window.show()
    return app.exec_()


if __name__ == "__main__":
    raise SystemExit(main())
