"""
后台线程 Worker：在非 UI 线程中执行 STS 调用，避免界面卡顿。
"""

from PyQt5.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot

from services.mfa.profile_parser import MfaProfile
from services.mfa.sts_service import StsResult, refresh_mfa_session


class MfaWorkerSignals(QObject):
    """Worker 的信号定义"""
    # 使用 object 类型传递 StsResult，确保跨线程 emit 安全
    finished = pyqtSignal(str, object)  # (profile_name, StsResult)


class MfaWorker(QRunnable):
    """在线程池中执行 MFA 刷新"""

    def __init__(self, profile: MfaProfile, mfa_code: str):
        super().__init__()
        self.profile = profile
        self.mfa_code = mfa_code
        self.signals = MfaWorkerSignals()

    @pyqtSlot()
    def run(self):
        result = refresh_mfa_session(self.profile, self.mfa_code)
        self.signals.finished.emit(self.profile.session_name, result)
