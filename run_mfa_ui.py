"""
MFA 模块开发入口 — 只启动 MFA Widget 作为独立窗口。
用于开发调试，不启动完整的多 Tab 应用。
"""

from utils.logging_helper import setup_logging
from UI.app import run_single_widget
from UI.mfa.mfa_widget import MfaWidget

if __name__ == "__main__":
    setup_logging()
    run_single_widget(MfaWidget, title="AWS MFA Manager — Dev")
