"""
AWS Tools 正式启动入口 — 完整的多 Tab 应用。
"""

from utils.logging_helper import setup_logging
from UI.app import run_app

if __name__ == "__main__":
    setup_logging()
    run_app()
