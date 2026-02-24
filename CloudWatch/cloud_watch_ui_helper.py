import logging
import os

__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_QSS_PATH = os.path.join(__SCRIPT_DIR, "CloudWatchUi.qss")


def load_app_style() -> str:
    try:
        with open(BASE_QSS_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except OSError as exc:
        logging.error("Failed to load base style file: %s", exc)
        return ""
