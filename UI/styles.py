"""
所有 UI 组件的 QSS 样式常量集中管理。
"""

# ══════════════════════════════════════════════════════════
#  主窗口
# ══════════════════════════════════════════════════════════

WINDOW_STYLE = """
    QMainWindow {
        background-color: #f9fafb;
    }
"""

HEADER_STYLE = """
    QLabel {
        font-size: 22px;
        font-weight: bold;
        color: #111827;
    }
"""

RELOAD_BTN_STYLE = """
    QPushButton {
        background-color: #10b981;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 6px 14px;
        font-size: 15px;
        font-weight: bold;
    }
    QPushButton:hover {
        background-color: #059669;
    }
"""

TOGGLE_BTN_ON_STYLE = """
    QPushButton {
        background-color: #3b82f6;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 6px 14px;
        font-size: 15px;
        font-weight: bold;
    }
    QPushButton:hover {
        background-color: #2563eb;
    }
"""

TOGGLE_BTN_OFF_STYLE = """
    QPushButton {
        background-color: #e5e7eb;
        color: #374151;
        border: none;
        border-radius: 4px;
        padding: 6px 14px;
        font-size: 15px;
    }
    QPushButton:hover {
        background-color: #d1d5db;
    }
"""

GROUP_HEADER_STYLE = """
    QLabel {
        font-size: 16px;
        font-weight: bold;
        color: #6b7280;
        padding: 8px 0 4px 4px;
    }
"""

SEPARATOR_STYLE = """
    QFrame {
        background-color: #e5e7eb;
        max-height: 1px;
    }
"""

# ══════════════════════════════════════════════════════════
#  Profile 卡片
# ══════════════════════════════════════════════════════════

CARD_STYLE_ACTIVE = """
    QFrame#profileCard {
        background-color: #f0fdf4;
        border: 1px solid #86efac;
        border-radius: 8px;
        padding: 12px;
    }
"""

CARD_STYLE_EXPIRED = """
    QFrame#profileCard {
        background-color: #fef2f2;
        border: 1px solid #fca5a5;
        border-radius: 8px;
        padding: 12px;
    }
"""

STAR_BTN_STYLE = """
    QPushButton {
        border: none;
        background: transparent;
        font-size: 20px;
        padding: 2px 6px;
    }
    QPushButton:hover {
        background-color: rgba(0,0,0,0.05);
        border-radius: 4px;
    }
"""

PROFILE_NAME_STYLE = """
    QLabel {
        font-size: 17px;
        font-weight: bold;
        color: #1f2937;
    }
"""

DETAIL_LABEL_STYLE = """
    QLabel {
        font-size: 13px;
        color: #6b7280;
    }
"""

TIMER_LABEL_STYLE_ACTIVE = """
    QLabel {
        color: #16a34a;
        font-size: 15px;
        font-weight: bold;
    }
"""

TIMER_LABEL_STYLE_WARNING = """
    QLabel {
        color: #ea580c;
        font-size: 15px;
        font-weight: bold;
    }
"""

STATUS_EXPIRED_STYLE = """
    QLabel {
        color: #dc2626;
        font-size: 14px;
        font-weight: bold;
    }
"""

REFRESH_BTN_STYLE = """
    QPushButton {
        background-color: #f59e0b;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 4px 12px;
        font-size: 14px;
        font-weight: bold;
    }
    QPushButton:hover {
        background-color: #d97706;
    }
"""

MFA_LABEL_STYLE = """
    QLabel {
        font-size: 15px;
        color: #374151;
    }
"""

MFA_INPUT_STYLE = """
    QLineEdit {
        border: 2px solid #d1d5db;
        border-radius: 4px;
        padding: 4px 8px;
        font-size: 16px;
        font-family: Consolas, monospace;
        letter-spacing: 4px;
    }
    QLineEdit:focus {
        border-color: #3b82f6;
    }
"""

SUBMIT_BTN_STYLE = """
    QPushButton {
        background-color: #3b82f6;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 6px 18px;
        font-size: 15px;
        font-weight: bold;
    }
    QPushButton:hover {
        background-color: #2563eb;
    }
    QPushButton:disabled {
        background-color: #93c5fd;
    }
"""

TOAST_SUCCESS_STYLE = """
    QLabel {
        color: #16a34a;
        font-size: 14px;
        font-weight: bold;
        padding: 2px 0;
    }
"""

TOAST_ERROR_STYLE = """
    QLabel {
        color: #dc2626;
        font-size: 14px;
        font-weight: bold;
        padding: 2px 0;
    }
"""
