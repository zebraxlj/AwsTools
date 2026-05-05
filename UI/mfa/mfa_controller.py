"""
MFA 控制器：负责数据加载、分组过滤、调度 worker、数据回写。
不包含任何 UI 代码，通过信号与 widget 通信。
"""

from PyQt5.QtCore import QObject, QThreadPool, pyqtSignal

from services.mfa.config_store import get_starred_profiles, toggle_profile_starred
from services.mfa.profile_parser import MfaProfile, RegionGroup, parse_profiles
from services.mfa.sts_service import StsResult
from UI.mfa.mfa_worker import MfaWorker

GROUP_ORDER = [RegionGroup.CN, RegionGroup.US, RegionGroup.OTHER]
GROUP_LABELS = {
    RegionGroup.CN: "🇨🇳 中国区 (CN)",
    RegionGroup.US: "🇺🇸 国际区 (US)",
    RegionGroup.OTHER: "🌍 其他",
}


class GroupedProfiles:
    """分组 + 过滤后的 profile 数据，传递给 widget 渲染"""

    def __init__(self):
        self.groups: list[tuple[str, list[tuple[MfaProfile, bool]]]] = []
        # [(group_label, [(profile, is_starred), ...]), ...]


class MfaController(QObject):
    """
    MFA 功能的编排控制器。

    职责：
    - 加载 / 刷新 profile 列表
    - 分组 + 收藏过滤
    - 调度后台 STS worker
    - 收到 STS 结果后更新数据模型

    信号：
    - profiles_changed: 数据变化后通知 widget 重新渲染
    - mfa_result: 单个 profile 的 MFA 验证结果
    """

    profiles_changed = pyqtSignal(GroupedProfiles, bool)  # (data, show_starred_only)
    mfa_result = pyqtSignal(str, bool, str)  # (profile_name, success, error_message)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._profiles: list[MfaProfile] = []
        self._profile_map: dict[str, MfaProfile] = {}
        self._show_starred_only = False
        self._thread_pool = QThreadPool()

        # 有收藏时默认只显示收藏
        self._show_starred_only = len(get_starred_profiles()) > 0

    # ── 数据加载 ─────────────────────────────────────────

    def load_profiles(self):
        """加载 / 重新加载 profiles，并通知 widget"""
        self._profiles = parse_profiles()
        self._profile_map = {p.session_name: p for p in self._profiles}
        self._emit_profiles()

    @property
    def show_starred_only(self) -> bool:
        return self._show_starred_only

    # ── 分组 + 过滤 ──────────────────────────────────────

    def _build_grouped_profiles(self) -> GroupedProfiles:
        """根据当前过滤状态构建分组数据"""
        starred = get_starred_profiles()

        # 按区域分组
        raw_groups: dict[RegionGroup, list[MfaProfile]] = {}
        for p in self._profiles:
            raw_groups.setdefault(p.region_group, []).append(p)

        result = GroupedProfiles()

        for group in GROUP_ORDER:
            profiles_in_group = raw_groups.get(group, [])
            if not profiles_in_group:
                continue

            # 收藏过滤
            if self._show_starred_only:
                profiles_in_group = [p for p in profiles_in_group if p.session_name in starred]
                if not profiles_in_group:
                    continue

            label = GROUP_LABELS.get(group, str(group))
            items = [(p, p.session_name in starred) for p in profiles_in_group]
            result.groups.append((label, items))

        return result

    def _emit_profiles(self):
        data = self._build_grouped_profiles()
        self.profiles_changed.emit(data, self._show_starred_only)

    # ── 收藏 ─────────────────────────────────────────────

    def toggle_starred_filter(self):
        """切换 仅收藏 / 全部 视图"""
        self._show_starred_only = not self._show_starred_only
        self._emit_profiles()

    def toggle_profile_star(self, profile_name: str):
        """切换单个 profile 的收藏状态"""
        toggle_profile_starred(profile_name)
        # 仅收藏模式下需要刷新列表
        if self._show_starred_only:
            self._emit_profiles()

    def get_profile(self, profile_name: str) -> MfaProfile | None:
        """获取指定名称的 profile"""
        return self._profile_map.get(profile_name)

    # ── MFA 提交 ─────────────────────────────────────────

    def submit_mfa(self, profile_name: str, mfa_code: str):
        """提交 MFA 验证码，启动后台 STS 调用"""
        profile = self._profile_map.get(profile_name)
        if profile is None:
            self.mfa_result.emit(profile_name, False, "Profile not found")
            return

        worker = MfaWorker(profile, mfa_code)
        worker.signals.finished.connect(self._on_mfa_finished)
        self._thread_pool.start(worker)

    def _on_mfa_finished(self, profile_name: str, result: StsResult):
        """STS 调用完成回调：更新数据模型，通知 widget"""
        if result.success and result.session_info:
            profile = self._profile_map.get(profile_name)
            if profile:
                profile.session = result.session_info

        self.mfa_result.emit(
            profile_name,
            result.success,
            result.error_message or "",
        )
