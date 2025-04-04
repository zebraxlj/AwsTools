import os
from typing import Tuple


def check_proxy() -> Tuple[bool, str]:
    """ 检查系统代理 """
    import platform
    sys_type = platform.system()
    if sys_type == 'Windows':
        import winreg
        path = r'SOFTWARE\Microsoft\Windows\CurrentVersion\Internet Settings'
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, path, 0, winreg.KEY_READ)
        proxy_server, _ = winreg.QueryValueEx(key, 'ProxyServer')
        proxy_enable, _ = winreg.QueryValueEx(key, 'ProxyEnable')
        # print(f'proxy: {proxy_enable} {proxy_server}\n')
        return proxy_enable == 1, proxy_server
    elif sys_type == 'Linux':
        proxy_keys = ['http_proxy', 'https_proxy']

        for key in proxy_keys:
            if os.environ.get(key):
                return True, os.environ.get(key)
            if os.environ.get(key.upper()):
                return True, os.environ.get(key.upper())
        return False, None
    else:
        from warnings import warn
        warn(f'Unsupported OS: {sys_type}')
        return False, None
