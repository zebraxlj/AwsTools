from typing import Optional

from boto3 import Session

from utils.ColorHelper.color_xterm_256 import ColorXTerm256


def set_xterm256_color(text: str, bg_color: Optional[ColorXTerm256] = None, fg_color: Optional[ColorXTerm256] = None):
    if not bg_color and not fg_color:
        return text
    if bg_color and fg_color:
        return f'\033[;48;5;{bg_color};38;5;{fg_color}m{text}\033[0m'
    if bg_color:
        return f'\033[;48;5;{bg_color}m{text}\033[0m'
    if fg_color:
        return f'\033[;38;5;{fg_color}m{text}\033[0m'


def handle_expired_token_exception(session: Session):
    cli_mfa = f'aws-mfa --duration 43200 --profile {session.profile_name}'
    print('#' * 50)
    print(set_xterm256_color(
        '[Error] Expired Token - run the following commands ', ColorXTerm256.BRIGHT_RED, ColorXTerm256.BRIGHT_WHITE
    ))
    print(f'export AWS_DEFAULT_REGION={session.region_name};{cli_mfa}')
    print('#' * 50)


def print_err(header: str = '', body: str = ''):
    if header:
        print(set_xterm256_color(header, ColorXTerm256.BRIGHT_RED, ColorXTerm256.BRIGHT_WHITE))
    if body:
        print(body)
