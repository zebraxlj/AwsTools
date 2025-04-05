import os


def create_dir_if_not_exists(dir_path: str = None, file_path: str = None) -> None:
    if dir_path and file_path:
        raise ValueError('Only one of dir_path or file_path should be provided.')
    if file_path:
        dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)
