from typing import List

from utils.aws_consts import AllEnvs, Env


def get_env_from_pa_table(table_name: str) -> Env:
    possible_envs: List[Env] = [
        env for env in AllEnvs.__dict__.values()
        if isinstance(env, Env) and env.name.replace('-', '') in table_name
    ]
    if not possible_envs:
        raise ValueError(f'No environment found in table name: {table_name}')

    # Standalone Standalone-cand 的表都包含 Standalone 关键词，倒序找到最长的环境名即为目标环境
    possible_envs.sort(key=lambda x: x.name, reverse=True)
    return possible_envs[0]
