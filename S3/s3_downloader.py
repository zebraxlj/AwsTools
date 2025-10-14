import logging
import os
from boto3.session import Session
from botocore.client import Config as BotoConfig

from utils.aws_client_helper import get_aws_profile
from utils.aws_consts import Env
from utils.SystemTools.file_system_helper import create_dir_if_not_exists


def download_dir_from_s3(env: Env, region: str, bucket_name: str, dir_key: str, output_path: str):
    session = Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('s3', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    paginator = client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=dir_key):
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            # Get the relative path of the file
            relative_path = obj['Key']
            if relative_path.endswith('/'):  # Skip directory markers
                continue

            # Create the local file path
            local_path = os.path.join(output_path, os.path.relpath(relative_path, dir_key))

            # Create the directory structure if it doesn't exist
            local_dir = os.path.dirname(local_path)
            create_dir_if_not_exists(dir_path=local_dir)

            # Download the file
            client.download_file(bucket_name, relative_path, local_path)
        logging.info(f'Completed page with {len(page["Contents"])} files')
    logging.info(f'Download Completed. output_path={output_path}')


def download_file_from_s3(
        env: Env, region: str, bucket_name: str, file_key: str, output_dir: str, output_file_name: str = '',
) -> None:
    session = Session(region_name=region, profile_name=get_aws_profile(region, env.is_prod_aws))
    client = session.client('s3', config=BotoConfig(connect_timeout=3, retries={"mode": "standard"}))

    # Download the file
    if output_file_name:
        output_path = os.path.join(output_dir, output_file_name)
    else:
        output_path = os.path.join(output_dir, file_key)
    create_dir_if_not_exists(file_path=output_path)

    client.download_file(bucket_name, file_key, output_path)
