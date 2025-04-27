import os
import sys
from typing import List

CURR_FOLDER_NAME = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.chdir('..')
sys.path.append(os.getcwd())

from DynamoDB.pa_dynamodb_helper import get_env_from_pa_table
from DynamoDB.dynamodb_helper import extract_table_files
from S3.s3_downloader import download_dir_from_s3
from utils.aws_consts import AllEnvs

BUCKET_TABLE_ADHOC_CN = '471636885451-heo-table-export-adhoc'
BUCKET_TABLE_ADHOC_US = '471636885451-heo-table-export-adhoc-us'
DATA_OUTPUT_DIR = f'./{CURR_FOLDER_NAME}/Data/output'


def download_pa_table_export_adhoc_from_s3(
        region: str, table_name: str, output_dir: str = DATA_OUTPUT_DIR,
        is_skip_download: bool = False,
) -> None:
    bucket_name = BUCKET_TABLE_ADHOC_CN if region.startswith('cn') else BUCKET_TABLE_ADHOC_US
    env = get_env_from_pa_table(table_name)
    dir_key = f'{env.name}/{table_name}'
    print(
        'Task:\n'
        f'Env={env.name}, region={region} table={table_name}\n'
        f'BucketName={bucket_name}\n'
        f'DirKey={dir_key}'
    )
    output_dir = os.path.join(output_dir, dir_key)
    if not is_skip_download:
        download_dir_from_s3(
            env=AllEnvs.PartyAnimalsInteral, region=region,
            bucket_name=bucket_name, dir_key=dir_key,
            output_path=output_dir,
        )
    extract_table_files(output_dir)
    return output_dir


def read_extracted_table(table_dir_path: str) -> List[dict]:
    import glob
    import amazon.ion.simpleion as ion

    if not os.path.exists(table_dir_path):
        print(f"Directory {table_dir_path} does not exist")
        return []

    # Find all ion files recursively
    ion_files = glob.glob(os.path.join(table_dir_path, "**/*.ion"), recursive=True)
    if not ion_files:
        print(f"No .ion files found in {table_dir_path}")
        return []

    print(f"Found {len(ion_files)} .ion files to read")

    records = []
    # Read each ion file
    for ion_file in ion_files:
        try:
            print(f"Reading {os.path.basename(ion_file)}")
            with open(ion_file, 'rb') as f:
                # Read and parse ion file
                reader = ion.load(f, single_value=False)
                for record in reader:
                    # Convert Ion object to Python dict
                    records.append(ion.dumps(record, binary=False))
        except Exception as e:
            print(f"Error reading {ion_file}: {str(e)}")

    return [ion.loads(record) for record in records]


if __name__ == "__main__":
    table_name = ''
    output_dir = download_pa_table_export_adhoc_from_s3(region='cn-northwest-1', table_name=table_name,
                                                        # is_skip_download=True,
                                                        )
    table_items = read_extracted_table(output_dir)
    table_data = [item['Item'] for item in table_items]
    for elem in table_data:
        print(elem)
