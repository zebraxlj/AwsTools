import glob
import gzip
import os


def extract_table_files(output_path: str) -> None:
    # Create output path if not exists
    if not os.path.exists(output_path):
        print(f"Directory {output_path} does not exist")
        return

    # Find all .gz files recursively
    gz_files = glob.glob(os.path.join(output_path, "**/*.gz"), recursive=True)
    if not gz_files:
        print(f"No .gz files found in {output_path}")
        return

    print(f"Found {len(gz_files)} .gz files to extract")

    # Extract each .gz file
    for gz_file in gz_files:
        try:
            # Get the output file name (remove .gz extension)
            output_file = gz_file[:-3]  # Remove .gz extension

            print(f"Extracting {os.path.basename(gz_file)}")

            # Extract the file
            with gzip.open(gz_file, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    f_out.write(f_in.read())

        except Exception as e:
            print(f"Error extracting {gz_file}: {str(e)}")
