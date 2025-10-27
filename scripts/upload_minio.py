import os
import sys
import yaml
import argparse
from datetime import datetime
import pandas as pd
import tempfile
from minio import Minio
from minio.error import S3Error



def load_minio_config(config_file: str):
    """Load a single global MinIO config from YAML."""
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)

    if "minio_asset" not in cfg:
        raise ValueError("Missing 'minio_asset' section in config file.")

    return cfg["minio_asset"]

def upload_folder(client: Minio, bucket: str, in_dir: str, dest_prefix: str, file_filter=None):
    """Walk through all files in in_dir and upload to MinIO under dest_prefix."""
    for root, dirs, files in os.walk(in_dir):
        for filename in files:
            if filename.endswith(".DS_Store"):
                continue
            if file_filter and file_filter not in filename.lower():
                continue

            local_path = os.path.join(root, filename)
            rel_path = os.path.relpath(local_path, in_dir)
            object_path = os.path.join(dest_prefix, rel_path).replace("\\", "/")

            # Handle tabular data (CSV or TSV)
            ext = os.path.splitext(filename)[1].lower()
            if ext in [".csv", ".tsv"]:
                sep = "," if ext == ".csv" else "\t"
                try:
                    df = pd.read_csv(local_path, sep=sep)
                    df["timestamp"] = datetime.utcnow().isoformat()

                    # Save to temporary file before upload
                    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmpf:
                        df.to_csv(tmpf.name, index=False, sep=sep)
                        temp_path = tmpf.name

                    client.fput_object(bucket_name=bucket, object_name=object_path, file_path=temp_path)
                    print(f"Uploaded (with timestamp) {local_path} → {bucket}/{object_path}")
                    os.remove(temp_path)

                except Exception as e:
                    print(f"Error processing {local_path}: {e}")
            else:
                # Non-tabular files (upload as-is)
                try:
                    client.fput_object(bucket_name=bucket, object_name=object_path, file_path=local_path)
                    print(f"Uploaded {local_path} → {bucket}/{object_path}")
                except S3Error as err:
                    print(f"Error uploading {local_path}: {err}")


def upload_to_minio(country: str, dataset_type: str, config_file="configs/minio_config.yaml"):
    """Main entrypoint: uploads all files for dataset_type of given country."""
    dataset_type = dataset_type.lower()
    config = load_minio_config(config_file)

    in_dir = os.path.join("data", country, "Output")
    if not os.path.isdir(in_dir):
        raise FileNotFoundError(f"Output directory not found: {in_dir}")

    client = Minio(
        endpoint=config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config.get("secure", True),
    )

    # Ensure bucket exists
    try:
        if not client.bucket_exists(config["bucket"]):
            raise RuntimeError(f"Bucket does not exist: {config['bucket']}")
    except S3Error as err:
        raise RuntimeError(f"Error checking bucket {config['bucket']}: {err}")

    # heigit-hdx-public/risk_assessment_inputs/rwa/<filename>
    dest_prefix = os.path.join(config.get("dest_prefix", ""), country.lower())
    dest_prefix = dest_prefix.replace("\\", "/")

    upload_folder(
        client=client,
        bucket=config["bucket"],
        in_dir=in_dir,
        dest_prefix=dest_prefix,
        file_filter=dataset_type,  # still filter filenames by dataset_type
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Upload files to MinIO bucket.")
    parser.add_argument("country", type=str, help="ISO 3166-3 country code, e.g. 'RWA'")
    parser.add_argument(
        "dataset_type",
        type=str,
        choices=["demographics", "facilities", "ndvi", "crops", "flood"],
        help="Type of dataset to upload (used for filtering filenames)",
    )
    parser.add_argument(
        "--config_file",
        default="configs/minio_config.yaml",
        help="Path to the unified MinIO config file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        upload_to_minio(args.country, args.dataset_type, args.config_file)
    except Exception as e:
        print(f"Upload failed: {e}")
        sys.exit(1)