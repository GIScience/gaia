import os
import sys
import yaml
import argparse
from minio import Minio
from minio.error import S3Error


def load_minio_config(config_file: str, dataset_type: str):
    """Load MinIO config for the given dataset_type from YAML."""
    with open(config_file, "r") as f:
        cfg = yaml.safe_load(f)

    if dataset_type not in cfg["minio_asset"]:
        raise ValueError(f"Dataset type '{dataset_type}' not found in config.")

    return cfg["minio_asset"][dataset_type]


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

            try:
                client.fput_object(bucket_name=bucket, object_name=object_path, file_path=local_path)
                print(f"Uploaded {local_path} to {bucket}/{object_path}")
            except S3Error as err:
                print(f"Error uploading {local_path}: {err}")


def upload_to_minio(country: str, dataset_type: str, config_file="configs/minio_config.yaml"):
    """Main entrypoint: uploads all files for dataset_type of given country."""
    dataset_type = dataset_type.lower()
    config = load_minio_config(config_file, dataset_type)

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

    upload_folder(
        client=client,
        bucket=config["bucket"],
        in_dir=in_dir,
        dest_prefix=f"{config.get('dest_prefix')}/{country.lower()}",
        file_filter=dataset_type,
    )




def parse_args():
    parser = argparse.ArgumentParser(description="Upload files to MinIO bucket.")
    parser.add_argument("country", type=str, help="ISO 3166-3 country code, e.g. 'RWA'")
    parser.add_argument(
        "dataset_type",
        type=str,
        choices=["demographics", "facilities", "ndvi", "crops", "flood"],
        help="Type of dataset to upload (matches config section)",
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