import os
import sys
import argparse
from minio import Minio
from minio.error import S3Error


def upload_folder(
    client: Minio, bucket: str, in_dir: str, dest_prefix: str, file_filter=None
):
    """Walk through all files in in_dir and upload to MinIO under dest_prefix."""
    for root, dirs, files in os.walk(in_dir):
        for filename in files:
            if filename.endswith(".DS_Store"):
                continue
            if file_filter and file_filter not in filename.lower():
                continue

            local_path = os.path.join(root, filename)
            rel_path = os.path.relpath(local_path, in_dir)
            # Keep subdirectory structure but put everything under dest_prefix/country
            object_path = os.path.join(dest_prefix, rel_path).replace("\\", "/")

            try:
                client.fput_object(
                    bucket_name=bucket, object_name=object_path, file_path=local_path
                )
                print(f"Uploaded {local_path} → {bucket}/{object_path}")
            except S3Error as err:
                print(f"Error uploading {local_path}: {err}")


def upload_to_minio(
    country: str,
    dataset_type: str,
    endpoint: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    dest_prefix: str,
    secure: bool = True,
):
    """Main entrypoint: uploads all files for dataset_type of given country."""
    dataset_type = dataset_type.lower()

    in_dir = os.path.join("data", country, "Output")

    if not os.path.isdir(in_dir):
        raise FileNotFoundError(f"Output directory not found: {in_dir}")

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    # Ensure bucket exists
    try:
        if not client.bucket_exists(bucket):
            raise RuntimeError(f"Bucket does not exist: {bucket}")
    except S3Error as err:
        raise RuntimeError(f"Error checking bucket {bucket}: {err}")

    # heigit-hdx-public/risk_assessment_inputs/rwa/<filename>
    dest_prefix = os.path.join(dest_prefix, country.lower())
    dest_prefix = dest_prefix.replace("\\", "/")

    upload_folder(
        client=client,
        bucket=bucket,
        in_dir=in_dir,
        dest_prefix=dest_prefix,
        file_filter=dataset_type,  # still filter filenames by dataset_type
    )


def _parse_bool(value) -> bool:
    return str(value).lower() in ("1", "true", "yes", "on")


def parse_args():
    parser = argparse.ArgumentParser(description="Upload files to MinIO bucket.")
    parser.add_argument("country", type=str, help="ISO 3166-3 country code, e.g. 'RWA'")
    parser.add_argument(
        "dataset_type",
        type=str,
        choices=[
            "demographics",
            "facilities",
            "ndvi",
            "crops",
            "flood",
            "risk",
            "pmtiles",
        ],
        help="Type of dataset to upload (used for filtering filenames)",
    )
    parser.add_argument(
        "--endpoint",
        default=os.getenv("MINIO_ENDPOINT", "hot.storage.heigit.org"),
        help="MinIO endpoint (host only)",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET", "heigit-hdx-public"),
        help="MinIO bucket",
    )
    parser.add_argument(
        "--access_key", default=os.getenv("MINIO_ACCESS_KEY"), help="MinIO access key"
    )
    parser.add_argument(
        "--secret_key", default=os.getenv("MINIO_SECRET_KEY"), help="MinIO secret key"
    )
    parser.add_argument(
        "--dest_prefix",
        default=os.getenv("MINIO_DEST_PREFIX", "risk_assessment_inputs"),
        help="Destination prefix inside the bucket",
    )
    parser.add_argument(
        "--secure",
        default=os.getenv("MINIO_SECURE", "true"),
        type=_parse_bool,
        help="Use HTTPS (default: true)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        upload_to_minio(
            args.country,
            args.dataset_type,
            endpoint=args.endpoint,
            bucket=args.bucket,
            access_key=args.access_key,
            secret_key=args.secret_key,
            dest_prefix=args.dest_prefix,
            secure=args.secure,
        )
    except Exception as e:
        print(f"Upload failed: {e}")
        sys.exit(1)
