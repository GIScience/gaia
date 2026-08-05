import os

import dagster as dg


class S3Resource(dg.ConfigurableResource):
    endpoint: str = os.getenv("S3_ENDPOINT", "hot.storage.heigit.org")
    bucket: str = os.getenv("S3_BUCKET", "heigit-hdx-public")
    access_key: str = os.getenv("S3_ACCESS_KEY")
    secret_key: str = os.getenv("S3_SECRET_KEY")
    dest_prefix: str = os.getenv("S3_DEST_PREFIX", "risk_assessment_inputs")
    secure: bool = os.getenv("S3_SECURE", "true").lower() == "true"

    def upload(self, country: str, dataset_type: str) -> None:
        from gaia.scripts.upload_s3 import upload_to_s3

        upload_to_s3(
            country=country,
            dataset_type=dataset_type,
            endpoint=self.endpoint,
            bucket=self.bucket,
            access_key=self.access_key,
            secret_key=self.secret_key,
            dest_prefix=self.dest_prefix,
            secure=self.secure,
        )


class HdxResource(dg.ConfigurableResource):
    site: str = os.getenv("HDX_SITE", "prod")
    api_key: str = os.getenv("HDX_API_KEY")
    owner_org: str = os.getenv(
        "HDX_OWNER_ORG", "heidelberg-institute-for-geoinformation-technology"
    )
    data_update_frequency: str = os.getenv(
        "HDX_DATA_UPDATE_FREQUENCY", "Every six months"
    )
    maintainer: str = os.getenv("HDX_MAINTAINER", "valentin-boehmer-8808")
    maintainer_email: str = os.getenv(
        "HDX_MAINTAINER_EMAIL", "valentin.boehmer@heigit.org"
    )
    private: bool = os.getenv("HDX_PRIVATE", "false").lower() == "true"

    def smart_upload(self, country_code: str, file_map: dict, context) -> str:
        from gaia.scripts.upload_to_hdx import smart_upload_to_hdx

        return smart_upload_to_hdx(
            country_code=country_code,
            file_map=file_map,
            hdx_config=self,
            context=context,
        )


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "s3": S3Resource(),
            "hdx": HdxResource(),
        }
    )
