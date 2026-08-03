import yaml
from importlib.resources import files

import dagster as dg

_countries = yaml.safe_load(
    files("gaia.configs").joinpath("hdx_countries.yaml").read_text()
)
ALL_COUNTRIES = list(_countries.keys())
country_partitions = dg.StaticPartitionsDefinition(partition_keys=ALL_COUNTRIES)

category_partitions = dg.StaticPartitionsDefinition(
    [
        "demographics",
        "facilities",
        "ndvi",
        "crops",
        "rural",
        "access",
        "coping",
        "vulnerability",
        "exposure",
        "rai",
    ]
)

multi_partitions = dg.MultiPartitionsDefinition(
    {
        "country": country_partitions,
        "category": category_partitions,
    }
)
