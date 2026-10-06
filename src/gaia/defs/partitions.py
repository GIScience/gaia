import yaml
from importlib.resources import files

import dagster as dg

_countries = yaml.safe_load(
    files("gaia.configs").joinpath("countries.yaml").read_text()
)
ALL_COUNTRIES = list(_countries.keys())
NUTS_COUNTRIES = {
    code for code, cfg in _countries.items() if cfg.get("boundary_source") == "nuts"
}
NUTS_ADM1_LEVELS = {
    code: cfg["nuts_adm1_level"]
    for code, cfg in _countries.items()
    if "nuts_adm1_level" in cfg
}
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
        "evacuability",
    ]
)

multi_partitions = dg.MultiPartitionsDefinition(
    {
        "country": country_partitions,
        "category": category_partitions,
    }
)
