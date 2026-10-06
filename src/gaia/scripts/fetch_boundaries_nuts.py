import os
import sys

import geopandas as gpd

from gaia.defs.constants import (
    GISCO_NUTS_URL,
    NUTS_EXCLUDED_PREFIXES,
    NUTS_LEVELS,
    NUTS_YEAR,
)
from gaia.defs.partitions import NUTS_ADM1_LEVELS, NUTS_COUNTRIES
from gaia.scripts.download_utils import download_file


def nuts_levels(country_code):
    """Admin level -> NUTS level of a NUTS country (ADM0 is NUTS0)."""
    levels = {"ADM0": 0, **NUTS_LEVELS}
    if country_code in NUTS_ADM1_LEVELS:
        levels["ADM1"] = NUTS_ADM1_LEVELS[country_code]
    return levels


def published_id_columns(country_code):
    """Renames applied to the ID columns of published files (S3 CSVs, risk
    parquet, PMTiles): NUTS countries carry NUTS codes, not OCHA pcodes, e.g.
    ADM2_PCODE -> NUTS3_CODE. Internally the pipeline always uses *_PCODE.
    Empty for OCHA countries."""
    if country_code not in NUTS_COUNTRIES:
        return {}
    renames = {
        f"{adm}_PCODE": f"NUTS{level}_CODE"
        for adm, level in nuts_levels(country_code).items()
    }
    renames["ADM_PCODE"] = "NUTS_CODE"
    return renames


def load_nuts_level(level, country_code):
    """Europe-wide NUTS file for `level` (cached in downloads/), filtered to one country."""
    filename = f"NUTS_RG_01M_{NUTS_YEAR}_4326_LEVL_{level}.geojson"
    path = os.path.join("downloads", filename)
    if not os.path.exists(path):
        os.makedirs("downloads", exist_ok=True)
        print(f"Downloading: {filename}")
        download_file(f"{GISCO_NUTS_URL}/{filename}", path)

    gdf = gpd.read_file(path)
    gdf = gdf[gdf["ISO3_CODE"] == country_code]
    gdf = gdf[~gdf["NUTS_ID"].str.startswith(NUTS_EXCLUDED_PREFIXES)]
    # A few NUTS 2024 rings self-intersect (e.g. DE27, DE929)
    return gdf.set_geometry(gdf.geometry.make_valid())


def write_level(gdf, country_code, adm, code_col, name_col):
    out = gpd.GeoDataFrame(
        {
            f"{adm}_PCODE": gdf[code_col],
            f"{adm}_EN": gdf[name_col].str.strip(),
        },
        geometry=gdf.geometry,
        crs=gdf.crs,
    )
    path = os.path.join("data", country_code, f"{country_code}_{adm}.geojson")
    out.to_file(path, driver="GeoJSON")
    print(f"Wrote {len(out)} {adm} units -> {path}")


def download_nuts_boundaries(country_code):
    """Writes data/<ISO3>/<ISO3>_ADM{0,1,2}.geojson from Eurostat GISCO NUTS,
    using NUTS_ID as PCODE."""
    country_code = country_code.upper()
    os.makedirs(os.path.join("data", country_code), exist_ok=True)

    levels = {
        adm: load_nuts_level(lvl, country_code)
        for adm, lvl in nuts_levels(country_code).items()
        if adm != "ADM0"
    }
    if levels["ADM2"].empty:
        raise ValueError(f"{country_code}: no NUTS regions found")

    # Dissolved from ADM1 rather than read from NUTS0, so excluded overseas
    # regions also stay out of the country outline.
    adm0 = levels["ADM1"].dissolve(by="CNTR_CODE", as_index=False)
    write_level(adm0, country_code, "ADM0", "CNTR_CODE", "NAME_ENGL")
    for adm, gdf in levels.items():
        write_level(gdf, country_code, adm, "NUTS_ID", "NAME_LATN")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a country code, e.g., `python fetch_boundaries_nuts.py DEU`")
        sys.exit(1)

    download_nuts_boundaries(sys.argv[1])
