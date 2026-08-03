from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.constants import SetupConfig

ACLED_HDX_URL = "https://data.humdata.org/dataset/3e6bfc98-f837-495d-b8de-71e5ac026f59/resource/99a32d01-d0ca-4f57-a0f5-cb6b5f01f14f/download/political-violence-events-and-fatalities.xlsx"


@dg.asset(
    partitions_def=country_partitions,
    ins={"boundary_asset": dg.AssetIn()},
)
def acled_hrp_asset(context, config: SetupConfig, boundary_asset: str) -> str:
    """
    Downloads ACLED HRP data from HDX xlsx, filters by country ADM2 pcodes,
    and sums events and fatalities from 2018-2025.
    """
    country_code = context.partition_key.upper()
    base_path = Path(boundary_asset if boundary_asset else f"data/{country_code}")

    admin_levels = config.admin_levels
    if not admin_levels:
        raise ValueError("No admin_levels configured")

    lvl_num = int(admin_levels[0].replace("ADM", ""))
    fallback_levels = [f"ADM{n}" for n in range(lvl_num, -1, -1)]

    boundary_path = None
    level = None
    for test_level in fallback_levels:
        candidate = base_path / f"{country_code}_{test_level}.geojson"
        if candidate.exists():
            boundary_path = candidate
            level = test_level
            break

    if not boundary_path:
        context.log.error(
            f"[{country_code}] No boundary file found for {fallback_levels}"
        )
        raise FileNotFoundError(f"No boundary file found for {country_code}")

    gdf = gpd.read_file(boundary_path)
    id_col = f"{level.upper()}_PCODE"

    if id_col not in gdf.columns:
        candidate = next((c for c in gdf.columns if "pcode" in c.lower()), None)
        if candidate:
            id_col = candidate
        else:
            raise ValueError(f"No PCODE column found in {boundary_path}")

    adm2_pcodes = gdf[id_col].dropna().unique().tolist()
    context.log.info(f"[{country_code}] Found {len(adm2_pcodes)} {level} pcodes")

    xlsx_path = Path("downloads") / "acled_hrp.xlsx"

    if xlsx_path.exists():
        context.log.info(f"[{country_code}] Using cached ACLED HRP data: {xlsx_path}")
    else:
        context.log.info(
            f"[{country_code}] Downloading ACLED HRP data from {ACLED_HDX_URL}"
        )
        try:
            response = requests.get(ACLED_HDX_URL, timeout=300)
            response.raise_for_status()
            with open(xlsx_path, "wb") as f:
                f.write(response.content)
            context.log.info(f"[{country_code}] Saved ACLED HRP data to {xlsx_path}")
        except Exception as e:
            context.log.error(f"[{country_code}] Failed to download xlsx: {e}")
            raise

    try:
        df = pd.read_excel(xlsx_path, sheet_name="HRP_1")
    except Exception as e:
        context.log.error(f"[{country_code}] Failed to read xlsx: {e}")
        raise

    df.columns = df.columns.str.strip()
    pcode_col = "Admin2 Pcode"

    if pcode_col not in df.columns:
        context.log.error(
            f"Column '{pcode_col}' not found. Available columns: {df.columns.tolist()}"
        )
        raise ValueError(f"Missing column: {pcode_col}")

    country_rows = df[df[pcode_col].isin(adm2_pcodes)]
    context.log.info(
        f"[{country_code}] Filtered to {len(country_rows)} rows matching country pcodes"
    )

    if country_rows.empty:
        context.log.warning(f"[{country_code}] No matching rows found for country")
        output_dir = Path("data") / country_code / "Output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{country_code}_{level}_acled_hrp.csv"
        empty_cols = {id_col: adm2_pcodes}
        for yr in range(2018, 2026):
            suffix = str(yr)[-2:]
            empty_cols[f"acled_evts_{suffix}"] = pd.NA
            empty_cols[f"acled_fatl_{suffix}"] = pd.NA
        result = pd.DataFrame(empty_cols)
        result.to_csv(output_path, index=False)
        return str(output_path)

    country_rows["Year"] = pd.to_numeric(country_rows["Year"], errors="coerce")
    country_rows = country_rows[
        (country_rows["Year"] >= 2018) & (country_rows["Year"] <= 2025)
    ]

    all_pcodes = pd.DataFrame({id_col: adm2_pcodes})

    for yr in range(2018, 2026):
        suffix = str(yr)[-2:]
        yr_data = country_rows[country_rows["Year"] == yr]
        yr_agg = (
            yr_data.groupby(pcode_col)
            .agg(
                **{
                    f"acled_evts_{suffix}": ("Events", "sum"),
                    f"acled_fatl_{suffix}": ("Fatalities", "sum"),
                }
            )
            .reset_index()
            .rename(columns={pcode_col: id_col})
        )
        all_pcodes = all_pcodes.merge(yr_agg, on=id_col, how="left")

    result = all_pcodes

    evt_cols = [f"acled_evts_{str(yr)[-2:]}" for yr in range(2018, 2026)]
    fatl_cols = [f"acled_fatl_{str(yr)[-2:]}" for yr in range(2018, 2026)]

    weights = [2 ** (yr - 2018) for yr in range(2018, 2026)]

    result["acled_evts_w"] = sum(
        result[col].fillna(0) * w for col, w in zip(evt_cols, weights)
    )
    result["acled_fatl_w"] = sum(
        result[col].fillna(0) * w for col, w in zip(fatl_cols, weights)
    )

    context.log.info(f"[{country_code}] Output contains {len(result)} admin units")

    output_dir = Path("data") / country_code / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{country_code}_{level}_acled_hrp.csv"
    result.to_csv(output_path, index=False)
    context.log.info(f"[{country_code}] Saved ACLED HRP data to {output_path}")

    return str(output_path)
