import os
from pathlib import Path
from typing import List

import pandas as pd
import dagster as dg

from gaia.defs.partitions import country_partitions
from gaia.defs.utils import dedupe_adm_pcode


@dg.asset(
    deps=["access_asset", "facilities_asset", "evacuability_asset", "RAI_asset"],
    partitions_def=country_partitions,
)
def coping_asset(
    context,
    access_asset: List[str],
    facilities_asset: List[str],
    evacuability_asset: List[str],
    RAI_asset: List[str],
) -> list[str]:
    """
    Combine accessibility, facilities, evacuability, and RAI CSVs into a single
    coping dataset. Joins on the ADM*_PCODE column per admin level.
    Produces one coping CSV per admin level in Output/.
    """
    country_code = context.partition_key.upper()
    outputs = []

    # Pair up by index — all asset outputs are ordered by admin level
    for i, access_csv in enumerate(access_asset):
        if i >= len(facilities_asset):
            break
        facilities_csv = facilities_asset[i]
        evac_csv = evacuability_asset[i] if i < len(evacuability_asset) else None
        rai_csv = RAI_asset[i] if i < len(RAI_asset) else None

        if not os.path.exists(access_csv) or not os.path.exists(facilities_csv):
            context.log.warning(
                f"Skipping merge for {country_code}: missing files {access_csv}, {facilities_csv}"
            )
            continue

        try:
            df_access = pd.read_csv(access_csv)
            df_facilities = pd.read_csv(facilities_csv)

            # detect admin code column automatically (ADM0_PCODE, ADM1_PCODE, etc.)
            id_col = [c for c in df_access.columns if c.endswith("_PCODE")]
            if not id_col:
                context.log.warning(f"Skipping {access_csv}: no *_PCODE column found")
                continue
            id_col = id_col[0]

            merged = pd.merge(df_access, df_facilities, on=id_col, how="left")

            # Merge evacuability CSV if available
            if evac_csv and os.path.exists(evac_csv):
                df_evac = pd.read_csv(evac_csv)
                if id_col in df_evac.columns:
                    evac_cols = [c for c in df_evac.columns if c != id_col]
                    if evac_cols:
                        merged = pd.merge(
                            merged, df_evac[[id_col] + evac_cols], on=id_col, how="left"
                        )
                        context.log.info(
                            f"[{country_code}] Merged evacuability data from {evac_csv}"
                        )
                else:
                    context.log.warning(
                        f"[{country_code}] Evacuability CSV missing ID column '{id_col}', skipping"
                    )

            # Merge RAI CSV if available
            if rai_csv and os.path.exists(rai_csv):
                df_rai = pd.read_csv(rai_csv)
                if id_col in df_rai.columns:
                    rai_cols = [c for c in df_rai.columns if c != id_col]
                    if rai_cols:
                        merged = pd.merge(
                            merged, df_rai[[id_col] + rai_cols], on=id_col, how="left"
                        )
                        context.log.info(
                            f"[{country_code}] Merged RAI data from {rai_csv} ({len(rai_cols)} cols)"
                        )
                else:
                    context.log.warning(
                        f"[{country_code}] RAI CSV missing ID column '{id_col}', skipping"
                    )

            # --- keep only one ADM_PCODE column ---
            merged = dedupe_adm_pcode(merged)

            admin_level = id_col.split("_")[0]
            output_dir = Path("data") / country_code / "Output"
            output_dir.mkdir(parents=True, exist_ok=True)

            output_path = output_dir / f"{country_code}_{admin_level}_coping.csv"
            merged.to_csv(output_path, index=False)
            outputs.append(str(output_path))

            context.log.info(
                f"[{country_code}] Wrote coping CSV: {output_path} ({len(merged.columns)} cols)"
            )

        except Exception as e:
            context.log.warning(f"Failed to merge for {country_code}: {e}")

    if not outputs:
        context.log.warning(f"No coping outputs created for {country_code}")
    return outputs
