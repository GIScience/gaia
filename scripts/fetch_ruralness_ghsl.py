import os
import shutil
import zipfile
import requests
from pathlib import Path
import rioxarray
import rasterio
import pandas as pd
import numpy as np
import argparse
import logging
import geopandas as gpd

from rasterstats import zonal_stats
from scripts.fetch_worldpop import fetch_worldpop

RECLASS_MAP = {
    10: None,
    11: 1, 12: 1, 13: 1,   # rural
    21: 2, 22: 2, 23: 2, 30: 2,  # urban
}
SMOD_ZIP_URL = (
    "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/"
    "GHS_SMOD_GLOBE_R2023A/GHS_SMOD_E2030_GLOBE_R2023A_54009_1000/"
    "V2-0/GHS_SMOD_E2030_GLOBE_R2023A_54009_1000_V2_0.zip"
)


def download_and_unzip_smod(work_dir, context):
    """Download SMOD ZIP, unzip, return path to TIF."""
    zip_path = Path(work_dir) / "smod.zip"
    unzip_dir = Path(work_dir) / "unzipped"
    unzip_dir.mkdir(parents=True, exist_ok=True)

    if not zip_path.exists():
        context.info("Downloading SMOD global raster…")
        resp = requests.get(SMOD_ZIP_URL, stream=True)
        resp.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(1024 * 1024):
                f.write(chunk)
    else:
        context.info("SMOD ZIP already exists, skipping download.")

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(unzip_dir)

    for root, _, files in os.walk(unzip_dir):
        for fn in files:
            if fn.lower().endswith(".tif"):
                return zip_path, unzip_dir, Path(root) / fn

    raise FileNotFoundError("No .tif found in SMOD ZIP")


def reclassify_raster(in_tif, out_tif, reclass_map, context):
    with rasterio.open(in_tif) as src:
        meta = src.meta.copy()
        arr = src.read(1)

        nodata_val = 0
        out = np.full(arr.shape, nodata_val, dtype=np.uint8)

        for old, new in reclass_map.items():
            mask_val = (arr == old)
            if new is None:
                out[mask_val] = nodata_val
            else:
                out[mask_val] = new

        meta.update(count=1, dtype="uint8", nodata=nodata_val)
        with rasterio.open(out_tif, "w", **meta) as dst:
            dst.write(out, 1)
    context.info(f"Reclassified raster saved to {out_tif}")


def compute_rural_population(country_code, admin_level, gdf, work_dir, output_dir, context):
    """
    Compute rural population counts by admin unit for each indicator.
    Generates smod_reclass.tif if missing. Skips if output CSV already exists.
    Adds percentage columns (_rural_perc) for each indicator.
    """
    country_code = country_code.upper()
    work_dir = Path(f"{work_dir}/downloads")
    temp_dir = Path(f"{work_dir}/data/{country_code}/Temporary")
    output_dir = Path(output_dir)
    reclass_tif = work_dir / "smod_reclass.tif"
    out_csv = output_dir / f"{country_code}_{admin_level}_rural_population.csv"

    # --- skip if CSV exists ---
    if out_csv.exists():
        context.info(f"CSV already exists, skipping: {out_csv}")
        return str(out_csv)

    zip_path = None
    unzip_dir = None

    try:
        # --- ensure smod_reclass.tif exists ---
        if not reclass_tif.exists():
            context.info("smod_reclass.tif missing — will generate it")
            zip_path, unzip_dir, smod_tif = download_and_unzip_smod(work_dir, context)
            reclassify_raster(smod_tif, reclass_tif, RECLASS_MAP, context)
        else:
            context.info("Using existing smod_reclass.tif")

        # Ensure WorldPop files exist
        context.info(f"Ensuring demographic rasters exist in {temp_dir}...")
        indicator_tifs = fetch_worldpop(country_code)
        indicators = ["female_pop", "children_u5", "female_u5", "elderly", "pop_u15", "female_u15"]
        tif_map = dict(zip(indicators, indicator_tifs))

        # --- load SMOD raster ---
        smod = rioxarray.open_rasterio(reclass_tif, masked=True).squeeze()

        rural_df = pd.DataFrame({f"{admin_level}_PCODE": gdf[f"{admin_level}_PCODE"]})

        # Store total population sums
        total_pop_counts = {}

        for label in indicators:
            pop_raster_path = tif_map[label]
            pop_raster = rioxarray.open_rasterio(pop_raster_path, masked=True).squeeze()

            # Align SMOD raster to population raster
            smod_aligned = smod.rio.reproject_match(pop_raster, resampling=rasterio.enums.Resampling.nearest)
            rural_mask = (smod_aligned == 1).astype("float32")
            rural_pop = pop_raster * rural_mask

            tmp_rural = work_dir / f"tmp_rural_{label}.tif"
            with rasterio.open(pop_raster_path) as src:
                meta = src.meta.copy()
                meta.update(compress="lzw", tiled=True,
                            bigtiff="yes" if src.width * src.height > 2**32 else "no")
            with rasterio.open(tmp_rural, "w", **meta) as dst:
                dst.write(rural_pop.values, 1)

            # Rural population sums per admin unit
            stats = zonal_stats(gdf, tmp_rural, stats="sum", nodata=0)
            rural_df[f"{label}_rural"] = [s["sum"] if s["sum"] is not None else 0 for s in stats]

            # Total population sums per admin unit
            total_stats = zonal_stats(gdf, pop_raster_path, stats="sum", nodata=0)
            total_pop_counts[label] = [s["sum"] if s["sum"] is not None else 0 for s in total_stats]

            context.info(f"Processed rural population for {label}")

        # --- calculate one overall rural percentage column ---
        # Use total population (e.g. pop_u15 as proxy, or sum of all groups)
        total_pop = pd.Series(total_pop_counts["female_pop"]).replace({0: np.nan})
        rural_df["rural_pop_perc"] = (
            rural_df["female_pop_rural"] / total_pop * 100
        ).fillna(0).round(2)

        # --- finalize ---
        count_cols = [c for c in rural_df.columns if c.endswith("_rural")]
        perc_cols = [c for c in rural_df.columns if c.endswith("_rural_perc")]

        # Counts → integers
        rural_df[count_cols] = rural_df[count_cols].fillna(0).round(0).astype(int)

        # Percentages → keep 2 decimals
        rural_df[perc_cols] = rural_df[perc_cols].fillna(0).round(2)

        output_dir.mkdir(parents=True, exist_ok=True)
        rural_df.to_csv(out_csv, index=False)
        context.info(f"Rural population CSV written to {out_csv}")

    finally:
        # cleanup
        if zip_path and zip_path.exists():
            zip_path.unlink()
            context.info(f"Deleted {zip_path}")
        if unzip_dir and unzip_dir.exists():
            shutil.rmtree(unzip_dir)
            context.info(f"Deleted {unzip_dir}")
        
                # cleanup tmp_rural rasters
        for tmp_file in work_dir.glob("tmp_rural_*.tif"):
            try:
                tmp_file.unlink()
                context.info(f"Deleted {tmp_file}")
            except Exception as e:
                context.info(f"Failed to delete {tmp_file}: {e}")

    return str(out_csv)


if __name__ == "__main__":
    # --- argument parsing ---
    parser = argparse.ArgumentParser(
        description="Compute rural population counts by administrative units."
    )
    parser.add_argument("country_code", help="Country code (ISO3)")
    parser.add_argument("admin_level", help="Administrative level (e.g., ADM1, ADM2)")
    parser.add_argument(
        "--temp-dir",
        default="temp",
        help="Temporary working directory (default: ./temp)",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory (default: ./output)",
    )
    args = parser.parse_args()

    country_code = args.country_code.upper()
    admin_level = args.admin_level
    temp_dir = f"data/{country_code}/Temporary"
    output_dir = f"data/{country_code}/Output"

    # --- logging context ---
    class Context:
        @staticmethod
        def info(msg):
            logging.info(msg)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    context = Context()

    try:
        # --- locate admin GeoJSON ---
        admin_geojson = Path(f"data/{country_code}/{country_code}_{admin_level}.geojson")
        if not admin_geojson.exists():
            context.info(f"ERROR: Admin file not found: {admin_geojson}")
            exit(1)

        gdf = gpd.read_file(admin_geojson)

        csv_path = compute_rural_population(
            country_code, admin_level, gdf, temp_dir, output_dir, context
        )

        context.info(f"Finished. Output CSV: {csv_path}")

    except Exception as e:
        context.info(f"ERROR: {e}")
        exit(1)