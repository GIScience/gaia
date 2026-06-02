import os
import geopandas as gpd
import pandas as pd
import geemap
import ee
import yaml
import argparse



def load_years_from_config(config_path="configs/assets_config.yaml"):
    """Load years from crops_asset in assets_config.yaml"""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    years = cfg.get("crops_asset", {}).get("years", [])
    if not years or len(years) != 2:
        raise ValueError("assets_config.yaml must define crops_asset: years: [year1, year2]")
    return years[0], years[1]


def generate_crops_tif(country_code: str, target_year: int, target_scale: int = 100, grid_size_deg: float = 0.5):
    """Generates a binary crop TIF for the country bounds by downloading chunks from GEE."""
    import math
    import pickle
    import shapely.geometry
    import rasterio
    from rasterio.merge import merge

    country_boundary_file = f"data/{country_code}/{country_code}_ADM0.geojson"
    if not os.path.exists(country_boundary_file):
        print(f"Boundary file not found: {country_boundary_file}")
        return None

    output_tif = f"data/{country_code}/Temporary/{country_code}_crops_{target_year}.tif"
    results_file = f"data/{country_code}/Temporary/{country_code}_crops_{target_year}_raster_chunks.pkl"

    if os.path.exists(output_tif):
        print(f"Crops TIF already exists at {output_tif}. Skipping generation.")
        return output_tif

    os.makedirs(f"data/{country_code}/Temporary", exist_ok=True)

    # Initialize Earth Engine only if we actually need to generate the TIF
    ee.Initialize(project="aa-automatization")

    country_gdf = gpd.read_file(country_boundary_file).to_crs(epsg=4326)
    country_ee = geemap.geopandas_to_ee(country_gdf)

    xmin, ymin, xmax, ymax = country_gdf.total_bounds
    columns = math.ceil((xmax - xmin) / grid_size_deg)
    rows = math.ceil((ymax - ymin) / grid_size_deg)

    grid_cells = []
    for i in range(columns):
        for j in range(rows):
            x1 = xmin + i * grid_size_deg
            y1 = ymin + j * grid_size_deg
            x2 = x1 + grid_size_deg
            y2 = y1 + grid_size_deg
            grid_cells.append(shapely.geometry.box(x1, y1, x2, y2))

    grid_gdf = gpd.GeoDataFrame(geometry=grid_cells, crs="EPSG:4326")
    chunks_gdf = gpd.overlay(grid_gdf, country_gdf, how="intersection")

    total_chunks = len(chunks_gdf)
    print(f"Total spatial chunks to process for {country_code} crops TIF: {total_chunks}")

    start_date = f"{target_year}-01-01"
    end_date = f"{target_year}-12-31"

    dw_col = (ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
              .filterDate(start_date, end_date)
              .filterBounds(country_ee))

    dw_mode = dw_col.select('label').reduce(ee.Reducer.mode())
    crop_mask = dw_mode.eq(4).rename('crops').clip(country_ee)

    local_chunk_files = []
    start_idx = 0
    chunk_count = 0

    if os.path.exists(results_file):
        with open(results_file, "rb") as f:
            saved = pickle.load(f)
        local_chunk_files = saved.get("local_chunk_files", [])
        start_idx = saved.get("last_idx", 0)
        chunk_count = saved.get("chunk_count", 0)
        print(f"Resuming from chunk index {start_idx} ({chunk_count}/{total_chunks})")

    while start_idx < total_chunks:
        print(f"Processing spatial chunk {start_idx + 1}/{total_chunks} ...")

        chunk_geom = chunks_gdf.iloc[start_idx:start_idx+1]
        ee_chunk_geom = geemap.geopandas_to_ee(chunk_geom).geometry()

        chunk_tif = f"data/{country_code}/Temporary/temp_crop_{target_year}_chunk_{start_idx}.tif"

        try:
            geemap.ee_export_image(
                crop_mask,
                filename=chunk_tif,
                scale=target_scale,
                region=ee_chunk_geom,
                file_per_band=False
            )

            if os.path.exists(chunk_tif):
                local_chunk_files.append(chunk_tif)

                with open(results_file, "wb") as f:
                    pickle.dump({
                        "local_chunk_files": local_chunk_files,
                        "last_idx": start_idx + 1,
                        "chunk_count": chunk_count + 1
                    }, f)

        except Exception as e:
            print(f"Error processing chunk index {start_idx}: {e}")
            if os.path.exists(chunk_tif):
                os.remove(chunk_tif)

        start_idx += 1
        chunk_count += 1

    if not local_chunk_files:
        print("No chunks were successfully downloaded. Returning.")
        return None

    print(f"Merging {len(local_chunk_files)} raster chunks into final mosaic...")
    src_files_to_mosaic = []
    for fp in local_chunk_files:
        if os.path.exists(fp):
            src_files_to_mosaic.append(rasterio.open(fp))

    if src_files_to_mosaic:
        mosaic, out_trans = merge(src_files_to_mosaic)

        out_meta = src_files_to_mosaic[0].meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
            "crs": src_files_to_mosaic[0].crs
        })

        with rasterio.open(output_tif, "w", **out_meta) as dest:
            dest.write(mosaic)

        for src in src_files_to_mosaic:
            src.close()

        print(f"Successfully generated cohesive map: {output_tif}")

    for fp in local_chunk_files:
        if os.path.exists(fp):
            os.remove(fp)
    if os.path.exists(results_file):
        os.remove(results_file)

    return output_tif


def process_crops_for_admin(country_code: str, admin_level: str, config_path="configs/assets_config.yaml") -> str:
    """Run crop coverage calculation for a given country/admin level and return output CSV path."""

    year_prev, year_curr = load_years_from_config(config_path)

    # Generate the crops TIF for both target years
    tif_prev = generate_crops_tif(country_code, target_year=year_prev)
    tif_curr = generate_crops_tif(country_code, target_year=year_curr)

    gdf = gpd.read_file(f"data/{country_code}/{country_code}_{admin_level}.geojson")

    # Compute area in km2 (using equal area projection CEA)
    gdf_area = gdf.to_crs("+proj=cea")
    area_km2 = gdf_area.geometry.area / 10**6

    df = pd.DataFrame()
    df[f"{admin_level.upper()}_PCODE"] = gdf[f"{admin_level.upper()}_PCODE"]

    if tif_prev and os.path.exists(tif_prev):
        from rasterstats import zonal_stats
        stats_prev = zonal_stats(gdf, tif_prev, stats="mean", nodata=-9999)
        df[f"crops_{year_prev}_pct"] = [ (s["mean"] * 100) if s["mean"] is not None else 0 for s in stats_prev ]
    else:
        df[f"crops_{year_prev}_pct"] = 0

    if tif_curr and os.path.exists(tif_curr):
        from rasterstats import zonal_stats
        stats_curr = zonal_stats(gdf, tif_curr, stats="mean", nodata=-9999)
        df[f"crops_{year_curr}_pct"] = [ (s["mean"] * 100) if s["mean"] is not None else 0 for s in stats_curr ]
    else:
        df[f"crops_{year_curr}_pct"] = 0

    df["crops_diff_pctpts"] = df[f"crops_{year_curr}_pct"] - df[f"crops_{year_prev}_pct"]
    df["crops_diff_km2"] = df["crops_diff_pctpts"] * area_km2 / 100

    # Calculate relative change pct safely (avoid division by zero)
    def calc_rel_change(prev, diff):
        if prev == 0:
            return None
        return (diff / prev) * 100

    df["crops_change_rel_pct"] = df.apply(
        lambda row: calc_rel_change(row[f"crops_{year_prev}_pct"], row["crops_diff_pctpts"]), 
        axis=1
    )

    # Ensure Output folder exists
    os.makedirs(f"data/{country_code}/Output", exist_ok=True)
    output_csv = f"data/{country_code}/Output/{country_code}_{admin_level}_crops.csv"

    col_order = [
        f"{admin_level.upper()}_PCODE",
        "ADM_PCODE",  # duplicate column for consistent schema
        f"crops_{year_prev}_pct",
        f"crops_{year_curr}_pct",
        "crops_diff_km2",
        "crops_diff_pctpts",
        "crops_change_rel_pct",
    ]

    df = df[col_order]
    df[col_order[1:]] = df[col_order[1:]].round(2)
    df.to_csv(output_csv, index=False)
    
    return output_csv

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch crops coverage stats from GEE Dynamic World (two years from assets_config.yaml)"
    )
    parser.add_argument("country_code", type=str, help="Country code (e.g., STP)")
    parser.add_argument("admin_level", type=str, help="Admin level (e.g., ADM2)")
    parser.add_argument(
        "--config", type=str, default="configs/assets_config.yaml", help="Path to assets_config.yaml"
    )
    args = parser.parse_args()

    output_csv = process_crops_for_admin(args.country_code, args.admin_level, config_path=args.config)
    print(f"Crops calculation complete. Output: {output_csv}")