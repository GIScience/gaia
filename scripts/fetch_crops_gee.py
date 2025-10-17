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


def process_crops_for_admin(country_code: str, admin_level: str, config_path="configs/assets_config.yaml") -> str:
    """Run crop coverage calculation for a given country/admin level and return output CSV path."""

    # Initialize Earth Engine
    ee.Initialize(project="aa-automatization")

    year_prev, year_curr = load_years_from_config(config_path)

    gdf = gpd.read_file(f"data/{country_code}/{country_code}_{admin_level}.geojson")

    chunk_size = 20
    start_idx = 0
    
    # Ensure Output folder exists
    os.makedirs(f"data/{country_code}/Output", exist_ok=True)
    output_csv = f"data/{country_code}/Output/{country_code}_{admin_level}_crops.csv"

    if os.path.exists(output_csv):
        os.remove(output_csv)

    col_order = [
        f"{admin_level.upper()}_PCODE",
        f"crops_{year_prev}_pct",
        f"crops_{year_curr}_pct",
        "crops_diff_km2",
        "crops_diff_pctpts",
        "crops_change_rel_pct",
    ]

    dw = ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")

    def yearly_composite(y, geom):
        coll = (
            dw.filterDate(f"{y}-01-01", f"{y}-12-31")
            .filterBounds(geom)
            .select("label")
        )
        return coll.reduce(ee.Reducer.mode())

    while start_idx < len(gdf):
        end_idx = start_idx + chunk_size
        gdf_chunk = gdf.iloc[start_idx:end_idx]

        try:
            fc = geemap.geopandas_to_ee(gdf_chunk)

            def add_crops_stats(feature):
                geom = feature.geometry()
                comp_prev = yearly_composite(year_prev, geom)
                comp_curr = yearly_composite(year_curr, geom)

                polygon_area_km2 = ee.Number(geom.area()).divide(1e6)

                crop_prev_raw = comp_prev.eq(4).rename("crops").reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=10,
                    bestEffort=True,
                ).get("crops")

                crop_curr_raw = comp_curr.eq(4).rename("crops").reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=geom,
                    scale=10,
                    bestEffort=True,
                ).get("crops")

                crop_prev_perc = ee.Number(ee.Algorithms.If(crop_prev_raw, crop_prev_raw, 0)).multiply(100)
                crop_curr_perc = ee.Number(ee.Algorithms.If(crop_curr_raw, crop_curr_raw, 0)).multiply(100)

                crops_perc_point_dif = crop_curr_perc.subtract(crop_prev_perc)
                crops_abs_dif_km2 = crops_perc_point_dif.multiply(polygon_area_km2).divide(100)

                crops_rel_change_perc = ee.Algorithms.If(
                    crop_prev_perc.neq(0),
                    crops_perc_point_dif.divide(crop_prev_perc).multiply(100),
                    None,
                )

                return feature.set({
                    f"crops_{year_prev}_pct": crop_prev_perc,
                    f"crops_{year_curr}_pct": crop_curr_perc,
                    "crops_diff_km2": crops_abs_dif_km2,
                    "crops_diff_pctpts": crops_perc_point_dif,
                    "crops_change_rel_pct": crops_rel_change_perc,
                })

            fc_with_stats = fc.map(add_crops_stats)
            fc_filtered = fc_with_stats.select(
                propertySelectors=col_order, retainGeometry=False
            )

            temp_csv = f"data/{country_code}/{country_code}_crops_{admin_level}_temp_chunk.csv"
            geemap.ee_to_csv(fc_filtered, filename=temp_csv)

            df_chunk = pd.read_csv(temp_csv)
            df_chunk = df_chunk[[c for c in col_order if c in df_chunk.columns]]
            df_chunk[col_order[1:]] = df_chunk[col_order[1:]].round(2)

            if start_idx == 0 and not os.path.exists(output_csv):
                df_chunk.to_csv(output_csv, index=False)
            else:
                df_chunk.to_csv(output_csv, mode="a", header=False, index=False)

            os.remove(temp_csv)
            start_idx = end_idx

        except Exception:
            start_idx = end_idx

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