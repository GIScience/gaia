import geopandas as gpd
import geemap
import ee
import pandas as pd
import os
import yaml
import argparse

def load_year_from_config(config_path="configs/assets_config.yaml"):
    """Load year from ndvi_asset in assets_config.yaml"""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    year = cfg.get("ndvi_asset", {}).get("year", [])
    if not year or len(year) != 1:
        raise ValueError("assets_config.yaml must define ndvi_asset: year: [year]")
    return year[0]



def process_ndvi_for_admin(country_code: str, admin_level: str, config_path="configs/assets_config.yaml") -> str:
    """Run NDVI calculation for a given country/admin level and return output CSV path."""
    # Initialize Earth Engine with your project
    ee.Initialize(project='aa-automatization')

    # Load your local multipolygon GeoJSON/Shapefile
    gdf = gpd.read_file(f"data/{country_code}/{country_code}_{admin_level}.geojson")

    chunk_size = 20
    start_idx = 0
    year = load_year_from_config(config_path)

    # Ensure Output folder exists
    os.makedirs(f"data/{country_code}/Output", exist_ok=True)
    output_csv = f"data/{country_code}/Output/{country_code}_{admin_level}_ndvi.csv"

    if os.path.exists(output_csv):
        os.remove(output_csv)

    # Desired output column order
    col_order = [
        f"{admin_level.upper()}_PCODE",
        "NDVI_mean", "NDVI_median", "NDVI_p25", "NDVI_p75",
        "NDVI_high_sqkm", "NDVI_medium_sqkm", "NDVI_low_sqkm"
    ]

    while start_idx < len(gdf):
        end_idx = start_idx + chunk_size
        gdf_chunk = gdf.iloc[start_idx:end_idx]

        try:
            fc = geemap.geopandas_to_ee(gdf_chunk)

            # Landsat NDVI collection
            landsat = (ee.ImageCollection("LANDSAT/COMPOSITES/C02/T1_L2_8DAY_NDVI")
                    .filterDate(f"{year}-01-01", f"{year}-12-31")
                    .filterBounds(fc))

            ndvi_image = landsat.select("NDVI").median()

            # Mask NDVI for each category
            high_ndvi_image = ndvi_image.rename("NDVI_high_sqkm")
            high_ndvi_mask = high_ndvi_image.updateMask(high_ndvi_image.gte(0.6).And(high_ndvi_image.lte(1)))

            medium_ndvi_image = ndvi_image.rename("NDVI_medium_sqkm")
            medium_ndvi_mask = medium_ndvi_image.updateMask(medium_ndvi_image.gte(0.1).And(medium_ndvi_image.lt(0.6)))

            low_ndvi_image = ndvi_image.rename("NDVI_low_sqkm")
            low_ndvi_mask = low_ndvi_image.updateMask(low_ndvi_image.gte(-1).And(low_ndvi_image.lt(0.1)))

            def add_stats(feature):
                # Basic stats
                stats = ndvi_image.reduceRegion(
                    reducer=ee.Reducer.mean()
                            .combine(ee.Reducer.median(), "", True)
                            .combine(ee.Reducer.percentile([25, 75]), "", True),
                    geometry=feature.geometry(),
                    scale=30,
                    bestEffort=True
                )

                # Count pixels per category
                high_count = high_ndvi_mask.reduceRegion(
                    reducer=ee.Reducer.count(),
                    geometry=feature.geometry(),
                    scale=30,
                    bestEffort=True
                )
                medium_count = medium_ndvi_mask.reduceRegion(
                    reducer=ee.Reducer.count(),
                    geometry=feature.geometry(),
                    scale=30,
                    bestEffort=True
                )
                low_count = low_ndvi_mask.reduceRegion(
                    reducer=ee.Reducer.count(),
                    geometry=feature.geometry(),
                    scale=30,
                    bestEffort=True
                )

                # Multiply counts by 900 and divide by 1,000,000 to get km²
                high_count = ee.Dictionary(high_count).map(lambda k, v: ee.Number(v).multiply(900).divide(1000000))
                medium_count = ee.Dictionary(medium_count).map(lambda k, v: ee.Number(v).multiply(900).divide(1000000))
                low_count = ee.Dictionary(low_count).map(lambda k, v: ee.Number(v).multiply(900).divide(1000000))

                # Merge all stats
                all_stats = stats.combine(high_count).combine(medium_count).combine(low_count)
                return feature.set(all_stats)

            fc_with_stats = fc.map(add_stats)
            fc_filtered = fc_with_stats.select(**{
                'propertySelectors': col_order,
                'retainGeometry': False
            })

            # Export to CSV
            temp_csv = f"data/{country_code}/{country_code}_ndvi_{admin_level}_temp_chunk.csv"
            geemap.ee_to_csv(fc_filtered, filename=temp_csv)

            # Read back and reorder columns explicitly
            df_chunk = pd.read_csv(temp_csv)
            df_chunk = df_chunk[[c for c in col_order if c in df_chunk.columns]]

            # Columns to round
            round_cols = ["NDVI_mean", "NDVI_median", "NDVI_p25", "NDVI_p75"]
            # Round only these columns
            df_chunk[round_cols] = df_chunk[round_cols].round(2)

            if start_idx == 0 and not os.path.exists(output_csv):
                df_chunk.to_csv(output_csv, index=False)
            else:
                df_chunk.to_csv(output_csv, mode='a', header=False, index=False)

            os.remove(temp_csv)

            print(f"Processed chunk {start_idx}-{end_idx} (size {chunk_size})")
            start_idx = end_idx

        except ee.EEException as e:
            msg = str(e)
            if "Request payload size exceeds the limit" in msg:
                if chunk_size > 1:
                    chunk_size = max(1, chunk_size // 2)
                    print(f"Payload too large. Reducing chunk size to {chunk_size} and retrying index {start_idx}...")
                else:
                    print(f"Chunk size 1 still too large. Skipping index {start_idx}.")
                    start_idx += 1
            else:
                print(f"EEException: {e}. Skipping chunk.")
                start_idx = end_idx
        except FileNotFoundError:
            print(f"Temp CSV not found. Retrying index {start_idx} with smaller chunk...")
            if chunk_size > 1:
                chunk_size = max(1, chunk_size // 2)
            else:
                start_idx += 1
        except Exception as e:
            print(f"Unexpected error: {e}. Skipping chunk {start_idx}-{end_idx}.")
            start_idx = end_idx

    return output_csv

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch NDVI stats from GEE")
    parser.add_argument("country_code", type=str, help="Country code (e.g., STP)")
    parser.add_argument("admin_level", type=str, help="Admin level (e.g., ADM2)")
    args = parser.parse_args()

    output_csv = process_ndvi_for_admin(args.country_code, args.admin_level)
    print(f"NDVI calculation complete. Output: {output_csv}")