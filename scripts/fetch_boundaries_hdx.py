import os
import sys
import requests
import zipfile
import shutil
import tempfile
import geopandas as gpd

def get_dataset_resources(dataset_id):
    print(f"Fetching dataset metadata for: {dataset_id}")
    api_url = f"https://data.humdata.org/api/3/action/package_show?id={dataset_id}"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GaiaDownloader/1.0)"}
    r = requests.get(api_url, headers=headers)
    if r.status_code == 404:
        print(f"Dataset '{dataset_id}' not found on HDX. Please check the country code.")
        sys.exit(1)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        sys.exit(1)
    data = r.json()
    if not data.get("success"):
        print("Failed to get dataset info from HDX.")
        sys.exit(1)
    return data["result"]["resources"]

def download_file(url, save_path):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; GaiaDownloader/1.0)"}
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    print(f"Saved to: {save_path}")

def convert_shapefiles_to_geojson(input_folder, base_output_folder, country_code,
                                  simplify_tolerance=0.0001):
    """
    Convert shapefiles to simplified GeoJSON.
    - simplify_tolerance: higher = more simplified, smaller file
    """
    country_code = country_code.upper()
    output_folder = os.path.join(base_output_folder, country_code)
    os.makedirs(output_folder, exist_ok=True)

    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.lower().endswith(".shp"):
                shp_path = os.path.join(root, file)
                try:
                    gdf = gpd.read_file(shp_path)
                    basename = os.path.splitext(file)[0]

                    # Detect admin level (ADM0–ADM4)
                    level = None
                    for i in range(5):
                        if f"adm{i}" in basename.lower() or f"admin{i}" in basename.lower():
                            level = f"ADM{i}"
                            break

                    if not level:
                        print(f"Skipping {file} (unknown admin level)")
                        continue

                    # Simplify geometry
                    gdf_simplified = gdf.copy()
                    gdf_simplified["geometry"] = gdf.geometry.simplify(
                        tolerance=simplify_tolerance,
                        preserve_topology=True
                    )

                    # Write simplified GeoJSON
                    geojson_filename = f"{country_code}_{level}.geojson"
                    geojson_path = os.path.join(output_folder, geojson_filename)
                    gdf_simplified.to_file(geojson_path, driver="GeoJSON")

                    print(f"Converted + simplified {file} → {geojson_path} (tolerance={simplify_tolerance})")

                except Exception as e:
                    print(f"Failed to convert {file}: {e}")
def find_shapefile_resources(resources):
    urls = []
    keywords = ["adm", "admin", "shp", "shapefile"] 

    for res in resources:
        fmt = res.get("format", "").lower()
        url = res.get("url", "").lower()
        name = res.get("name", "").lower()

        # 1. Direct format match
        if fmt in ("zipped shapefiles", "shapefile", "zip"):
            urls.append(res.get("url"))
            continue

        # 2. URL looks like a ZIP file
        if url.endswith(".zip"):
            urls.append(res.get("url"))
            continue

        # 3. keywords in name
        if any(k in name for k in keywords) or any(k in url for k in keywords):
            urls.append(res.get("url"))
            continue

    return urls

def download_shapefiles(country_code):
    dataset_id = f"cod-ab-{country_code.lower()}"
    downloads_dir = "downloads"
    os.makedirs(downloads_dir, exist_ok=True)

    resources = get_dataset_resources(dataset_id)
    zip_urls = find_shapefile_resources(resources)

    if not zip_urls:
        print("No shapefile ZIP found.")
        return

    for i, url in enumerate(zip_urls, start=1):
        filename = f"{country_code}_shapefile_{i}.zip"
        zip_path = os.path.join(downloads_dir, filename)
        print(f"Downloading: {filename}")
        try:
            download_file(url, zip_path)

            with tempfile.TemporaryDirectory() as tmpdir:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdir)
                convert_shapefiles_to_geojson(tmpdir, "data", country_code)

        except Exception as e:
            print(f"Failed to download {url}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide a country code, e.g., `python download_cod_ab.py STP`")
        sys.exit(1)

    country_code = sys.argv[1]
    print(f"Starting script for country: {country_code}")
    download_shapefiles(country_code)