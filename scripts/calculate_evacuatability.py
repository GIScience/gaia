"""
calculate_evacuatability.py

Calculates an "Evacuatability" indicator: the travel time (in minutes) for 
at-risk (flooded) population to reach the nearest safe zone.

Uses MCP_Geometric from scikit-image for least-cost path analysis on a 
friction surface, replicating GEE's cumulativeCost logic in pure Python.

Inputs:
    - Local flood raster: data/{COUNTRY}/Temporary/{COUNTRY}_flooded_RP{RP}.tif
    - Local population rasters: data/{COUNTRY}/Temporary/{COUNTRY}_pop_{indicator}_2020_constrained.tif
    - Local admin boundaries: data/{COUNTRY}/{COUNTRY}_{ADMIN_LEVEL}.geojson
    - Remote friction COG: https://hot.storage.heigit.org/.../friction_surface_cog.tif

Outputs:
    - CSV with ADM*_PCODE, evac_time stats, and pop_at_risk for each demographic
"""

import os
import argparse
import glob
import re
import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.windows import from_bounds, Window
from rasterio.warp import reproject, Resampling
from rasterstats import zonal_stats
from skimage.graph import MCP_Geometric
from pathlib import Path
import tempfile
import yaml

# Load config for flood threshold
ASSET_CONFIG_YAML_PATH = os.path.join(os.getcwd(), "configs", "assets_config.yaml")
try:
    with open(ASSET_CONFIG_YAML_PATH) as _fp:
        _asset_config = yaml.safe_load(_fp)
    FLOOD_THRESHOLD = float(_asset_config["setup"]["flood_threshold"])
except (FileNotFoundError, KeyError):
    FLOOD_THRESHOLD = 0.3  # default: 30cm

# Remote friction surface COG URL
FRICTION_COG_URL = "https://hot.storage.heigit.org/heigit-hdx-public/risk_assessment_inputs/2020_motorized_friction_surface_cog.tif"

# Performance tuning
MAX_PIXELS_FOR_MCP = 10_000_000  # 10M pixels max before downsampling
TARGET_RESOLUTION_M = 500  # Target resolution when downsampling (meters)
MAX_MCP_SOURCES = 20000  # Max safe zone pixels to sample for MCP

# Demographic indicators to process (matching WorldPop naming)
DEMOGRAPHIC_INDICATORS = [
    "total_pop",
    "children_u5",
    "elderly", 
    "female_pop",
    "female_u5",
    "female_u15",
    "pop_u15"
]


def find_population_rasters(temp_dir, country_code):
    """
    Find all population rasters in the temporary directory.
    
    Returns:
        dict: {indicator_name: path} for each found demographic raster
    """
    pop_rasters = {}
    pattern = temp_dir / f"{country_code}_pop_*_2020_constrained.tif"
    
    for path in glob.glob(str(pattern)):
        # Extract indicator name from filename
        # e.g., STP_pop_children_u5_2020_constrained.tif -> children_u5
        filename = os.path.basename(path)
        match = re.search(rf"{country_code}_pop_(.+)_2020_constrained\.tif", filename)
        if match:
            indicator = match.group(1)
            pop_rasters[indicator] = Path(path)
    
    return pop_rasters


def downsample_array(arr, scale_factor, method='mean'):
    """
    Downsample a 2D array by a scale factor.
    
    Args:
        arr: Input 2D numpy array
        scale_factor: Factor to reduce dimensions by (e.g., 5 = 5x smaller)
        method: 
            - 'mean': Average values (for continuous data like friction, flood depth)
            - 'max': Any True in block = True (for binary masks)
            - 'sum': Sum values (for population - preserves total count)
        
    Returns:
        Downsampled array
    
    IMPORTANT: For population data, ALWAYS use method='sum' to preserve total counts.
    """
    from scipy.ndimage import zoom
    
    if scale_factor <= 1:
        return arr
    
    scale_factor = int(round(scale_factor))
    
    if method == 'sum':
        # For population: proper block-sum to preserve total
        # Trim array to be divisible by scale_factor
        h, w = arr.shape
        new_h = (h // scale_factor) * scale_factor
        new_w = (w // scale_factor) * scale_factor
        trimmed = arr[:new_h, :new_w]
        
        # Reshape into blocks and sum
        reshaped = trimmed.reshape(
            new_h // scale_factor, scale_factor,
            new_w // scale_factor, scale_factor
        )
        # Sum over the block dimensions, handling NaN
        result = np.nansum(reshaped, axis=(1, 3))
        return result.astype(arr.dtype)
    
    elif method == 'mean':
        # For continuous data: bilinear interpolation
        return zoom(arr, 1/scale_factor, order=1)
    
    elif method == 'max':
        # For masks: any True in block = True
        return zoom(arr.astype(float), 1/scale_factor, order=0) > 0.5
    
    else:
        # Nearest neighbor
        return zoom(arr, 1/scale_factor, order=0)


def upsample_array(arr, target_shape, method='bilinear'):
    """
    Upsample array to target shape.
    
    Args:
        arr: Input array
        target_shape: (height, width) target dimensions
        method: 'bilinear' for continuous, 'nearest' for masks/categorical
        
    IMPORTANT: Never upsample population data - it creates fake precision.
    Only upsample masks or travel time results.
    """
    from scipy.ndimage import zoom
    
    scale_y = target_shape[0] / arr.shape[0]
    scale_x = target_shape[1] / arr.shape[1]
    
    order = 1 if method == 'bilinear' else 0
    return zoom(arr, (scale_y, scale_x), order=order)


def load_local_raster(path, band=1):
    """Load a local raster and return array, profile, and nodata value."""
    with rasterio.open(path) as src:
        arr = src.read(band).astype(np.float32)
        profile = src.profile.copy()
        nodata = src.nodata
        bounds = src.bounds
        transform = src.transform
        crs = src.crs
    return arr, profile, nodata, bounds, transform, crs


def fetch_friction_window(bounds, target_crs, target_transform, target_shape,
                          friction_url=FRICTION_COG_URL):
    """
    Fetch only the required window from the remote friction COG and 
    reproject/resample to match the target grid.
    
    Args:
        bounds: (left, bottom, right, top) in target CRS
        target_crs: CRS of target raster
        target_transform: Affine transform of target raster
        target_shape: (height, width) of target raster
        friction_url: URL of the friction COG
        
    Returns:
        friction_arr: numpy array aligned with target grid
    """
    print("Fetching friction surface from COG (window only)...")
    
    with rasterio.open(friction_url) as src:
        friction_crs = src.crs
        friction_nodata = src.nodata
        
        # Transform bounds from target CRS to friction CRS if different
        if target_crs != friction_crs:
            from rasterio.warp import transform_bounds
            src_bounds = transform_bounds(target_crs, friction_crs, *bounds)
        else:
            src_bounds = bounds
        
        # Calculate window in friction raster coordinates
        window = from_bounds(*src_bounds, src.transform)
        
        # Expand window slightly to ensure coverage after reprojection
        col_off = max(0, int(window.col_off) - 5)
        row_off = max(0, int(window.row_off) - 5)
        width = min(src.width - col_off, int(window.width) + 10)
        height = min(src.height - row_off, int(window.height) + 10)
        
        window = Window(col_off, row_off, width, height)
        
        print(f"  Reading window: {window}")
        friction_raw = src.read(1, window=window).astype(np.float32)
        
        # Get the transform for the windowed data
        window_transform = src.window_transform(window)
    
    # Reproject friction to match target grid
    friction_aligned = np.zeros(target_shape, dtype=np.float32)
    
    reproject(
        source=friction_raw,
        destination=friction_aligned,
        src_transform=window_transform,
        src_crs=friction_crs,
        src_nodata=friction_nodata,
        dst_transform=target_transform,
        dst_crs=target_crs,
        dst_nodata=np.nan,
        resampling=Resampling.bilinear
    )
    
    print(f"  Friction array shape: {friction_aligned.shape}")
    return friction_aligned


def create_cost_surface(friction_arr, flood_arr, flood_threshold=FLOOD_THRESHOLD):
    """
    Create a cost surface from the friction array.
    
    The friction values represent minutes to traverse one meter.
    We need to account for pixel size when computing actual travel time.
    
    Args:
        friction_arr: Friction surface (minutes per meter)
        flood_arr: Flood depth array
        flood_threshold: Threshold for defining flooded areas (meters)
        
    Returns:
        cost_arr: Cost surface for MCP
        safe_mask: Boolean mask of safe zones (sources)
        at_risk_mask: Boolean mask of at-risk zones
    """
    # Create masks
    # Safe zones: NOT flooded (depth <= threshold or no flood data)
    # At-risk zones: flooded (depth > threshold)
    
    valid_flood = ~np.isnan(flood_arr)
    at_risk_mask = valid_flood & (flood_arr > flood_threshold)
    safe_mask = valid_flood & (flood_arr <= flood_threshold)
    
    # Also consider areas with no flood data as potentially safe
    # (areas outside flood model extent)
    no_flood_data = np.isnan(flood_arr) | (flood_arr == 0)
    safe_mask = safe_mask | no_flood_data
    
    # Cost surface: use friction values
    # Set invalid/nodata areas to very high cost (but not infinite, for MCP)
    cost_arr = friction_arr.copy()
    cost_arr[np.isnan(cost_arr)] = 1e6  # Very high cost for nodata
    cost_arr[cost_arr <= 0] = 1e6  # Invalid friction values
    
    return cost_arr, safe_mask, at_risk_mask


def calculate_travel_time_mcp(cost_arr, safe_mask, pixel_size_m):
    """
    Calculate travel time from every pixel to the nearest safe zone using MCP.
    
    MCP_Geometric accounts for diagonal movement (√2 factor).
    
    Args:
        cost_arr: Cost surface (friction: minutes per meter)
        safe_mask: Boolean mask where True = safe zone (source)
        pixel_size_m: Pixel size in meters
        
    Returns:
        travel_time: Array with travel time (minutes) to nearest safe zone
    """
    print("Calculating least-cost paths to safe zones...")
    
    # Get safe zone indices (these are our sources/targets)
    safe_indices = np.argwhere(safe_mask)
    
    if len(safe_indices) == 0:
        print("  Warning: No safe zones found!")
        return np.full(cost_arr.shape, np.nan)
    
    print(f"  Found {len(safe_indices)} safe zone pixels")
    
    # Scale cost by pixel size: friction is min/m, we want total cost per pixel
    # MCP_Geometric handles the geometry (diagonal = √2 * pixel)
    # Cost per pixel = friction (min/m) * pixel_size (m) = minutes per pixel
    cost_scaled = cost_arr * pixel_size_m
    
    # Create MCP object
    # MCP_Geometric accounts for diagonal distances automatically
    mcp = MCP_Geometric(cost_scaled, fully_connected=True)
    
    # Find cumulative cost from all safe zones
    # We use find_costs which returns cost to reach any of the start points
    # Convert safe_indices to list of tuples for MCP
    starts = [tuple(idx) for idx in safe_indices]
    
    # For large number of starts, MCP can be slow
    # Sample if too many safe pixels
    if len(starts) > MAX_MCP_SOURCES:
        print(f"  Sampling {MAX_MCP_SOURCES} safe zone pixels (from {len(starts)})")
        rng = np.random.default_rng(42)
        indices = rng.choice(len(starts), MAX_MCP_SOURCES, replace=False)
        starts = [starts[i] for i in indices]
    
    print(f"  Running MCP with {len(starts)} source pixels...")
    print(f"  Grid size: {cost_arr.shape[0]:,} x {cost_arr.shape[1]:,} = {cost_arr.size:,} pixels")
    cumulative_cost, traceback = mcp.find_costs(starts)
    
    # Convert to travel time in minutes
    travel_time = cumulative_cost.astype(np.float32)
    
    # Set unreachable areas to NaN
    travel_time[travel_time >= 1e9] = np.nan
    
    print(f"  Travel time range: {np.nanmin(travel_time):.1f} - {np.nanmax(travel_time):.1f} minutes")
    
    return travel_time


def get_pixel_size_meters(transform, crs):
    """
    Get approximate pixel size in meters.
    
    For geographic CRS, convert degrees to meters at typical latitude.
    """
    pixel_x = abs(transform.a)
    pixel_y = abs(transform.e)
    
    if crs.is_geographic:
        # Approximate conversion: 1 degree ≈ 111,000 meters at equator
        # This is a simplification; for more accuracy, use the centroid latitude
        meters_per_degree = 111000
        pixel_size_m = ((pixel_x + pixel_y) / 2) * meters_per_degree
    else:
        pixel_size_m = (pixel_x + pixel_y) / 2
    
    return pixel_size_m


def aggregate_by_admin(travel_time_arr, at_risk_mask, pop_rasters, 
                       gdf, admin_level, transform, crs, output_path):
    """
    Aggregate travel time statistics by admin boundaries for all demographics.
    
    Args:
        travel_time_arr: Travel time to safety (minutes)
        at_risk_mask: Boolean mask of at-risk pixels
        pop_rasters: dict {indicator_name: (array, nodata)} for each demographic
        gdf: GeoDataFrame with admin boundaries
        admin_level: e.g., 'ADM2'
        transform: Affine transform
        crs: CRS of rasters
        output_path: Path for output CSV
        
    Returns:
        DataFrame with aggregated statistics
    """
    print(f"Aggregating by {admin_level} boundaries...")
    
    id_col = f"{admin_level}_PCODE"
    if id_col not in gdf.columns:
        raise ValueError(f"Column {id_col} not found in boundaries")
    
    # Ensure GDF is in same CRS as rasters
    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)
    
    # Create masked travel time array for at-risk areas only
    travel_time_at_risk = travel_time_arr.copy()
    travel_time_at_risk[~at_risk_mask] = np.nan
    
    # Save temporary rasters and compute stats
    with tempfile.TemporaryDirectory() as tmpdir:
        # Travel time raster
        tt_path = os.path.join(tmpdir, "travel_time.tif")
        with rasterio.open(
            tt_path, 'w',
            driver='GTiff',
            height=travel_time_at_risk.shape[0],
            width=travel_time_at_risk.shape[1],
            count=1,
            dtype=np.float32,
            crs=crs,
            transform=transform,
            nodata=np.nan
        ) as dst:
            dst.write(travel_time_at_risk, 1)
        
        # Calculate travel time zonal statistics
        print("  Calculating zonal statistics for travel time...")
        tt_stats = zonal_stats(
            gdf, tt_path,
            stats=['mean', 'max', 'median', 'count'],
            nodata=np.nan
        )
        
        # Calculate population stats for each demographic
        # IMPORTANT: Population is kept at original resolution and SUMMED (not averaged)
        # Only the mask is resized if needed - population values are never interpolated
        pop_stats_all = {}
        for indicator, (pop_arr, pop_nodata) in pop_rasters.items():
            print(f"  Calculating zonal statistics for {indicator}...")
            
            # Handle shape mismatch between at_risk_mask and pop_arr
            # This can happen when flood and population rasters have different extents
            if at_risk_mask.shape != pop_arr.shape:
                # Resize mask to match population array using nearest neighbor
                # (preserves binary nature of mask, doesn't interpolate)
                mask_resized = upsample_array(
                    at_risk_mask.astype(np.float32), 
                    pop_arr.shape,
                    method='nearest'
                ) > 0.5
            else:
                mask_resized = at_risk_mask
            
            # Mask population to at-risk areas (population stays unchanged, just masked)
            pop_at_risk = pop_arr.copy()
            pop_at_risk[~mask_resized] = 0
            
            # Write temp raster
            pop_path = os.path.join(tmpdir, f"pop_{indicator}.tif")
            with rasterio.open(
                pop_path, 'w',
                driver='GTiff',
                height=pop_at_risk.shape[0],
                width=pop_at_risk.shape[1],
                count=1,
                dtype=np.float32,
                crs=crs,
                transform=transform,
                nodata=0
            ) as dst:
                dst.write(pop_at_risk, 1)
            
            pop_stats_all[indicator] = zonal_stats(
                gdf, pop_path,
                stats=['sum'],
                nodata=0
            )
    
    # Build results DataFrame
    results = []
    for i, (idx, row) in enumerate(gdf.iterrows()):
        pcode = row[id_col]
        tt = tt_stats[i]
        
        record = {
            id_col: pcode,
            'evac_time_mean_min': round(tt['mean'], 1) if tt['mean'] else None,
            'evac_time_max_min': round(tt['max'], 1) if tt['max'] else None,
            'evac_time_median_min': round(tt['median'], 1) if tt['median'] else None,
            'pixels_at_risk': tt['count'] if tt['count'] else 0,
        }
        
        # Add population at risk for each demographic
        for indicator, pop_stats in pop_stats_all.items():
            pop = pop_stats[i]
            record[f'pop_at_risk_{indicator}'] = int(pop['sum']) if pop['sum'] else 0
        
        results.append(record)
    
    df = pd.DataFrame(results)
    
    # Save CSV
    df.to_csv(output_path, index=False)
    print(f"  Saved results to {output_path}")
    
    return df


def process_evacuatability(country_code, admin_level='ADM2', rp='100', 
                           flood_threshold=None, context=None):
    """
    Main function to calculate evacuatability for a country.
    
    Args:
        country_code: ISO3 country code (e.g., 'STP')
        admin_level: Administrative level (e.g., 'ADM2')
        rp: Return period for flood raster (e.g., '100')
        flood_threshold: Override flood threshold (meters)
        context: Optional logging context (Dagster)
        
    Returns:
        Tuple of (csv_path, tif_path) for aggregated stats and travel time raster
    """
    country_code = country_code.upper()
    admin_level = admin_level.upper()
    threshold = flood_threshold if flood_threshold else FLOOD_THRESHOLD
    
    log = context.info if context else print
    
    log(f"Processing evacuatability for {country_code}, {admin_level}, RP{rp}")
    log(f"  Flood threshold: {threshold}m")
    
    # Paths
    base_dir = Path(f"data/{country_code}")
    temp_dir = base_dir / "Temporary"
    output_dir = base_dir / "Output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    flood_path = temp_dir / f"{country_code}_flooded_RP{rp}.tif"
    boundary_path = base_dir / f"{country_code}_{admin_level}.geojson"
    output_csv = output_dir / f"{country_code}_evacuatability_by_{admin_level}_RP{rp}.csv"
    output_tif = output_dir / f"{country_code}_travel_time_RP{rp}.tif"
    
    # Check inputs exist
    if not flood_path.exists():
        raise FileNotFoundError(f"Flood raster not found: {flood_path}")
    if not boundary_path.exists():
        raise FileNotFoundError(f"Boundary file not found: {boundary_path}")
    
    # Find all population rasters
    pop_raster_paths = find_population_rasters(temp_dir, country_code)
    if not pop_raster_paths:
        raise FileNotFoundError(
            f"No population rasters found in {temp_dir}. "
            f"Expected pattern: {country_code}_pop_*_2020_constrained.tif"
        )
    
    log(f"  Found {len(pop_raster_paths)} demographic indicators: {list(pop_raster_paths.keys())}")
    
    # Load flood raster
    log("Loading flood raster...")
    flood_arr, flood_profile, flood_nodata, bounds, transform, crs = load_local_raster(flood_path)
    
    # Handle nodata
    if flood_nodata is not None:
        flood_arr[flood_arr == flood_nodata] = np.nan
    
    # Load all population rasters
    log("Loading population rasters...")
    pop_rasters = {}
    for indicator, path in pop_raster_paths.items():
        pop_arr, _, pop_nodata, _, _, _ = load_local_raster(path)
        if pop_nodata is not None:
            pop_arr[pop_arr == pop_nodata] = 0
        pop_rasters[indicator] = (pop_arr, pop_nodata)
        log(f"    Loaded {indicator}: shape {pop_arr.shape}")
    
    # Fetch friction surface (windowed from COG)
    friction_arr = fetch_friction_window(
        bounds=(bounds.left, bounds.bottom, bounds.right, bounds.top),
        target_crs=crs,
        target_transform=transform,
        target_shape=flood_arr.shape
    )
    
    # Get pixel size in meters (needed for downsampling decision)
    pixel_size_m = get_pixel_size_meters(transform, crs)
    log(f"  Original pixel size: {pixel_size_m:.1f} meters")
    
    # Check if we need to downsample for performance
    total_pixels = flood_arr.size
    original_shape = flood_arr.shape
    scale_factor = 1
    
    if total_pixels > MAX_PIXELS_FOR_MCP:
        # Calculate scale factor to reach target resolution or max pixels
        scale_by_pixels = np.sqrt(total_pixels / MAX_PIXELS_FOR_MCP)
        scale_by_resolution = TARGET_RESOLUTION_M / pixel_size_m if pixel_size_m < TARGET_RESOLUTION_M else 1
        scale_factor = max(scale_by_pixels, scale_by_resolution)
        
        log(f"  Large raster detected ({total_pixels:,} pixels)")
        log(f"  Downsampling by factor {scale_factor:.1f} for MCP calculation...")
        
        # Downsample arrays for MCP
        flood_arr_ds = downsample_array(flood_arr, scale_factor, method='mean')
        friction_arr_ds = downsample_array(friction_arr, scale_factor, method='mean')
        
        # Adjust pixel size for downsampled grid
        pixel_size_m_ds = pixel_size_m * scale_factor
        
        log(f"  Downsampled shape: {flood_arr_ds.shape} ({flood_arr_ds.size:,} pixels)")
        log(f"  Effective pixel size: {pixel_size_m_ds:.1f} meters")
    else:
        flood_arr_ds = flood_arr
        friction_arr_ds = friction_arr
        pixel_size_m_ds = pixel_size_m
    
    # Create cost surface and masks (on downsampled if applicable)
    cost_arr, safe_mask, at_risk_mask_ds = create_cost_surface(
        friction_arr_ds, flood_arr_ds, threshold
    )
    
    log(f"  Safe zone pixels: {safe_mask.sum():,}")
    log(f"  At-risk pixels: {at_risk_mask_ds.sum():,}")
    
    if at_risk_mask_ds.sum() == 0:
        log("  No at-risk areas found. Creating empty output.")
        gdf = gpd.read_file(boundary_path)
        id_col = f"{admin_level}_PCODE"
        
        empty_data = {id_col: gdf[id_col]}
        empty_data['evac_time_mean_min'] = None
        empty_data['evac_time_max_min'] = None
        empty_data['evac_time_median_min'] = None
        empty_data['pixels_at_risk'] = 0
        for indicator in pop_rasters.keys():
            empty_data[f'pop_at_risk_{indicator}'] = 0
        
        df = pd.DataFrame(empty_data)
        df.to_csv(output_csv, index=False)
        
        # Create empty travel time raster (all NaN)
        empty_travel_time = np.full(original_shape, np.nan, dtype='float32')
        with rasterio.open(
            output_tif,
            'w',
            driver='GTiff',
            height=original_shape[0],
            width=original_shape[1],
            count=1,
            dtype='float32',
            crs=crs,
            transform=transform,
            nodata=np.nan,
            compress='lzw'
        ) as dst:
            dst.write(empty_travel_time, 1)
        
        return str(output_csv), str(output_tif)
    
    # Calculate travel times using MCP (on downsampled grid)
    travel_time_ds = calculate_travel_time_mcp(cost_arr, safe_mask, pixel_size_m_ds)
    
    # Upsample travel time back to original resolution if we downsampled
    if scale_factor > 1:
        log(f"  Upsampling travel time to original resolution {original_shape}...")
        travel_time = upsample_array(travel_time_ds, original_shape)
        # Recreate at-risk mask at original resolution
        at_risk_mask = create_cost_surface(friction_arr, flood_arr, threshold)[2]
    else:
        travel_time = travel_time_ds
        at_risk_mask = at_risk_mask_ds
    
    # Save travel time raster for validation
    log(f"  Saving travel time raster to {output_tif}...")
    travel_time_output = np.where(at_risk_mask, travel_time, np.nan)
    with rasterio.open(
        output_tif,
        'w',
        driver='GTiff',
        height=travel_time_output.shape[0],
        width=travel_time_output.shape[1],
        count=1,
        dtype='float32',
        crs=crs,
        transform=transform,
        nodata=np.nan,
        compress='lzw'
    ) as dst:
        dst.write(travel_time_output.astype('float32'), 1)
        dst.set_band_description(1, f"Travel time to safe zone (minutes), RP{rp}")
    log(f"  Saved: {output_tif}")
    
    # Load admin boundaries
    log("Loading admin boundaries...")
    gdf = gpd.read_file(boundary_path)
    
    # Aggregate by admin
    df = aggregate_by_admin(
        travel_time_arr=travel_time,
        at_risk_mask=at_risk_mask,
        pop_rasters=pop_rasters,
        gdf=gdf,
        admin_level=admin_level,
        transform=transform,
        crs=crs,
        output_path=output_csv
    )
    
    log(f"Evacuatability calculation complete for {country_code}")
    log(f"  CSV: {output_csv}")
    log(f"  TIF: {output_tif}")

    return str(output_csv), str(output_tif)

def main():
    parser = argparse.ArgumentParser(
        description="Calculate evacuatability indicator: travel time from flooded to safe zones"
    )
    parser.add_argument(
        "country_code",
        help="ISO3 country code (e.g., STP, PAK, BGD)"
    )
    parser.add_argument(
        "--admin-level",
        default="ADM2",
        help="Administrative level (default: ADM2)"
    )
    parser.add_argument(
        "--rp",
        default="100",
        help="Return period (default: 100)"
    )
    parser.add_argument(
        "--flood-threshold",
        type=float,
        default=None,
        help=f"Flood depth threshold in meters (default: {FLOOD_THRESHOLD})"
    )
    
    args = parser.parse_args()
    
    output_path = process_evacuatability(
        country_code=args.country_code,
        admin_level=args.admin_level,
        rp=args.rp,
        flood_threshold=args.flood_threshold
    )
    
    print(f"\nOutput saved to: {output_path}")


if __name__ == "__main__":
    main()
