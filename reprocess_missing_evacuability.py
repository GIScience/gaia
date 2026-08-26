#!/usr/bin/env python3
"""
Backfill evacuability CSVs for countries that were processed at ADM1 before
the ADM2->ADM1 fallback existed in calculate_evacuability_csv() (see
src/gaia/scripts/calculate_evacuatability.py). Those countries have
{country}_ADM1_*.csv outputs but no {country}_ADM1_evacuability.csv.

For each affected country this:
  1. Checks whether the Temporary/ flood and cyclone rasters evacuability
     needs still exist. exposure_flood_asset / exposure_cyclone_asset both
     skip raster regeneration once their own output CSV already looks
     complete, so if the rasters are gone (e.g. cleanup_asset already ran)
     their CSVs are removed first to force a full recompute.
  2. Materializes evacuability_asset and its upstream chain
     (boundary_asset, demographics_asset, facilities_asset,
     exposure_flood_asset, exposure_cyclone_asset) for that country's
     partition via `dagster asset materialize --select "*evacuability_asset"`.

Run from the repo root on the server (paths are relative, matching how the
assets themselves resolve `data/<COUNTRY>/...`).
"""

import argparse
import subprocess
import sys
from pathlib import Path

DATA_DIR = Path("data")
DEFS_MODULE = "gaia.definitions"
ENV_FILE = Path(".env")


def load_dotenv(env, path=ENV_FILE):
    """Minimal .env loader so subprocess runs (OHSOME_API_KEY, DAGSTER_HOME,
    ...) even if the caller's shell hasn't sourced it."""
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return env


def detect_affected_countries():
    """ADM1-only countries (no ADM2 output at all) missing the evacuability CSV."""
    affected = []
    for country_dir in sorted(DATA_DIR.iterdir()):
        if not country_dir.is_dir():
            continue
        country = country_dir.name
        output_dir = country_dir / "Output"
        if not output_dir.is_dir():
            continue

        adm1_csvs = [
            f
            for f in output_dir.glob(f"{country}_ADM1_*.csv")
            if "evacuability" not in f.name
        ]
        adm2_csvs = list(output_dir.glob(f"{country}_ADM2_*.csv"))
        if not adm1_csvs or adm2_csvs:
            continue  # not an ADM1-only country

        evac_csv = output_dir / f"{country}_ADM1_evacuability.csv"
        if evac_csv.exists():
            continue

        affected.append(country)
    return affected


def force_exposure_regen(country, dry_run):
    """Delete flood/cyclone exposure CSVs whose Temporary rasters are gone,
    so exposure_flood_asset / exposure_cyclone_asset rebuild them instead of
    short-circuiting on the already-complete CSV. Returns what was (or, in
    dry-run mode, would be) removed."""
    base_dir = DATA_DIR / country
    output_dir = base_dir / "Output"
    temp_dir = base_dir / "Temporary"
    removed = []

    has_flood_raster = any(temp_dir.glob(f"{country}_flooded_RP*.tif"))
    if not has_flood_raster:
        flood_csv = output_dir / f"{country}_ADM1_flood_exposure.csv"
        if flood_csv.exists():
            if not dry_run:
                flood_csv.unlink()
            removed.append(flood_csv.name)

    has_cyclone_raster = (temp_dir / f"{country}_cyclone_exposure.tif").exists()
    if not has_cyclone_raster:
        cyclone_csv = output_dir / f"{country}_ADM1_cyclone_exposure.csv"
        if cyclone_csv.exists():
            if not dry_run:
                cyclone_csv.unlink()
            removed.append(cyclone_csv.name)

    return removed


def materialize(country, env, dry_run):
    cmd = [
        "dagster",
        "asset",
        "materialize",
        "-m",
        DEFS_MODULE,
        "--select",
        "*evacuability_asset",
        "--partition",
        country,
    ]
    print(f"[{country}] {' '.join(cmd)}")
    if dry_run:
        return True
    result = subprocess.run(cmd, env=env)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show detected countries and the commands that would run.",
    )
    parser.add_argument(
        "--country",
        action="append",
        help="Restrict to this country code (repeatable). Default: all detected.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt before running for real.",
    )
    args = parser.parse_args()

    affected = detect_affected_countries()
    if args.country:
        wanted = {c.upper() for c in args.country}
        unknown = wanted - set(affected)
        if unknown:
            print(f"Warning: not detected as affected (running anyway): {sorted(unknown)}")
            affected = sorted(set(affected) | wanted)
        affected = [c for c in affected if c in wanted]

    if not affected:
        print("No affected countries found (nothing missing an evacuability CSV).")
        return

    print(f"Found {len(affected)} ADM1-only countries missing evacuability output:")
    for c in affected:
        print(f"  - {c}")

    if not args.dry_run and not args.yes:
        reply = input(f"\nProceed with reprocessing {len(affected)} countries? [y/N] ")
        if reply.strip().lower() != "y":
            print("Aborted.")
            return

    env = load_dotenv(dict(__import__("os").environ))

    failures = []
    for country in affected:
        print(f"\n=== {country} ===")
        removed = force_exposure_regen(country, args.dry_run)
        if removed:
            verb = "Would remove" if args.dry_run else "Removed"
            print(f"[{country}] {verb} to force raster regeneration: {removed}")
        else:
            print(f"[{country}] Existing rasters found, no forced regeneration needed.")

        ok = materialize(country, env, args.dry_run)
        if not ok:
            print(f"[{country}] FAILED")
            failures.append(country)

    print("\nDone.")
    if failures:
        print(f"Failed countries ({len(failures)}): {failures}")
        sys.exit(1)


if __name__ == "__main__":
    main()
