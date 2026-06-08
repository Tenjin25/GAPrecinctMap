"""
Run DRA-based crosswalk + district aggregation safely.

This script intentionally avoids overwriting production district outputs.

Steps:
1) Build DRA-derived crosswalks (downloads from GitHub) into Data/crosswalks_dra.
2) Run district aggregation using those crosswalks into Data/district_contests_dra.
3) Compare output file inventory against production Data/district_contests/manifest.json.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)


def download_file(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as resp, out_path.open("wb") as fh:
        shutil.copyfileobj(resp, fh)


def extract_single_geojson(zip_path: Path, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name.lower().endswith(".geojson")]
        if not members:
            raise SystemExit(f"No .geojson file found inside {zip_path}")
        member = members[0]
        out_path = out_dir / Path(member).name
        with zf.open(member) as src, out_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        return out_path


def normalize_dra_geojson_id_to_geoid20(input_geojson: Path, output_geojson: Path, *, id_property: str) -> None:
    data = json.loads(input_geojson.read_text(encoding="utf-8"))
    features = data.get("features")
    if not isinstance(features, list):
        raise SystemExit(f"Invalid GeoJSON features in {input_geojson}")
    for feat in features:
        props = feat.get("properties") or {}
        raw = props.get(id_property, "")
        geoid20 = str(raw or "").strip().upper()
        if geoid20:
            props["GEOID20"] = geoid20
            feat["properties"] = props
    output_geojson.parent.mkdir(parents=True, exist_ok=True)
    output_geojson.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def read_manifest_files(manifest_path: Path) -> set[str]:
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    files = data.get("files") or []
    out: set[str] = set()
    for entry in files:
        f = str((entry or {}).get("file") or "").strip()
        if f:
            out.add(f)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python-exe", default=sys.executable)
    ap.add_argument("--crosswalk-dir", type=Path, default=Path("Data/crosswalks_dra"))
    ap.add_argument("--out-dir", type=Path, default=Path("Data/district_contests_dra"))
    ap.add_argument("--prod-manifest", type=Path, default=Path("Data/district_contests/manifest.json"))
    ap.add_argument("--state", default="GA")
    ap.add_argument("--year", default="2020")
    ap.add_argument("--version", default="07")
    ap.add_argument("--id-property", default="id")
    ap.add_argument("--cache-dir", type=Path, default=Path("Data/dra_cache"))
    ap.add_argument("--normalized-vtd-geojson", type=Path, default=Path("Data/dra/ga_dra_vtd20.geojson"))
    ap.add_argument("--cd-geojson", type=Path, default=Path("Data/tl_2022_13_cd118.geojson"))
    ap.add_argument("--sldl-geojson", type=Path, default=Path("Data/tl_2022_13_sldl.geojson"))
    ap.add_argument("--sldu-geojson", type=Path, default=Path("Data/tl_2022_13_sldu.geojson"))
    args = ap.parse_args()

    state = args.state.upper()
    geo_zip_name = f"Geojson_{state}.v{args.version}.zip"
    url = (
        "https://raw.githubusercontent.com/dra2020/vtd_data/master/"
        f"{args.year}_VTD/{state}/{geo_zip_name}"
    )
    zip_path = args.cache_dir / geo_zip_name
    extract_dir = args.cache_dir / f"{state}_{args.year}_v{args.version}"
    print(f"Downloading DRA zip: {url}")
    download_file(url, zip_path)
    extracted_geojson = extract_single_geojson(zip_path, extract_dir)
    normalize_dra_geojson_id_to_geoid20(
        extracted_geojson,
        args.normalized_vtd_geojson,
        id_property=args.id_property,
    )

    run(
        [
            args.python_exe,
            "scripts/build_precinct_district_crosswalks_from_blockassign.py",
            "--vtd20-geojson",
            str(args.normalized_vtd_geojson),
            "--cd118-geojson",
            str(args.cd_geojson),
            "--sldl-geojson",
            str(args.sldl_geojson),
            "--sldu-geojson",
            str(args.sldu_geojson),
            "--out-dir",
            str(args.crosswalk_dir),
            "--copy-2022-to-2024",
        ]
    )

    run(
        [
            args.python_exe,
            "scripts/build_district_contests_from_derived_vtd20.py",
            "--crosswalk-cd",
            str(args.crosswalk_dir / "precinct_to_cd118.csv"),
            "--crosswalk-state-house-2022",
            str(args.crosswalk_dir / "precinct_to_2022_state_house.csv"),
            "--crosswalk-state-house-2024",
            str(args.crosswalk_dir / "precinct_to_2024_state_house.csv"),
            "--crosswalk-state-senate-2022",
            str(args.crosswalk_dir / "precinct_to_2022_state_senate.csv"),
            "--crosswalk-state-senate-2024",
            str(args.crosswalk_dir / "precinct_to_2024_state_senate.csv"),
            "--out-dir",
            str(args.out_dir),
        ]
    )

    prod_files = read_manifest_files(args.prod_manifest)
    dra_manifest = args.out_dir / "manifest.json"
    dra_files = read_manifest_files(dra_manifest)

    only_prod = sorted(prod_files - dra_files)
    only_dra = sorted(dra_files - prod_files)

    print(f"Production manifest files: {len(prod_files)}")
    print(f"DRA manifest files: {len(dra_files)}")
    print(f"Only in production: {len(only_prod)}")
    print(f"Only in DRA output: {len(only_dra)}")

    if only_prod:
        print("Examples only in production:", only_prod[:10])
    if only_dra:
        print("Examples only in DRA output:", only_dra[:10])

    if only_prod or only_dra:
        raise SystemExit(
            "Parity check failed: DRA output file inventory differs from production. "
            "Review before replacing live district_contests."
        )

    print("Parity check passed.")


if __name__ == "__main__":
    main()
