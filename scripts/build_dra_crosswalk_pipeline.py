"""
Build precinct->district crosswalks from DRA's GitHub VTD data.

Pipeline:
1) Download DRA GeoJSON ZIP for a state/year/version from:
   https://raw.githubusercontent.com/dra2020/vtd_data/master/{year}_VTD/{state}/Geojson_{state}.v{version}.zip
2) Extract the GeoJSON file.
3) Normalize DRA precinct id -> GEOID20-compatible key field (GEOID20) so existing
   crosswalk tooling can consume it.
4) Run scripts/build_precinct_district_crosswalks_from_blockassign.py in geometry mode
   using the normalized DRA VTD geometry.

Notes:
- This script is configured for Georgia defaults.
- Outputs are written to Data/crosswalks_dra by default to avoid overwriting
  existing production crosswalk files.
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


def normalize_dra_geojson_id_to_geoid20(
    input_geojson: Path,
    output_geojson: Path,
    *,
    id_property: str,
) -> None:
    data = json.loads(input_geojson.read_text(encoding="utf-8"))
    features = data.get("features")
    if not isinstance(features, list):
        raise SystemExit(f"Invalid GeoJSON features in {input_geojson}")

    missing = 0
    for feat in features:
        props = feat.get("properties") or {}
        raw = props.get(id_property, "")
        geoid20 = str(raw or "").strip().upper()
        if not geoid20:
            missing += 1
            continue
        props["GEOID20"] = geoid20
        feat["properties"] = props

    if missing:
        print(f"Warning: {missing} features missing '{id_property}'")

    output_geojson.parent.mkdir(parents=True, exist_ok=True)
    output_geojson.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def run_crosswalk_builder(
    *,
    python_exe: str,
    normalized_vtd_geojson: Path,
    cd_geojson: Path,
    sldl_geojson: Path,
    sldu_geojson: Path,
    out_dir: Path,
) -> None:
    cmd = [
        python_exe,
        "scripts/build_precinct_district_crosswalks_from_blockassign.py",
        "--vtd20-geojson",
        str(normalized_vtd_geojson),
        "--cd118-geojson",
        str(cd_geojson),
        "--sldl-geojson",
        str(sldl_geojson),
        "--sldu-geojson",
        str(sldu_geojson),
        "--out-dir",
        str(out_dir),
        "--copy-2022-to-2024",
    ]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", default="GA", help="State abbreviation used by DRA repo path (default: GA)")
    ap.add_argument("--year", default="2020", help="DRA VTD directory year (default: 2020)")
    ap.add_argument("--version", default="07", help="DRA GeoJSON zip version suffix (default: 07)")
    ap.add_argument(
        "--id-property",
        default="id",
        help="DRA GeoJSON feature properties key to use as precinct key (default: id)",
    )
    ap.add_argument("--work-dir", type=Path, default=Path("Data/dra_cache"))
    ap.add_argument("--normalized-vtd-geojson", type=Path, default=Path("Data/dra/ga_dra_vtd20.geojson"))
    ap.add_argument("--cd-geojson", type=Path, default=Path("Data/tl_2022_13_cd118.geojson"))
    ap.add_argument("--sldl-geojson", type=Path, default=Path("Data/tl_2022_13_sldl.geojson"))
    ap.add_argument("--sldu-geojson", type=Path, default=Path("Data/tl_2022_13_sldu.geojson"))
    ap.add_argument("--out-dir", type=Path, default=Path("Data/crosswalks_dra"))
    ap.add_argument("--python-exe", default=sys.executable, help="Python executable to run downstream builder")
    args = ap.parse_args()

    state = args.state.upper()
    geo_zip_name = f"Geojson_{state}.v{args.version}.zip"
    url = (
        "https://raw.githubusercontent.com/dra2020/vtd_data/master/"
        f"{args.year}_VTD/{state}/{geo_zip_name}"
    )

    zip_path = args.work_dir / geo_zip_name
    extract_dir = args.work_dir / f"{state}_{args.year}_v{args.version}"

    print(f"Downloading DRA zip: {url}")
    download_file(url, zip_path)
    print(f"Saved: {zip_path}")

    extracted_geojson = extract_single_geojson(zip_path, extract_dir)
    print(f"Extracted GeoJSON: {extracted_geojson}")

    normalize_dra_geojson_id_to_geoid20(
        extracted_geojson,
        args.normalized_vtd_geojson,
        id_property=args.id_property,
    )
    print(f"Wrote normalized VTD GeoJSON: {args.normalized_vtd_geojson}")

    run_crosswalk_builder(
        python_exe=args.python_exe,
        normalized_vtd_geojson=args.normalized_vtd_geojson,
        cd_geojson=args.cd_geojson,
        sldl_geojson=args.sldl_geojson,
        sldu_geojson=args.sldu_geojson,
        out_dir=args.out_dir,
    )

    print("Done.")


if __name__ == "__main__":
    main()

