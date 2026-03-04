"""
Build a VTD10 -> VTD20 crosswalk for Georgia using census blocks + NHGIS 2010->2020 block weights.

Why:
  If you want to compare elections across years on a single, consistent VTD geography,
  you can convert VTD10-aligned data onto VTD20 using this crosswalk.

How it works:
  1) Spatially assign each 2010 block to a 2010 VTD (VTD10) via point-in-polygon.
  2) Assign each 2020 block to a 2020 VTD (VTD20):
     - Preferred: Census block-assignment file (Data/BlockAssign_ST13_GA.zip).
     - Fallback: point-in-polygon spatial join.
  3) Use the NHGIS block crosswalk weights to allocate each 2010 block to 2020 blocks.
  4) Aggregate weights from (VTD10 -> VTD20) by summing block weights.

Inputs (defaults match this repo):
  - Data/tl_2012_13_vtd10.zip                (VTD10 polygons, Georgia)
  - Data/tl_2012_13_tabblock.zip             (2010-era tabulation blocks, Georgia)
  - Data/BlockAssign_ST13_GA.zip             (optional; Census block assignment zip, preferred)
  - Data/tl_2020_13_vtd20.zip                (fallback path: VTD20 polygons, Georgia)
  - Data/tl_2020_13_tabblock20.zip           (fallback path: 2020 tabulation blocks, Georgia)
  - Data/nhgis_blk2010_blk2020_13/nhgis_blk2010_blk2020_13.csv  (NHGIS weights)

Outputs:
  - Data/vtd10_to_vtd20_crosswalk.csv        (full matrix: vtd10_geoid, vtd20_geoid, weight_sum)
  - Data/vtd10_to_vtd20_best.json            (best-match per VTD10)

This is a heavy job (large shapefiles). Expect minutes on first run.

Usage:
  py scripts/build_vtd10_to_vtd20_crosswalk.py
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import zipfile

import pandas as pd


def _require_cols(df: pd.DataFrame, cols: list[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{label} missing columns: {missing}")


def _pick_col(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise SystemExit(f"{label} missing required column. Tried: {candidates}")


def _block_geoid_to_gisjoin(series: pd.Series) -> pd.Series:
    # NHGIS block GISJOIN format:
    # G + state(2) + "0" + county(3) + "0" + tract(6) + block(4)
    s = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True).str.zfill(15)
    return "G" + s.str.slice(0, 2) + "0" + s.str.slice(2, 5) + "0" + s.str.slice(5, 11) + s.str.slice(11, 15)


def _load_blockassign_vtd(zip_path: Path, member_name: str) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path, "r") as zf:
        if member_name not in zf.namelist():
            raise SystemExit(f"{zip_path} missing member '{member_name}'")
        with zf.open(member_name, "r") as fh:
            df = pd.read_csv(
                fh,
                sep="|",
                dtype=str,
                usecols=["BLOCKID", "COUNTYFP", "DISTRICT"],
            )

    _require_cols(df, ["BLOCKID", "COUNTYFP", "DISTRICT"], "Block assignment VTD")

    df["BLOCKID"] = df["BLOCKID"].astype(str).str.strip().str.zfill(15)
    df["COUNTYFP"] = df["COUNTYFP"].astype(str).str.strip().str.zfill(3)
    df["DISTRICT"] = df["DISTRICT"].astype(str).str.strip().str.upper()
    df = df[(df["BLOCKID"] != "") & (df["COUNTYFP"] != "") & (df["DISTRICT"] != "")]

    # For Georgia VTD20, GEOID20 is state(13) + countyfp(3) + VTDST20(6).
    out = pd.DataFrame(
        {
            "blk2020gj": _block_geoid_to_gisjoin(df["BLOCKID"]),
            "vtd20_geoid": "13" + df["COUNTYFP"] + df["DISTRICT"],
        }
    ).drop_duplicates(subset=["blk2020gj"], keep="first")

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vtd10-zip", type=Path, default=Path("Data/tl_2012_13_vtd10.zip"))
    ap.add_argument("--vtd20-zip", type=Path, default=Path("Data/tl_2020_13_vtd20.zip"))
    ap.add_argument("--blk10-zip", type=Path, default=Path("Data/tl_2012_13_tabblock.zip"))
    ap.add_argument("--blk20-zip", type=Path, default=Path("Data/tl_2020_13_tabblock20.zip"))
    ap.add_argument(
        "--blockassign-vtd-zip",
        type=Path,
        default=Path("Data/BlockAssign_ST13_GA.zip"),
        help="Optional Census block assignment zip. If present, uses BLK20->VTD20 assignment from text file.",
    )
    ap.add_argument(
        "--blockassign-vtd-member",
        default="BlockAssign_ST13_GA_VTD.txt",
        help="Member name inside --blockassign-vtd-zip.",
    )
    ap.add_argument(
        "--nhgis",
        type=Path,
        default=Path("Data/nhgis_blk2010_blk2020_13/nhgis_blk2010_blk2020_13.csv"),
        help="NHGIS blk2010->blk2020 crosswalk CSV with 'blk2010gj','blk2020gj','weight'",
    )
    ap.add_argument("--out-csv", type=Path, default=Path("Data/vtd10_to_vtd20_crosswalk.csv"))
    ap.add_argument("--out-best", type=Path, default=Path("Data/vtd10_to_vtd20_best.json"))
    ap.add_argument(
        "--point-mode",
        choices=["representative_point", "centroid"],
        default="representative_point",
        help="How to convert blocks into join points for spatial assignment",
    )
    args = ap.parse_args()

    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required: {e}")

    use_blockassign_for_b20 = args.blockassign_vtd_zip.exists()

    required = [args.vtd10_zip, args.blk10_zip, args.nhgis]
    if not use_blockassign_for_b20:
        required.extend([args.vtd20_zip, args.blk20_zip])

    for p in required:
        if not p.exists():
            raise SystemExit(f"Missing {p}")

    print("Reading VTD10...")
    vtd10 = gpd.read_file(f"zip://{args.vtd10_zip}").to_crs("EPSG:4326")
    vtd10_geoid_col = _pick_col(vtd10, ["GEOID10", "GEOID"], "VTD10")
    vtd10 = vtd10[[vtd10_geoid_col, "geometry"]].rename(columns={vtd10_geoid_col: "vtd10_geoid"})

    print("Reading BLK10...")
    blk10 = gpd.read_file(f"zip://{args.blk10_zip}").to_crs("EPSG:4326")
    blk10_geoid_col = _pick_col(blk10, ["GEOID10", "GEOID"], "BLK10")
    blk10 = blk10[[blk10_geoid_col, "geometry"]].rename(columns={blk10_geoid_col: "blk2010ge"})
    blk10["blk2010gj"] = _block_geoid_to_gisjoin(blk10["blk2010ge"])

    blk10_pts = blk10.copy()
    if args.point_mode == "centroid":
        blk10_pts["geometry"] = blk10_pts.geometry.centroid
    else:
        blk10_pts["geometry"] = blk10_pts.geometry.representative_point()

    print("Spatial join BLK10 -> VTD10...")
    b10 = gpd.sjoin(blk10_pts[["blk2010gj", "geometry"]], vtd10, how="left", predicate="within")
    b10 = pd.DataFrame(b10.drop(columns=["geometry", "index_right"]))
    b10 = b10.dropna(subset=["vtd10_geoid"])

    if use_blockassign_for_b20:
        print(
            f"Reading BLK20 -> VTD20 from block assignment: "
            f"{args.blockassign_vtd_zip}#{args.blockassign_vtd_member}"
        )
        b20 = _load_blockassign_vtd(args.blockassign_vtd_zip, args.blockassign_vtd_member)
    else:
        print("Block assignment zip not found; falling back to spatial join BLK20 -> VTD20...")
        print("Reading VTD20...")
        vtd20 = gpd.read_file(f"zip://{args.vtd20_zip}").to_crs("EPSG:4326")
        vtd20_geoid_col = _pick_col(vtd20, ["GEOID20", "GEOID"], "VTD20")
        vtd20 = vtd20[[vtd20_geoid_col, "geometry"]].rename(columns={vtd20_geoid_col: "vtd20_geoid"})

        print("Reading BLK20...")
        blk20 = gpd.read_file(f"zip://{args.blk20_zip}").to_crs("EPSG:4326")
        blk20_geoid_col = _pick_col(blk20, ["GEOID20", "GEOID"], "BLK20")
        blk20 = blk20[[blk20_geoid_col, "geometry"]].rename(columns={blk20_geoid_col: "blk2020ge"})
        blk20["blk2020gj"] = _block_geoid_to_gisjoin(blk20["blk2020ge"])

        blk20_pts = blk20.copy()
        if args.point_mode == "centroid":
            blk20_pts["geometry"] = blk20_pts.geometry.centroid
        else:
            blk20_pts["geometry"] = blk20_pts.geometry.representative_point()

        b20 = gpd.sjoin(blk20_pts[["blk2020gj", "geometry"]], vtd20, how="left", predicate="within")
        b20 = pd.DataFrame(b20.drop(columns=["geometry", "index_right"]))
        b20 = b20.dropna(subset=["vtd20_geoid"])

    print("Reading NHGIS weights...")
    w = pd.read_csv(args.nhgis, dtype=str)
    for c in ["blk2010gj", "blk2020gj", "weight"]:
        if c not in w.columns:
            raise SystemExit(f"NHGIS crosswalk missing '{c}' column")
    w["weight"] = pd.to_numeric(w["weight"], errors="coerce").fillna(0.0)
    w = w[w["weight"] > 0]

    print("Joining weights to VTD assignments...")
    w = w.merge(b10, on="blk2010gj", how="inner").merge(b20, on="blk2020gj", how="inner")

    print("Aggregating VTD10 -> VTD20 weights...")
    agg = (
        w.groupby(["vtd10_geoid", "vtd20_geoid"], dropna=False)["weight"]
        .sum()
        .reset_index()
        .rename(columns={"weight": "weight_sum"})
        .sort_values(["vtd10_geoid", "weight_sum"], ascending=[True, False], kind="mergesort")
    )

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(args.out_csv, index=False)
    print(f"Wrote {args.out_csv} ({len(agg)} rows)")

    best = (
        agg.sort_values(["vtd10_geoid", "weight_sum"], ascending=[True, False], kind="mergesort")
        .groupby("vtd10_geoid", sort=False)
        .nth(0)
        .reset_index()
    )
    best_map = {
        r["vtd10_geoid"]: {"to_vtd20_geoid": r["vtd20_geoid"], "weight_sum": float(r["weight_sum"])}
        for _, r in best.iterrows()
    }
    args.out_best.write_text(json.dumps(best_map, indent=2), encoding="utf-8")
    print(f"Wrote {args.out_best} ({len(best_map)} VTD10 keys)")


if __name__ == "__main__":
    main()
