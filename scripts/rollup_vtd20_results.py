"""
Roll up a VTD20-keyed contest JSON into county/congressional/legislative totals.

Input:
  A contest JSON produced by scripts/build_contest_jsons.py with level == "vtd20",
  where results are keyed by VTD20 GEOID20 (e.g., "1311100TOCC").

Output:
  Writes one JSON per requested rollup level:
    <out>/<level>/<contest_slug>.json

Notes:
  - This uses spatial assignment from VTD20 polygons -> target polygons using representative points.
  - Candidate names can't be recovered from (DEM/REP/OTH) totals, so winner_candidate is blank;
    winner_party / winner_votes / margin_votes are computed from dem/rep/other.
  - District boundaries included in this repo are 2022-era (CD118/SLDL/SLDU). Using them to roll up
    2020 results is for analysis/visualization and may not match 2020 district lines exactly.

Usage:
  py scripts/rollup_vtd20_results.py --results Data/derived_json_2020_geoid/vtd20/President.json --out Data/derived_json_2020_rollups --levels county,congressional,state_house,state_senate
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Literal

import pandas as pd

Level = Literal["county", "congressional", "state_house", "state_senate"]


def _levels_from_csv(levels: str) -> list[Level]:
    out: list[Level] = []
    for raw in levels.split(","):
        s = raw.strip().lower()
        if not s:
            continue
        if s not in {"county", "congressional", "state_house", "state_senate"}:
            raise SystemExit(f"Unknown level: {raw}")
        out.append(s)  # type: ignore[arg-type]
    return out


def _winner_from_three(dem: int, rep: int, oth: int) -> tuple[str, int, int]:
    items = [("DEM", dem), ("REP", rep), ("OTH", oth)]
    items.sort(key=lambda x: (-x[1], x[0]))
    winner_party, winner_votes = items[0]
    runner_votes = items[1][1]
    margin = winner_votes - runner_votes
    if winner_votes == 0:
        return ("UNK", 0, 0)
    return (winner_party, int(winner_votes), int(margin))


def _read_contest_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_vtd_points(vtd20_geojson: Path):
    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required: {e}")

    vtd = gpd.read_file(vtd20_geojson).to_crs("EPSG:4326")
    if "GEOID20" not in vtd.columns:
        raise SystemExit(f"VTD20 GeoJSON missing GEOID20: {vtd20_geojson}")
    pts = vtd[["GEOID20", "geometry"]].copy()
    pts["geometry"] = pts.geometry.representative_point()
    return pts


def _assign_by_spatial_join(points, boundaries_geojson: Path, *, join_field: str):
    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required: {e}")

    b = gpd.read_file(boundaries_geojson).to_crs("EPSG:4326")
    if join_field not in b.columns:
        raise SystemExit(f"Boundaries missing {join_field}: {boundaries_geojson}")
    b = b[[join_field, "geometry"]].copy()
    joined = gpd.sjoin(points, b, how="left", predicate="within")
    out = pd.DataFrame(joined.drop(columns=["geometry", "index_right"]))
    out[join_field] = out[join_field].astype(str).str.strip()
    return out.rename(columns={join_field: "target_key"})


def _rollup(results_by_geoid20: dict[str, dict[str, object]], mapping: pd.DataFrame) -> dict[str, dict[str, object]]:
    rows: list[dict[str, object]] = []
    map_dict = dict(zip(mapping["GEOID20"].astype(str), mapping["target_key"].astype(str)))
    for geoid20, r in results_by_geoid20.items():
        target = map_dict.get(str(geoid20), "")
        if not target:
            continue
        rows.append(
            {
                "target_key": target,
                "total_votes": int(r.get("total_votes") or 0),
                "dem_votes": int(r.get("dem_votes") or 0),
                "rep_votes": int(r.get("rep_votes") or 0),
                "other_votes": int(r.get("other_votes") or 0),
            }
        )
    if not rows:
        return {}

    df = pd.DataFrame(rows)
    agg = df.groupby("target_key", dropna=False)[["total_votes", "dem_votes", "rep_votes", "other_votes"]].sum()

    out: dict[str, dict[str, object]] = {}
    for key, r in agg.iterrows():
        dem = int(r["dem_votes"])
        rep = int(r["rep_votes"])
        oth = int(r["other_votes"])
        winner_party, winner_votes, margin_votes = _winner_from_three(dem, rep, oth)
        out[str(key)] = {
            "total_votes": int(r["total_votes"]),
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": oth,
            "winner_candidate": "",
            "winner_party": winner_party,
            "winner_votes": winner_votes,
            "margin_votes": margin_votes,
        }
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True, type=Path, help="Input contest JSON (level vtd20)")
    ap.add_argument("--out", required=True, type=Path, help="Output base dir")
    ap.add_argument("--levels", default="county,congressional,state_house,state_senate")
    ap.add_argument("--vtd20-geojson", type=Path, default=Path("Data/tl_2020_13_vtd20.geojson"))
    ap.add_argument("--county-geojson", type=Path, default=Path("Data/tl_2020_13_county20.geojson"))
    ap.add_argument("--cd-geojson", type=Path, default=Path("Data/tl_2022_13_cd118.geojson"))
    ap.add_argument("--sldl-geojson", type=Path, default=Path("Data/tl_2022_13_sldl.geojson"))
    ap.add_argument("--sldu-geojson", type=Path, default=Path("Data/tl_2022_13_sldu.geojson"))
    args = ap.parse_args()

    levels = _levels_from_csv(args.levels)
    contest = _read_contest_json(args.results)
    if str(contest.get("level") or "").strip().lower() != "vtd20":
        raise SystemExit("Input --results must be a vtd20 contest JSON (from build_contest_jsons.py --levels vtd20).")

    office = str(contest.get("office") or "")
    district = str(contest.get("district") or "")
    results = contest.get("results") or {}
    if not isinstance(results, dict):
        raise SystemExit("Invalid contest JSON: results must be an object.")
    results_by_geoid20: dict[str, dict[str, object]] = {
        str(k): (v if isinstance(v, dict) else {}) for k, v in results.items()
    }

    contest_slug = office.strip().replace(" ", "_")
    if district.strip():
        contest_slug = contest_slug + "__" + district.strip().replace(" ", "_")

    if not args.vtd20_geojson.exists():
        raise SystemExit(f"Missing {args.vtd20_geojson}")

    pts = _build_vtd_points(args.vtd20_geojson)

    for level in levels:
        if level == "county":
            if not args.county_geojson.exists():
                raise SystemExit(f"Missing {args.county_geojson}")
            mapping = _assign_by_spatial_join(pts, args.county_geojson, join_field="COUNTYFP20")
        elif level == "congressional":
            mapping = _assign_by_spatial_join(pts, args.cd_geojson, join_field="CD118FP")
        elif level == "state_house":
            mapping = _assign_by_spatial_join(pts, args.sldl_geojson, join_field="SLDLST")
        elif level == "state_senate":
            mapping = _assign_by_spatial_join(pts, args.sldu_geojson, join_field="SLDUST")
        else:
            raise AssertionError(level)

        rolled = _rollup(results_by_geoid20, mapping)

        out_path = args.out / level / f"{contest_slug}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "office": office,
            "district": district,
            "level": level,
            "keys": f"Rolled up from VTD20 GEOID20 using spatial assignment to {level}.",
            "results": rolled,
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {out_path} ({len(rolled)} keys)")


if __name__ == "__main__":
    main()

