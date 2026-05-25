"""
Approximate pre-2014 district contests from county-level contest files.

Method:
- Build county->district area weights using county and district geometries.
- Allocate county totals into districts by those weights.

Outputs are separate and explicitly approximate:
- Data/district_contests_pre2014_county_approx/
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd


def normalize_district_number(raw: str) -> str:
    s = (raw or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return str(int(digits)) if digits else s


def normalize_county(name: str) -> str:
    return (name or "").strip().upper()


def build_county_weights(county_geojson: Path, district_geojson: Path, district_field: str) -> dict[str, list[tuple[str, float]]]:
    c = gpd.read_file(county_geojson)[["NAME20", "geometry"]].copy()
    d = gpd.read_file(district_geojson)[[district_field, "geometry"]].copy()
    c["county"] = c["NAME20"].astype(str).map(normalize_county)
    d["district"] = d[district_field].astype(str).map(normalize_district_number)
    c = c.to_crs("EPSG:5070")
    d = d.to_crs("EPSG:5070")
    inter = gpd.overlay(c[["county", "geometry"]], d[["district", "geometry"]], how="intersection", keep_geom_type=False)
    inter["area"] = inter.geometry.area
    inter = inter[inter["area"] > 0].copy()
    totals = inter.groupby("county")["area"].sum().to_dict()
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for _, r in inter.iterrows():
        county = str(r["county"])
        dist = str(r["district"])
        total = float(totals.get(county, 0.0))
        if total <= 0:
            continue
        w = float(r["area"]) / total
        if not math.isfinite(w) or w <= 0:
            continue
        out[county].append((dist, w))
    # coalesce duplicates and normalize
    final: dict[str, list[tuple[str, float]]] = {}
    for county, vals in out.items():
        agg: dict[str, float] = defaultdict(float)
        for dist, w in vals:
            agg[dist] += w
        s = sum(agg.values())
        if s <= 0:
            continue
        final[county] = sorted([(dist, w / s) for dist, w in agg.items()], key=lambda x: (int(x[0]) if x[0].isdigit() else x[0]))
    return final


def allocate_county_rows(rows: list[dict], weights: dict[str, list[tuple[str, float]]]) -> dict[str, dict[str, int]]:
    by_district: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        county = normalize_county(str(row.get("county") or ""))
        wlist = weights.get(county)
        if not wlist:
            continue
        dem = int(row.get("dem_votes", 0) or 0)
        rep = int(row.get("rep_votes", 0) or 0)
        oth = int(row.get("other_votes", 0) or 0)
        tot = int(row.get("total_votes", 0) or 0)
        for dist, w in wlist:
            by_district[dist]["dem_votes"] += int(round(dem * w))
            by_district[dist]["rep_votes"] += int(round(rep * w))
            by_district[dist]["other_votes"] += int(round(oth * w))
            by_district[dist]["total_votes"] += int(round(tot * w))
    return by_district


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contests-dir", type=Path, default=Path("Data/contests"))
    ap.add_argument("--out-dir", type=Path, default=Path("Data/district_contests_pre2014_county_approx"))
    ap.add_argument("--county-geojson", type=Path, default=Path("Data/tl_2020_13_county20.geojson"))
    ap.add_argument("--cd-geojson", type=Path, default=Path("Data/tl_2022_13_cd118.geojson"))
    ap.add_argument("--sldl-geojson", type=Path, default=Path("Data/tl_2022_13_sldl.geojson"))
    ap.add_argument("--sldu-geojson", type=Path, default=Path("Data/tl_2022_13_sldu.geojson"))
    ap.add_argument("--max-year", type=int, default=2012)
    args = ap.parse_args()

    weights = {
        "congressional": build_county_weights(args.county_geojson, args.cd_geojson, "CD118FP"),
        "state_house": build_county_weights(args.county_geojson, args.sldl_geojson, "SLDLST"),
        "state_senate": build_county_weights(args.county_geojson, args.sldu_geojson, "SLDUST"),
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files = []
    for p in sorted(args.contests_dir.glob("*.json")):
        if p.name == "manifest.json":
            continue
        contest = json.loads(p.read_text(encoding="utf-8-sig"))
        year = int(contest.get("year") or 0)
        if year <= 0 or year > args.max_year:
            continue
        contest_type = str(contest.get("contest_type") or "").strip()
        rows = contest.get("rows") or []
        if not contest_type or not isinstance(rows, list) or not rows:
            continue
        for scope in ("congressional", "state_house", "state_senate"):
            agg = allocate_county_rows(rows, weights[scope])
            results = {}
            for dist, vals in agg.items():
                dem = int(vals.get("dem_votes", 0))
                rep = int(vals.get("rep_votes", 0))
                oth = int(vals.get("other_votes", 0))
                tot = int(vals.get("total_votes", 0))
                results[str(dist)] = {
                    "dem_votes": dem,
                    "rep_votes": rep,
                    "other_votes": oth,
                    "total_votes": tot,
                }
            payload = {
                "meta": {
                    "source": "county_approximation",
                    "note": "Pre-2014 district approximation from county totals using county->district area weights.",
                    "year": year,
                    "contest_type": contest_type,
                    "scope": scope,
                },
                "general": {"results": results},
            }
            out_name = f"{scope}_{contest_type}_{year}.json"
            out_path = args.out_dir / out_name
            out_path.write_text(json.dumps(payload), encoding="utf-8")
            manifest_files.append({"scope": scope, "contest_type": contest_type, "year": year, "file": out_name, "rows": len(results)})

    manifest = {
        "source": "county_approximation_pre2014",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "files": sorted(manifest_files, key=lambda x: (x["scope"], x["contest_type"], x["year"])),
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote {args.out_dir / 'manifest.json'} ({len(manifest_files)} files)")


if __name__ == "__main__":
    main()
