"""
Compare join-key coverage between a geometry GeoJSON and a contest results JSON.

Usage:
  py scripts/check_join_coverage.py --geojson Data/tl_2020_13_vtd20.geojson --geojson-key join_key_name --results Data/derived_json_2020/vtd/President.json
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--geojson", required=True, type=Path)
    ap.add_argument("--geojson-key", default="join_key", help="Property name in GeoJSON features")
    ap.add_argument("--results", required=True, type=Path, help="Contest JSON from scripts/build_contest_jsons.py")
    ap.add_argument("--show", type=int, default=20, help="How many sample keys to show")
    ap.add_argument("--by-county", action="store_true", help="Show counties with the most geometry keys lacking results")
    ap.add_argument("--county-key", default="county_norm", help="County property for --by-county")
    args = ap.parse_args()

    g = json.loads(args.geojson.read_text(encoding="utf-8"))
    feats = g.get("features") or []
    geo_keys: set[str] = set()
    county_keys: dict[str, set[str]] = defaultdict(set)
    for f in feats:
        props = (f or {}).get("properties") or {}
        k = props.get(args.geojson_key)
        if k is None:
            continue
        ks = str(k).strip()
        if ks:
            geo_keys.add(ks)
            if args.by_county:
                county = str(props.get(args.county_key) or "UNKNOWN").strip().upper()
                county_keys[county].add(ks)

    r = json.loads(args.results.read_text(encoding="utf-8"))
    results = r.get("results") or {}
    res_keys = {str(k).strip() for k in results.keys() if str(k).strip()}

    inter = geo_keys & res_keys
    only_geo = geo_keys - res_keys
    only_res = res_keys - geo_keys

    print(f"Geo features: {len(feats)}")
    print(f"Geo keys({args.geojson_key}): {len(geo_keys)}")
    print(f"Result keys: {len(res_keys)}")
    print(f"Intersection: {len(inter)}")
    if geo_keys:
        print(f"Coverage (results on geometry): {len(inter) / len(geo_keys):.1%}")
    if res_keys:
        print(f"Coverage (geometry for results): {len(inter) / len(res_keys):.1%}")

    if args.by_county:
        print("\nCounty gaps (missing / geometry keys):")
        gaps = sorted(
            ((len(keys - res_keys), county, len(keys)) for county, keys in county_keys.items()),
            reverse=True,
        )
        for missing, county, total in gaps:
            if missing:
                print(f"- {county}: {missing} / {total}")

    show = max(0, int(args.show))
    if show:
        if only_res:
            print("\nSample result-only keys:")
            for k in sorted(only_res)[:show]:
                print(f"- {k}")
        if only_geo:
            print("\nSample geometry-only keys:")
            for k in sorted(only_geo)[:show]:
                print(f"- {k}")


if __name__ == "__main__":
    main()
