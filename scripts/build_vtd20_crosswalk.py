"""
Build a crosswalk between TIGER 2020 VTD names and OpenElections precinct names.

Goal:
  Improve join coverage when VTD NAME20 doesn't exactly match OpenElections precinct naming.

Inputs:
  --vtd-geojson: GeoJSON made by scripts/prepare_tiger_2020_geojsons.py (expects county_norm + vtd_name_norm)
  --csv: OpenElections-style precinct CSV (expects county + precinct)

Output:
  JSON mapping from geometry join key -> best results join key.
  This lets a client do:
    key = props.join_key_name
    row = results[key] || results[crosswalk[key]]

Usage:
  py scripts/build_vtd20_crosswalk.py --vtd-geojson Data/tl_2020_13_vtd20.geojson --csv Data/20201103__ga__general__precinct.csv --out Data/vtd20_crosswalk_2020.json
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd


def normalize_name_loose(name: str) -> str:
    base = (name or "").replace("\u00a0", " ").strip().upper()
    base = re.sub(r"[^A-Z0-9 .\-]", "", base)
    return re.sub(r"\s+", " ", base).strip()


def extract_precinct_code(precinct_raw: str) -> str:
    p = (precinct_raw or "").strip()
    if not p:
        return ""
    u = p.upper()
    if (
        "ABSENTEE" in u
        or "PROVISIONAL" in u
        or "ONE STOP" in u
        or "CURBSIDE" in u
        or u.startswith("OS ")
        or u.startswith("OS-")
    ):
        return ""
    return re.split(r"[_\s]+", u, maxsplit=1)[0].strip()


def extract_precinct_name(precinct_raw: str) -> str:
    p = (precinct_raw or "").strip()
    if not p:
        return ""
    u = p.upper()
    if (
        "ABSENTEE" in u
        or "PROVISIONAL" in u
        or "ONE STOP" in u
        or "CURBSIDE" in u
        or u.startswith("OS ")
        or u.startswith("OS-")
    ):
        return ""

    parts = re.split(r"[_\s]+", u)
    parts = [x for x in parts if x]
    if not parts:
        return ""

    first = parts[0]
    looks_like_code = bool(
        re.fullmatch(r"\d{1,3}[A-Z]?", first)
        or re.fullmatch(r"[A-Z]{1,4}-\d{1,3}", first)
        or re.fullmatch(r"\d{2}[A-Z]{2,4}", first)
    )
    remainder = parts[1:] if (looks_like_code and len(parts) > 1) else parts
    return normalize_name_loose(" ".join(remainder))


def sim(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(a=a, b=b).ratio()


@dataclass(frozen=True)
class VtdRec:
    geoid20: str
    county_norm: str
    vtd_name_norm: str
    join_key_name: str
    join_key_code: str


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vtd-geojson", type=Path, default=Path("Data/tl_2020_13_vtd20.geojson"))
    ap.add_argument("--csv", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=Path("Data/vtd20_crosswalk.json"))
    ap.add_argument("--threshold", type=float, default=0.88, help="Fuzzy match threshold (0-1)")
    ap.add_argument("--use", choices=["name", "code"], default="name", help="Which VTD join key to map from")
    args = ap.parse_args()

    if not args.vtd_geojson.exists():
        raise SystemExit(f"Missing {args.vtd_geojson}")
    if not args.csv.exists():
        raise SystemExit(f"Missing {args.csv}")

    g = json.loads(args.vtd_geojson.read_text(encoding="utf-8"))
    feats = g.get("features") or []

    vtds_by_county: dict[str, list[VtdRec]] = {}
    for f in feats:
        props = (f or {}).get("properties") or {}
        county_norm = normalize_name_loose(str(props.get("county_norm") or ""))
        vtd_name_norm = normalize_name_loose(str(props.get("vtd_name_norm") or ""))
        geoid20 = str(props.get("GEOID20") or props.get("geoid20") or "")
        join_key_name = str(props.get("join_key_name") or "")
        join_key_code = str(props.get("join_key_code") or "")
        if not (county_norm and vtd_name_norm):
            continue
        vtds_by_county.setdefault(county_norm, []).append(
            VtdRec(
                geoid20=geoid20,
                county_norm=county_norm,
                vtd_name_norm=vtd_name_norm,
                join_key_name=join_key_name,
                join_key_code=join_key_code,
            )
        )

    df = pd.read_csv(args.csv, dtype=str)
    if "county" not in df.columns or "precinct" not in df.columns:
        raise SystemExit("CSV must include 'county' and 'precinct' columns for VTD crosswalk.")

    df["county_norm"] = df["county"].fillna("").astype(str).map(normalize_name_loose)
    df["prec_code"] = df["precinct"].fillna("").astype(str).map(extract_precinct_code)
    df["prec_name"] = df["precinct"].fillna("").astype(str).map(extract_precinct_name)
    df = df[(df["county_norm"] != "") & ((df["prec_name"] != "") | (df["prec_code"] != ""))]

    # Unique precinct parts per county
    prec_by_county: dict[str, set[str]] = {}
    for c, p_name, p_code in df[["county_norm", "prec_name", "prec_code"]].drop_duplicates().itertuples(index=False):
        if not c:
            continue
        if p_name:
            prec_by_county.setdefault(c, set()).add(p_name)
        if p_code:
            prec_by_county.setdefault(c, set()).add(p_code)

    crosswalk: dict[str, dict[str, object]] = {}

    for county_norm, vtds in vtds_by_county.items():
        choices = prec_by_county.get(county_norm) or set()
        if not choices:
            continue

        # Build a quick exact lookup for this county
        by_exact = {v.vtd_name_norm: v for v in vtds}

        for v in vtds:
            from_key = v.join_key_name if args.use == "name" else v.join_key_code
            if not from_key:
                continue

            # Exact match on name
            if v.vtd_name_norm in choices:
                to_key = f"{county_norm} - {v.vtd_name_norm}"
                crosswalk[from_key] = {"to": to_key, "score": 1.0, "method": "exact"}
                continue

            # Fuzzy match within county
            best = ("", 0.0)
            for cand in choices:
                s = sim(v.vtd_name_norm, cand)
                if s > best[1]:
                    best = (cand, s)

            if best[1] >= float(args.threshold) and best[0]:
                to_key = f"{county_norm} - {best[0]}"
                crosswalk[from_key] = {"to": to_key, "score": float(best[1]), "method": "fuzzy"}

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(crosswalk, indent=2), encoding="utf-8")
    print(f"Wrote {args.out} ({len(crosswalk)} mappings)")


if __name__ == "__main__":
    main()

