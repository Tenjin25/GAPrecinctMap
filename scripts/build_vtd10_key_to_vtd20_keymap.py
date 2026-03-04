"""
Build a supplemental key map for rekeying pre-2020 precinct results onto VTD20 GEOID20.

Output format:
  {
    "<COUNTY_NORM> - <PRECINCT_PART>": "<VTD20_GEOID20>",
    ...
  }

Where:
  - COUNTY_NORM follows normalize_county_loose() in build_contest_jsons.py
  - PRECINCT_PART is generated from VTD10 attributes (code/name variants)

This map is intended for --vtd20-supplemental-keymap in scripts/build_contest_jsons.py.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def normalize_county_loose(name: str) -> str:
    base = (name or "").replace("\u00a0", " ").strip().upper()
    base = re.sub(r"[^A-Z0-9 .\-]", "", base)
    return re.sub(r"\s+", " ", base).strip()


def normalize_name_loose(name: str) -> str:
    base = (name or "").replace("\u00a0", " ").strip().upper()
    base = re.sub(r"[^A-Z0-9 .\-]", "", base)
    return re.sub(r"\s+", " ", base).strip()


def _pick_col(columns: list[str], candidates: list[str], label: str) -> str:
    cols = set(columns)
    for c in candidates:
        if c in cols:
            return c
    raise SystemExit(f"{label} missing required column. Tried: {candidates}")


def _key(county_norm: str, precinct_part: str) -> str:
    return f"{county_norm} - {precinct_part}".replace("\u00a0", " ").strip()


def _name_variants(name10: str) -> set[str]:
    out: set[str] = set()
    raw = (name10 or "").strip().upper()
    if not raw:
        return out

    # Many NAME10 values look like "133008 - OLD SALEM"; keep both full and suffix.
    out.add(normalize_name_loose(raw))
    stripped = re.sub(r"^\s*\d+[A-Z0-9]*\s*[-:]\s*", "", raw)
    stripped = normalize_name_loose(stripped)
    if stripped:
        out.add(stripped)
    return {x for x in out if x}


def _code_variants(vtdst10: str) -> set[str]:
    out: set[str] = set()
    code = normalize_name_loose(vtdst10 or "")
    if not code:
        return out

    out.add(code)

    # Numeric-leading code variant: 008 -> 8, 001B -> 1B.
    m = re.fullmatch(r"0*(\d+)([A-Z]?)", code)
    if m:
        out.add(f"{m.group(1)}{m.group(2)}")

    # Keep alphanumeric compact form without spaces.
    compact = re.sub(r"\s+", "", code)
    if compact:
        out.add(compact)

    return {x for x in out if x}


def _load_county_fips_to_norm(path: Path) -> dict[str, str]:
    g = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for f in g.get("features", []):
        props = (f or {}).get("properties") or {}
        if not isinstance(props, dict):
            continue
        countyfp = str(
            props.get("COUNTYFP20")
            or props.get("COUNTYFP10")
            or props.get("COUNTYFP")
            or ""
        ).strip().zfill(3)
        county = normalize_county_loose(
            str(
                props.get("NAME20")
                or props.get("NAME10")
                or props.get("NAME")
                or props.get("COUNTYNAME")
                or ""
            )
        )
        if countyfp and county:
            out[countyfp] = county
    return out


def _load_best_map(path: Path) -> dict[str, str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"Expected object at {path}")
    out: dict[str, str] = {}
    for geoid10, rec in raw.items():
        k = str(geoid10 or "").strip()
        if not k or not isinstance(rec, dict):
            continue
        to20 = str(rec.get("to_vtd20_geoid") or "").strip()
        if to20:
            out[k] = to20
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vtd10-zip", type=Path, default=Path("Data/tl_2012_13_vtd10.zip"))
    ap.add_argument("--county-geojson", type=Path, default=Path("Data/tl_2020_13_county20.geojson"))
    ap.add_argument("--best", type=Path, default=Path("Data/vtd10_to_vtd20_best.json"))
    ap.add_argument("--out", type=Path, default=Path("Data/vtd10_keys_to_vtd20_geoid.json"))
    args = ap.parse_args()

    for p in [args.vtd10_zip, args.county_geojson, args.best]:
        if not p.exists():
            raise SystemExit(f"Missing {p}")

    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required: {e}")

    county_by_fips = _load_county_fips_to_norm(args.county_geojson)
    best = _load_best_map(args.best)

    vtd10 = gpd.read_file(f"zip://{args.vtd10_zip}")
    geoid_col = _pick_col(list(vtd10.columns), ["GEOID10", "GEOID"], "VTD10")
    countyfp_col = _pick_col(list(vtd10.columns), ["COUNTYFP10", "COUNTYFP"], "VTD10")
    code_col = _pick_col(list(vtd10.columns), ["VTDST10", "VTDST"], "VTD10")
    name_col = _pick_col(list(vtd10.columns), ["NAME10", "NAME"], "VTD10")

    mapping: dict[str, str] = {}
    conflicts: set[str] = set()
    used_rows = 0

    for _, row in vtd10.iterrows():
        geoid10 = str(row.get(geoid_col) or "").strip()
        if not geoid10:
            continue
        to20 = best.get(geoid10)
        if not to20:
            continue

        countyfp = str(row.get(countyfp_col) or "").strip().zfill(3)
        county_norm = county_by_fips.get(countyfp, "")
        if not county_norm:
            continue

        parts = set()
        parts |= _code_variants(str(row.get(code_col) or ""))
        parts |= _name_variants(str(row.get(name_col) or ""))
        if not parts:
            continue

        used_rows += 1
        for part in parts:
            k = _key(county_norm, part)
            existing = mapping.get(k)
            if existing is None:
                mapping[k] = to20
            elif existing != to20:
                conflicts.add(k)

    for k in conflicts:
        mapping.pop(k, None)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(dict(sorted(mapping.items())), indent=2), encoding="utf-8")

    print(f"Wrote {args.out} ({len(mapping)} keys)")
    print(f"Rows used from VTD10 with best-match mapping: {used_rows}")
    print(f"Conflicting keys dropped: {len(conflicts)}")


if __name__ == "__main__":
    main()
