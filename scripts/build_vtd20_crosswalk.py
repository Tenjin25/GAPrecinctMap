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


def _strip_trailing_parenthetical(name: str) -> str:
    return re.sub(r"\s*\([^)]*\)\s*$", "", (name or "").strip()).strip()


def normalize_name_match(name: str) -> str:
    """
    Normalization tuned for VTD<->precinct name matching.

    Keep this conservative: it should make common variants collide (ELEM/ELEMENTARY),
    but avoid aggressive stemming that could create false positives.
    """
    s = normalize_name_loose(name)
    if not s:
        return ""

    # Normalize common abbreviations.
    s = re.sub(r"\bCTR\b", "CENTER", s)
    s = re.sub(r"\bSCH\b", "SCHOOL", s)
    s = re.sub(r"\bSTA\b", "STATION", s)
    s = re.sub(r"\bDEPT\b", "DEPARTMENT", s)

    # Normalize common facility words.
    s = re.sub(r"\bELEMENTARY\b", "ELEM", s)
    s = re.sub(r"\bMIDDLE\b", "MID", s)
    s = re.sub(r"\bCOMMUNITY\s+CENTER\b", "COMM CENTER", s)

    # Normalize common street-type words.
    s = re.sub(r"\bROAD\b", "RD", s)
    s = re.sub(r"\bSTREET\b", "ST", s)
    s = re.sub(r"\bAVENUE\b", "AVE", s)
    s = re.sub(r"\bDRIVE\b", "DR", s)
    s = re.sub(r"\bBOULEVARD\b", "BLVD", s)
    s = re.sub(r"\bHIGHWAY\b", "HWY", s)
    s = re.sub(r"\bMOUNTAIN\b", "MTN", s)

    # Normalize directions (helps match "NORTH" vs "N").
    s = re.sub(r"\bNORTHEAST\b", "NE", s)
    s = re.sub(r"\bNORTHWEST\b", "NW", s)
    s = re.sub(r"\bSOUTHEAST\b", "SE", s)
    s = re.sub(r"\bSOUTHWEST\b", "SW", s)
    s = re.sub(r"\bNORTH\b", "N", s)
    s = re.sub(r"\bSOUTH\b", "S", s)
    s = re.sub(r"\bEAST\b", "E", s)
    s = re.sub(r"\bWEST\b", "W", s)

    # Strip leading zeros in numeric tokens (e.g., "02" -> "2").
    s = re.sub(r"\b0+([0-9])\b", r"\1", s)

    return re.sub(r"\s+", " ", s).strip()


_LOOSE_SUFFIX_DROP = {
    "CENTER",
    "BUILDING",
    "BLDG",
    "SCHOOL",
    "STATION",
    "HALL",
    "COURTHOUSE",
    "FIRE",
    "ROOM",
    "RM",
}


def loosen_name_match(name_match: str) -> str:
    """
    A slightly looser form used only for secondary matching.

    Removes some common trailing facility tokens so "SENIOR CITIZEN" can match
    "SENIOR CITIZEN CENTER", etc.
    """
    s = (name_match or "").strip()
    if not s:
        return ""
    parts = s.split()
    while parts and parts[-1] in _LOOSE_SUFFIX_DROP:
        parts.pop()
    return " ".join(parts).strip()


def strip_leading_numeric_token(name_match: str) -> str:
    """
    Drop a leading numeric-ish token when it looks like a precinct code.

    Examples:
      "3 CJC" -> "CJC"
      "3172A YOUNG FARMERS" -> "YOUNG FARMERS"
    """
    s = (name_match or "").strip()
    if not s:
        return ""
    parts = s.split()
    if len(parts) < 2:
        return ""
    if re.fullmatch(r"\d{1,4}[A-Z]?", parts[0]):
        return " ".join(parts[1:]).strip()
    return ""


def compact_name_match(name: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", normalize_name_match(name))


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
    vtd_name_base_norm: str
    vtd_name_match: str
    vtd_name_base_match: str
    vtd_name_loose: str
    vtd_name_base_loose: str
    vtd_name_compact: str
    vtd_name_base_compact: str
    vtd_name_loose_compact: str
    vtd_name_base_loose_compact: str
    join_key_name: str
    join_key_code: str


@dataclass(frozen=True)
class ChoiceRec:
    raw: str  # precinct part (already normalized loose by extract_*)
    match: str
    loose: str
    compact: str
    loose_compact: str


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
        name20 = str(props.get("NAME20") or "").strip()
        vtd_name_norm = normalize_name_loose(str(props.get("vtd_name_norm") or name20 or ""))
        vtd_name_base_norm = normalize_name_loose(_strip_trailing_parenthetical(name20)) if ("(" in name20 and ")" in name20) else ""
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
                vtd_name_base_norm=vtd_name_base_norm,
                vtd_name_match=normalize_name_match(vtd_name_norm),
                vtd_name_base_match=normalize_name_match(vtd_name_base_norm) if vtd_name_base_norm else "",
                vtd_name_loose=loosen_name_match(normalize_name_match(vtd_name_norm)),
                vtd_name_base_loose=loosen_name_match(normalize_name_match(vtd_name_base_norm)) if vtd_name_base_norm else "",
                vtd_name_compact=compact_name_match(vtd_name_norm),
                vtd_name_base_compact=compact_name_match(vtd_name_base_norm) if vtd_name_base_norm else "",
                vtd_name_loose_compact=re.sub(r"[^A-Z0-9]", "", loosen_name_match(normalize_name_match(vtd_name_norm))),
                vtd_name_base_loose_compact=re.sub(r"[^A-Z0-9]", "", loosen_name_match(normalize_name_match(vtd_name_base_norm))) if vtd_name_base_norm else "",
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

    # Unique precinct parts per county, with match-friendly normalization.
    prec_by_county: dict[str, list[ChoiceRec]] = {}
    seen_by_county: dict[str, set[str]] = {}
    for c, p_name, p_code in df[["county_norm", "prec_name", "prec_code"]].drop_duplicates().itertuples(index=False):
        if not c:
            continue
        for raw in (p_name, p_code):
            if not raw:
                continue
            raw_s = str(raw).strip()
            if not raw_s:
                continue
            seen = seen_by_county.setdefault(c, set())
            if raw_s in seen:
                continue
            seen.add(raw_s)
            prec_by_county.setdefault(c, []).append(
                ChoiceRec(
                    raw=raw_s,
                    match=normalize_name_match(raw_s),
                    loose=loosen_name_match(normalize_name_match(raw_s)),
                    compact=compact_name_match(raw_s),
                    loose_compact=re.sub(r"[^A-Z0-9]", "", loosen_name_match(normalize_name_match(raw_s))),
                )
            )

    crosswalk: dict[str, dict[str, object]] = {}

    for county_norm, vtds in vtds_by_county.items():
        choices = prec_by_county.get(county_norm) or []
        if not choices:
            continue

        by_match: dict[str, set[str]] = {}
        by_loose: dict[str, set[str]] = {}
        by_compact: dict[str, set[str]] = {}
        by_loose_compact: dict[str, set[str]] = {}
        for ch in choices:
            if ch.match:
                by_match.setdefault(ch.match, set()).add(ch.raw)
            if ch.loose:
                by_loose.setdefault(ch.loose, set()).add(ch.raw)
            if ch.compact:
                by_compact.setdefault(ch.compact, set()).add(ch.raw)
            if ch.loose_compact:
                by_loose_compact.setdefault(ch.loose_compact, set()).add(ch.raw)

        for v in vtds:
            from_key = v.join_key_name if args.use == "name" else v.join_key_code
            if not from_key:
                continue

            # Preferred exact matches (including a "base" variant when NAME20 has a parenthetical suffix).
            v_variants = [  # (label, strict, loose, compact, loose_compact)
                ("name", v.vtd_name_match, v.vtd_name_loose, v.vtd_name_compact, v.vtd_name_loose_compact),
            ]
            if v.vtd_name_base_match:
                v_variants.append(("base", v.vtd_name_base_match, v.vtd_name_base_loose, v.vtd_name_base_compact, v.vtd_name_base_loose_compact))

            # Add "numeric code stripped" variants (helps when VTD names embed codes but results don't).
            extra: list[tuple[str, str, str, str, str]] = []
            for label, strict, loose, comp, loose_comp in v_variants:
                stripped = strip_leading_numeric_token(strict)
                if stripped:
                    stripped_loose = loosen_name_match(stripped)
                    extra.append((f"{label}_numless", stripped, stripped_loose, re.sub(r"[^A-Z0-9]", "", stripped), re.sub(r"[^A-Z0-9]", "", stripped_loose)))
            v_variants.extend(extra)

            picked_raw = ""
            picked_score = 0.0
            picked_method = ""

            for label, v_match, _, _, _ in v_variants:
                raws = by_match.get(v_match) if v_match else None
                if raws and len(raws) == 1:
                    picked_raw = next(iter(raws))
                    picked_score = 1.0
                    picked_method = "exact" if label == "name" else "exact_base"
                    break
            if not picked_raw:
                for label, _, v_loose, _, _ in v_variants:
                    raws = by_loose.get(v_loose) if v_loose else None
                    if raws and len(raws) == 1:
                        picked_raw = next(iter(raws))
                        picked_score = 1.0
                        picked_method = "exact_loose" if label == "name" else "exact_loose_base"
                        break
            if not picked_raw:
                for label, _, _, v_compact, _ in v_variants:
                    raws = by_compact.get(v_compact) if v_compact else None
                    if raws and len(raws) == 1:
                        picked_raw = next(iter(raws))
                        picked_score = 1.0
                        picked_method = "compact" if label == "name" else "compact_base"
                        break
            if not picked_raw:
                for label, _, _, _, v_compact in v_variants:
                    raws = by_loose_compact.get(v_compact) if v_compact else None
                    if raws and len(raws) == 1:
                        picked_raw = next(iter(raws))
                        picked_score = 1.0
                        picked_method = "compact_loose" if label == "name" else "compact_loose_base"
                        break

            # Compact containment (e.g. "SENIORCITIZEN" vs "SENIORCITIZENCENTER").
            if not picked_raw:
                hits: set[str] = set()
                for _, _, _, v_compact, _ in v_variants:
                    if not v_compact or len(v_compact) < 8:
                        continue
                    for ch in choices:
                        if not ch.compact:
                            continue
                        if v_compact in ch.compact or ch.compact in v_compact:
                            hits.add(ch.raw)
                    if len(hits) > 1:
                        break
                if len(hits) == 1:
                    picked_raw = next(iter(hits))
                    picked_score = 0.99
                    picked_method = "compact_contains"

            # Fuzzy fallback within county using the match-normalized strings.
            if not picked_raw:
                best = ("", 0.0)
                second = 0.0
                for ch in choices:
                    if not ch.match:
                        continue
                    s = 0.0
                    for _, v_match, v_loose, _, _ in v_variants:
                        if v_match:
                            s = max(s, sim(v_match, ch.match))
                        if v_loose and ch.loose:
                            s = max(s, sim(v_loose, ch.loose))
                    if s > best[1]:
                        second = best[1]
                        best = (ch.raw, s)
                    elif s > second:
                        second = s

                # Require either (a) a clear winner, or (b) a high absolute similarity.
                # This helps avoid accidental "closest" picks when multiple precincts are similar.
                if best[0] and best[1] >= float(args.threshold):
                    if (best[1] - second) >= 0.06 or best[1] >= 0.93:
                        picked_raw = best[0]
                        picked_score = float(best[1])
                        picked_method = "fuzzy"

            if picked_raw:
                to_key = f"{county_norm} - {picked_raw}"
                crosswalk[from_key] = {"to": to_key, "score": picked_score, "method": picked_method}

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(crosswalk, indent=2), encoding="utf-8")
    print(f"Wrote {args.out} ({len(crosswalk)} mappings)")


if __name__ == "__main__":
    main()
