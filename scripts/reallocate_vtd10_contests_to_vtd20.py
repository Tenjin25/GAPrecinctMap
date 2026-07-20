"""
Reallocate older (pre-2020) precinct contest results onto VTD20 via the full
VTD10→VTD20 weight matrix (block/NHGIS path already encoded in that CSV).

Preferred path:
  1) Match OpenElections precincts → VTD10 GEOID10 (exact key variants + fuzzy names)
  2) Aggregate contest totals onto VTD10
  3) Split each VTD10's votes across VTD20 destinations using share-normalized
     weights from Data/vtd10_to_vtd20_crosswalk.csv
  4) Write VTD20-keyed contest JSONs (same shape as build_contest_jsons.py)

Optional fallback for precincts that do not match any VTD10:
  use the existing name-fuzzy VTD20 crosswalk + supplemental keymap (shortcut path)
  so unmatched mass is not dropped.

PSC / Public Service Commissioner contests are excluded by default.

Usage:
  py scripts/reallocate_vtd10_contests_to_vtd20.py \\
    --csv Data/20121106__ga__general__precinct.csv \\
    --year 2012 \\
    --out Data/derived_vtd20_blockpath/2012/contests \\
    --vtd20-crosswalk Data/derived_vtd20/2012/vtd20_crosswalk_2012.json \\
    --vtd20-supplemental-keymap Data/vtd10_keys_to_vtd20_geoid.json \\
    --only-offices President
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd

# Reuse contest-builder helpers for consistent keys / payload shape.
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_contest_jsons as bcj  # noqa: E402


PSC_OFFICE_RE = re.compile(r"PUBLIC\s+SERVICE\s+COMMISSION", re.IGNORECASE)


def _is_psc_office(office: str) -> bool:
    return bool(PSC_OFFICE_RE.search(office or ""))


def _name_variants(name10: str) -> set[str]:
    out: set[str] = set()
    raw = (name10 or "").strip().upper()
    if not raw:
        return out
    out.add(bcj.normalize_name_loose(raw))
    stripped = re.sub(r"^\s*\d+[A-Z0-9]*\s*[-:]\s*", "", raw)
    stripped = bcj.normalize_name_loose(stripped)
    if stripped:
        out.add(stripped)
    return {x for x in out if x}


def _code_variants(vtdst10: str) -> set[str]:
    out: set[str] = set()
    code = bcj.normalize_name_loose(vtdst10 or "")
    if not code:
        return out
    out.add(code)
    m = re.fullmatch(r"0*(\d+)([A-Z]?)", code)
    if m:
        out.add(f"{m.group(1)}{m.group(2)}")
    compact = re.sub(r"\s+", "", code)
    if compact:
        out.add(compact)
    return {x for x in out if x}


def _normalize_name_match(name: str) -> str:
    s = bcj.normalize_name_loose(name)
    if not s:
        return ""
    s = s.replace("/", " ").replace("&", " ").replace("@", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\bCTR\b", "CENTER", s)
    s = re.sub(r"\bSCH\b", "SCHOOL", s)
    s = re.sub(r"\bSTA\b", "STATION", s)
    s = re.sub(r"\bELEMENTARY\b", "ELEM", s)
    s = re.sub(r"\bMIDDLE\b", "MID", s)
    s = re.sub(r"\bCOMMUNITY\s+CENTER\b", "COMM CENTER", s)
    s = re.sub(r"\bROAD\b", "RD", s)
    s = re.sub(r"\bSTREET\b", "ST", s)
    s = re.sub(r"\bAVENUE\b", "AVE", s)
    s = re.sub(r"\bDRIVE\b", "DR", s)
    s = re.sub(r"\bHIGHWAY\b", "HWY", s)
    s = re.sub(r"\bMOUNTAIN\b", "MTN", s)
    return s


def _load_county_fips_to_norm(path: Path) -> dict[str, str]:
    g = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for f in g.get("features", []):
        props = (f or {}).get("properties") or {}
        if not isinstance(props, dict):
            continue
        countyfp = str(
            props.get("COUNTYFP20") or props.get("COUNTYFP10") or props.get("COUNTYFP") or ""
        ).strip().zfill(3)
        county = bcj.normalize_county_loose(
            str(props.get("NAME20") or props.get("NAME10") or props.get("NAME") or props.get("COUNTYNAME") or "")
        )
        if countyfp and county:
            out[countyfp] = county
    return out


def build_precinct_key_to_vtd10(
    *,
    vtd10_zip: Path,
    county_geojson: Path,
) -> tuple[dict[str, str], dict[str, list[tuple[str, str]]]]:
    """
    Returns:
      exact_map: '<COUNTY> - <PART>' -> vtd10_geoid
      fuzzy_cands: county_norm -> [(match_name, vtd10_geoid), ...]
    """
    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required to read VTD10: {e}")

    county_by_fips = _load_county_fips_to_norm(county_geojson)
    vtd10 = gpd.read_file(f"zip://{vtd10_zip}")

    exact: dict[str, str] = {}
    conflicts: set[str] = set()
    fuzzy_cands: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for _, row in vtd10.iterrows():
        geoid10 = str(row.get("GEOID10") or row.get("GEOID") or "").strip()
        if not geoid10:
            continue
        countyfp = str(row.get("COUNTYFP10") or row.get("COUNTYFP") or "").strip().zfill(3)
        county = county_by_fips.get(countyfp, "")
        if not county:
            continue

        name10 = str(row.get("NAME10") or row.get("NAME") or "")
        code10 = str(row.get("VTDST10") or row.get("VTDST") or "")
        parts = _code_variants(code10) | _name_variants(name10)
        for part in parts:
            k = f"{county} - {part}".replace("\u00a0", " ").strip()
            prev = exact.get(k)
            if prev is None:
                exact[k] = geoid10
            elif prev != geoid10:
                conflicts.add(k)

        stripped = re.sub(r"^\s*\d+[A-Z0-9]*\s*[-:]\s*", "", name10)
        for cand in {
            _normalize_name_match(name10),
            _normalize_name_match(stripped),
            _normalize_name_match(code10),
        }:
            if cand:
                fuzzy_cands[county].append((cand, geoid10))

    for k in conflicts:
        exact.pop(k, None)

    return exact, dict(fuzzy_cands)


def load_precinct_aliases(path: Path | None) -> dict[str, dict[str, list[str]]]:
    """Load county -> {csv_token -> [expanded names]} (same shape as build_vtd20_crosswalk)."""
    if path is None or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, list[str]]] = {}
    for county, mapping in raw.items():
        if str(county).startswith("_") or not isinstance(mapping, dict):
            continue
        county_norm = bcj.normalize_name_loose(str(county))
        if not county_norm:
            continue
        cleaned: dict[str, list[str]] = {}
        for token, names in mapping.items():
            tok = bcj.normalize_name_loose(str(token))
            if not tok:
                continue
            vals: list[str] = []
            if isinstance(names, str):
                names = [names]
            if not isinstance(names, list):
                continue
            for name in names:
                n = bcj.normalize_name_loose(str(name))
                if n and n not in vals:
                    vals.append(n)
            if vals:
                cleaned[tok] = vals
        if cleaned:
            out[county_norm] = cleaned
    return out


def _alias_parts(county_norm: str, precinct_part: str, aliases: dict[str, dict[str, list[str]]]) -> list[str]:
    """Return unique precinct name/token variants to try for matching (original first)."""
    parts: list[str] = []
    seen: set[str] = set()

    def _add(p: str) -> None:
        s = (p or "").strip()
        if not s:
            return
        key = s.upper()
        if key in seen:
            return
        seen.add(key)
        parts.append(s)

    _add(precinct_part)
    county_aliases = aliases.get(county_norm) or {}
    tok = bcj.normalize_name_loose(precinct_part)
    for expanded in county_aliases.get(tok, []):
        _add(expanded)
    return parts


def match_precinct_to_vtd10(
    *,
    county_norm: str,
    precinct_part: str,
    exact_map: dict[str, str],
    fuzzy_cands: dict[str, list[tuple[str, str]]],
    fuzzy_threshold: float,
    aliases: dict[str, dict[str, list[str]]] | None = None,
) -> tuple[str | None, str, float]:
    """Returns (vtd10_geoid|None, method, score)."""
    if not county_norm or not precinct_part:
        return None, "empty", 0.0

    alias_map = aliases or {}
    parts = _alias_parts(county_norm, precinct_part, alias_map)
    best_unmatched_score = 0.0

    for i, part in enumerate(parts):
        via_alias = i > 0
        key = f"{county_norm} - {part}".replace("\u00a0", " ").strip()
        hit = exact_map.get(key)
        if hit:
            return hit, "exact_alias" if via_alias else "exact", 1.0

        # Also try code-loose form of the key.
        key_code = bcj.normalize_join_key_code_loose(key)
        if key_code and key_code != key:
            hit = exact_map.get(key_code)
            if hit:
                return hit, "exact_code_loose_alias" if via_alias else "exact_code_loose", 1.0

        target = _normalize_name_match(part)
        if not target:
            continue

        best_geoid: str | None = None
        best_score = 0.0
        for cand, geoid10 in fuzzy_cands.get(county_norm, []):
            score = SequenceMatcher(None, target, cand).ratio()
            if score > best_score:
                best_score = score
                best_geoid = geoid10

        best_unmatched_score = max(best_unmatched_score, best_score)
        if best_geoid and best_score >= fuzzy_threshold:
            return best_geoid, "fuzzy_alias" if via_alias else "fuzzy", float(best_score)

    return None, "unmatched", float(best_unmatched_score)


def load_vtd10_to_vtd20_shares(path: Path) -> dict[str, list[tuple[str, float]]]:
    df = pd.read_csv(path, dtype=str)
    for col in ("vtd10_geoid", "vtd20_geoid", "weight_sum"):
        if col not in df.columns:
            raise SystemExit(f"{path} missing '{col}'")
    df["weight_sum"] = pd.to_numeric(df["weight_sum"], errors="coerce").fillna(0.0)
    df = df[df["weight_sum"] > 0].copy()
    totals = df.groupby("vtd10_geoid")["weight_sum"].transform("sum")
    df["share"] = df["weight_sum"] / totals
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in df.itertuples(index=False):
        out[str(r.vtd10_geoid)].append((str(r.vtd20_geoid), float(r.share)))
    return dict(out)


def _allocate_int_shares(total: int, shares: list[tuple[str, float]]) -> dict[str, int]:
    """Largest-remainder allocation so sum(out) == total."""
    if total == 0 or not shares:
        return {}
    raw = [(dst, total * share) for dst, share in shares]
    floors = {dst: int(math.floor(val)) for dst, val in raw}
    assigned = sum(floors.values())
    remainders = sorted(((val - floors[dst], dst) for dst, val in raw), reverse=True)
    leftover = total - assigned
    for i in range(max(0, leftover)):
        floors[remainders[i][1]] += 1
    return {dst: n for dst, n in floors.items() if n}


def _empty_bucket() -> dict[str, Any]:
    return {
        "total_votes": 0,
        "dem_votes": 0,
        "rep_votes": 0,
        "other_votes": 0,
        "by_candidate": defaultdict(lambda: {"votes": 0, "party": "UNK"}),
    }


def _finalize_bucket(bucket: dict[str, Any]) -> dict[str, object]:
    total = int(bucket["total_votes"])
    dem = int(bucket["dem_votes"])
    rep = int(bucket["rep_votes"])
    other = max(0, total - dem - rep)
    by_cand: dict[str, dict[str, Any]] = bucket["by_candidate"]
    ranked = sorted(
        ((name, int(rec["votes"]), str(rec["party"])) for name, rec in by_cand.items()),
        key=lambda t: (-t[1], t[0]),
    )
    winner_name = ranked[0][0] if ranked else ""
    winner_votes = ranked[0][1] if ranked else 0
    winner_party = ranked[0][2] if ranked else "UNK"
    runner_votes = ranked[1][1] if len(ranked) > 1 else 0

    dem_ranked = sorted(
        ((name, int(rec["votes"])) for name, rec in by_cand.items() if str(rec.get("party")) == "DEM"),
        key=lambda t: (-t[1], t[0]),
    )
    rep_ranked = sorted(
        ((name, int(rec["votes"])) for name, rec in by_cand.items() if str(rec.get("party")) == "REP"),
        key=lambda t: (-t[1], t[0]),
    )
    dem_candidate = bcj.decorate_candidate_label(dem_ranked[0][0], "DEM") if dem_ranked else ""
    rep_candidate = bcj.decorate_candidate_label(rep_ranked[0][0], "REP") if rep_ranked else ""

    return {
        "total_votes": total,
        "dem_votes": dem,
        "rep_votes": rep,
        "other_votes": other,
        "dem_candidate": dem_candidate,
        "rep_candidate": rep_candidate,
        "winner_candidate": bcj.decorate_candidate_label(winner_name, winner_party),
        "winner_party": winner_party,
        "winner_votes": winner_votes,
        "margin_votes": int(winner_votes - runner_votes),
    }


def _add_precinct_row_to_vtd20(
    *,
    dest: dict[str, dict[str, Any]],
    geoid20: str,
    votes: int,
    party_norm: str,
    candidate: str,
) -> None:
    if votes == 0:
        return
    b = dest.setdefault(geoid20, _empty_bucket())
    b["total_votes"] += votes
    if party_norm == "DEM":
        b["dem_votes"] += votes
    elif party_norm == "REP":
        b["rep_votes"] += votes
    cand = b["by_candidate"][candidate]
    cand["votes"] += votes
    if party_norm != "UNK":
        cand["party"] = party_norm


def reallocate_contest(
    *,
    sub: pd.DataFrame,
    exact_map: dict[str, str],
    fuzzy_cands: dict[str, list[tuple[str, str]]],
    shares: dict[str, list[tuple[str, float]]],
    fuzzy_threshold: float,
    fallback_rekey: dict[str, str] | None,
    aliases: dict[str, dict[str, list[str]]] | None = None,
) -> tuple[dict[str, dict[str, object]], dict[str, Any]]:
    """
    sub columns: county_norm, precinct_part, result_key, candidate, party_norm, votes
    """
    stats: dict[str, Any] = {
        "source_votes": int(sub["votes"].sum()),
        "matched_vtd10_votes": 0,
        "fallback_vtd20_votes": 0,
        "dropped_votes": 0,
        "precincts": 0,
        "matched_exact": 0,
        "matched_fuzzy": 0,
        "matched_alias": 0,
        "fallback": 0,
        "dropped": 0,
        "vtd10_missing_shares": 0,
    }

    # Aggregate candidate rows per precinct first.
    precinct_groups = sub.groupby(["result_key", "county_norm", "precinct_part"], dropna=False)
    dest_buckets: dict[str, dict[str, Any]] = {}

    for (result_key, county_norm, precinct_part), g in precinct_groups:
        stats["precincts"] += 1
        rows = [
            (str(r.candidate), str(r.party_norm), int(r.votes))
            for r in g.itertuples(index=False)
            if int(r.votes) != 0
        ]
        precinct_total = sum(v for _, _, v in rows)
        if precinct_total <= 0:
            continue

        vtd10, method, _score = match_precinct_to_vtd10(
            county_norm=str(county_norm),
            precinct_part=str(precinct_part),
            exact_map=exact_map,
            fuzzy_cands=fuzzy_cands,
            fuzzy_threshold=fuzzy_threshold,
            aliases=aliases,
        )

        if vtd10 and vtd10 in shares:
            if "alias" in method:
                stats["matched_alias"] += 1
            if method.startswith("exact"):
                stats["matched_exact"] += 1
            else:
                stats["matched_fuzzy"] += 1
            stats["matched_vtd10_votes"] += precinct_total
            dest_shares = shares[vtd10]
            # Allocate each candidate line independently so party/candidate totals conserve.
            for candidate, party_norm, votes in rows:
                alloc = _allocate_int_shares(votes, dest_shares)
                for geoid20, n in alloc.items():
                    _add_precinct_row_to_vtd20(
                        dest=dest_buckets,
                        geoid20=geoid20,
                        votes=n,
                        party_norm=party_norm,
                        candidate=candidate,
                    )
            continue

        if vtd10 and vtd10 not in shares:
            stats["vtd10_missing_shares"] += 1

        # Fallback: 1:1 onto a VTD20 geoid from the shortcut keymap/crosswalk path.
        geoid20 = ""
        if fallback_rekey:
            geoid20 = str(fallback_rekey.get(str(result_key), "")).strip()
        if geoid20:
            stats["fallback"] += 1
            stats["fallback_vtd20_votes"] += precinct_total
            for candidate, party_norm, votes in rows:
                _add_precinct_row_to_vtd20(
                    dest=dest_buckets,
                    geoid20=geoid20,
                    votes=votes,
                    party_norm=party_norm,
                    candidate=candidate,
                )
            continue

        stats["dropped"] += 1
        stats["dropped_votes"] += precinct_total

    results = {geoid: _finalize_bucket(bucket) for geoid, bucket in dest_buckets.items()}
    stats["output_votes"] = int(sum(int(r["total_votes"]) for r in results.values()))
    stats["output_vtd20"] = len(results)
    return results, stats


def build_fallback_rekey(
    *,
    results_keys: set[str],
    vtd20_geojson: Path,
    vtd20_join_prop: str,
    crosswalk_path: Path | None,
    supplemental_keymap_path: Path | None,
) -> dict[str, str]:
    """
    Map precinct result keys -> VTD20 GEOID20 using the same logic as
    build_contest_jsons._rekey_results_to_vtd20_geoid, but return the key map
    rather than rekeyed rows.
    """
    out: dict[str, str] = {}

    supplemental: dict[str, str] = {}
    if supplemental_keymap_path and supplemental_keymap_path.exists():
        raw = json.loads(supplemental_keymap_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for k, v in raw.items():
                key = str(k or "").strip()
                geoid = ""
                if isinstance(v, str):
                    geoid = v
                elif isinstance(v, dict):
                    geoid = str(v.get("vtd20_geoid") or v.get("to_vtd20_geoid") or "")
                geoid = str(geoid or "").strip()
                if key and geoid:
                    supplemental[key] = geoid

    crosswalk: dict[str, dict[str, object]] = {}
    if crosswalk_path and crosswalk_path.exists():
        loaded = json.loads(crosswalk_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            crosswalk = loaded  # type: ignore[assignment]

    props = bcj._load_geojson_props(vtd20_geojson)
    join_to_geoid: dict[str, str] = {}
    join_to_geoid_norm: dict[str, str] = {}
    join_to_geoid_code: dict[str, str] = {}
    valid: set[str] = set()
    for p in props:
        geoid20 = str(p.get("GEOID20") or "").strip()
        if not geoid20:
            continue
        valid.add(geoid20)
        for prop_name in (vtd20_join_prop, "join_key_name", "join_key_code"):
            jk = str(p.get(prop_name) or "").strip()
            if not jk:
                continue
            join_to_geoid.setdefault(jk, geoid20)
            join_to_geoid_norm.setdefault(bcj.normalize_join_key_loose(jk), geoid20)
            join_to_geoid_code.setdefault(bcj.normalize_join_key_code_loose(jk), geoid20)

    # Direct geometry join / crosswalk: result key equals a VTD20 join key, or
    # a VTD20 join key crosswalks to the result key.
    result_set = set(results_keys)
    result_norm = {bcj.normalize_join_key_loose(k): k for k in results_keys}
    result_code = {bcj.normalize_join_key_code_loose(k): k for k in results_keys}

    for jk, geoid20 in join_to_geoid.items():
        if jk in result_set:
            out.setdefault(jk, geoid20)
        n = bcj.normalize_join_key_loose(jk)
        if n in result_norm:
            out.setdefault(result_norm[n], geoid20)
        c = bcj.normalize_join_key_code_loose(jk)
        if c in result_code:
            out.setdefault(result_code[c], geoid20)

    for jk, payload in crosswalk.items():
        if not isinstance(payload, dict):
            continue
        to_key = str(payload.get("to") or "").strip()
        geoid20 = (
            join_to_geoid.get(jk)
            or join_to_geoid_norm.get(bcj.normalize_join_key_loose(jk))
            or join_to_geoid_code.get(bcj.normalize_join_key_code_loose(jk))
            or ""
        )
        if not geoid20 or not to_key:
            continue
        if to_key in result_set:
            out.setdefault(to_key, geoid20)
        n = bcj.normalize_join_key_loose(to_key)
        if n in result_norm:
            out.setdefault(result_norm[n], geoid20)
        c = bcj.normalize_join_key_code_loose(to_key)
        if c in result_code:
            out.setdefault(result_code[c], geoid20)

    for key in results_keys:
        if key in out:
            continue
        geoid = supplemental.get(key, "")
        if geoid and geoid in valid:
            out[key] = geoid

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, required=True, help="OpenElections precinct CSV")
    ap.add_argument("--year", default=None, help="Election year label (default: inferred from CSV name)")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output contests dir (default: Data/derived_vtd20_blockpath/<year>/contests)",
    )
    ap.add_argument("--vtd10-zip", type=Path, default=Path("Data/tl_2012_13_vtd10.zip"))
    ap.add_argument("--county-geojson", type=Path, default=Path("Data/tl_2020_13_county20.geojson"))
    ap.add_argument(
        "--vtd10-to-vtd20",
        type=Path,
        default=Path("Data/vtd10_to_vtd20_crosswalk.csv"),
        help="Full weight matrix (vtd10_geoid,vtd20_geoid,weight_sum)",
    )
    ap.add_argument("--vtd20-geojson", type=Path, default=Path("Data/tl_2020_13_vtd20.geojson"))
    ap.add_argument("--vtd20-join-prop", default="join_key_name")
    ap.add_argument(
        "--vtd20-crosswalk",
        type=Path,
        default=None,
        help="Optional year fuzzy crosswalk for unmatched-precinct fallback",
    )
    ap.add_argument(
        "--vtd20-supplemental-keymap",
        type=Path,
        default=Path("Data/vtd10_keys_to_vtd20_geoid.json"),
        help="Optional best-match keymap for unmatched-precinct fallback",
    )
    ap.add_argument("--fuzzy-threshold", type=float, default=0.85)
    ap.add_argument(
        "--aliases",
        type=Path,
        default=Path("Data/crosswalks/precinct_name_aliases.json"),
        help="Optional county-scoped precinct name aliases for VTD10 matching",
    )
    ap.add_argument("--no-fallback", action="store_true", help="Drop precincts that do not match VTD10")
    ap.add_argument("--only-office", default=None)
    ap.add_argument("--only-offices", default=None, help="Comma-separated offices (exact names)")
    ap.add_argument("--include-psc", action="store_true", help="Do not exclude Public Service Commissioner")
    ap.add_argument(
        "--write-match-report",
        type=Path,
        default=None,
        help="Optional path for precinct->VTD10 match diagnostics JSON",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    year = args.year
    if not year:
        m = re.match(r"^(\d{4})", args.csv.name)
        year = m.group(1) if m else "unknown"
    out_dir = args.out or Path(f"Data/derived_vtd20_blockpath/{year}/contests")

    for p in (args.csv, args.vtd10_zip, args.county_geojson, args.vtd10_to_vtd20):
        if not p.exists():
            raise SystemExit(f"Missing {p}")

    print("Building precinct -> VTD10 key map from VTD10 attributes...")
    exact_map, fuzzy_cands = build_precinct_key_to_vtd10(
        vtd10_zip=args.vtd10_zip,
        county_geojson=args.county_geojson,
    )
    print(f"  exact keys: {len(exact_map)}; counties with fuzzy cands: {len(fuzzy_cands)}")

    aliases = load_precinct_aliases(args.aliases if args.aliases and args.aliases.exists() else None)
    if aliases:
        print(f"Loaded precinct aliases for {len(aliases)} counties from {args.aliases}")

    print(f"Loading VTD10->VTD20 shares from {args.vtd10_to_vtd20}...")
    shares = load_vtd10_to_vtd20_shares(args.vtd10_to_vtd20)
    print(f"  VTD10 sources with shares: {len(shares)}")

    df = pd.read_csv(args.csv, dtype=str)
    votes_col = bcj._pick_votes_col(list(df.columns))
    df["county"] = df["county"].fillna("").astype(str)
    df["county_norm"] = df["county"].map(bcj.normalize_county_loose)
    df["office"] = df["office"].fillna("").astype(str).str.strip()
    df["office_norm"] = df["office"].map(bcj.normalize_office_loose)
    df["district_raw"] = df.get("district", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    df["party_norm"] = df.get("party", pd.Series([""] * len(df))).fillna("").astype(str).map(bcj.normalize_party)
    df["candidate"] = df["candidate"].fillna("").astype(str).map(bcj.normalize_candidate_case)
    if "precinct" not in df.columns:
        raise SystemExit("CSV missing 'precinct' column")
    df["precinct"] = df["precinct"].fillna("").astype(str)
    df["precinct_part"] = df["precinct"].map(bcj.extract_precinct_name)
    df = df[df["precinct_part"] != ""].copy()
    df["result_key"] = (
        (df["county_norm"] + " - " + df["precinct_part"]).str.replace(r"\s+", " ", regex=True).str.strip()
    )
    df["votes"] = pd.to_numeric(df[votes_col], errors="coerce").fillna(0).astype(int)

    if not args.include_psc:
        before = len(df)
        df = df[~df["office"].map(_is_psc_office)].copy()
        print(f"Excluded PSC rows: {before - len(df)}")

    contests = (
        df[["office", "office_norm", "district_raw"]]
        .drop_duplicates()
        .sort_values(["office", "district_raw"], kind="mergesort")
        .reset_index(drop=True)
    )
    offices_filter: set[str] = set()
    if args.only_office:
        offices_filter.add(args.only_office)
    if args.only_offices:
        offices_filter |= {x.strip() for x in args.only_offices.split(",") if x.strip()}
    if offices_filter:
        office_norms = {bcj.normalize_office_loose(x) for x in offices_filter}
        contests = contests[contests["office_norm"].isin(sorted(office_norms))].reset_index(drop=True)

    print(f"{args.csv}: {len(contests)} contests -> {out_dir}")

    fallback_rekey: dict[str, str] | None = None
    if not args.no_fallback:
        if not args.vtd20_geojson.exists():
            raise SystemExit(f"Fallback requires VTD20 geojson: {args.vtd20_geojson}")
        print("Building unmatched-precinct VTD20 fallback keymap...")
        fallback_rekey = build_fallback_rekey(
            results_keys=set(df["result_key"].unique()),
            vtd20_geojson=args.vtd20_geojson,
            vtd20_join_prop=args.vtd20_join_prop,
            crosswalk_path=args.vtd20_crosswalk if args.vtd20_crosswalk and args.vtd20_crosswalk.exists() else None,
            supplemental_keymap_path=(
                args.vtd20_supplemental_keymap
                if args.vtd20_supplemental_keymap and args.vtd20_supplemental_keymap.exists()
                else None
            ),
        )
        print(f"  fallback keys available: {len(fallback_rekey)}")

    # Optional match report on unique precincts (President-weighted if present).
    if args.write_match_report:
        probe = df.copy()
        if (probe["office"] == "President").any():
            probe = probe[probe["office"] == "President"]
        pt = probe.groupby(["result_key", "county_norm", "precinct_part"], as_index=False)["votes"].sum()
        rows = []
        for r in pt.itertuples(index=False):
            vtd10, method, score = match_precinct_to_vtd10(
                county_norm=str(r.county_norm),
                precinct_part=str(r.precinct_part),
                exact_map=exact_map,
                fuzzy_cands=fuzzy_cands,
                fuzzy_threshold=args.fuzzy_threshold,
                aliases=aliases,
            )
            fb = fallback_rekey.get(str(r.result_key), "") if fallback_rekey else ""
            rows.append(
                {
                    "result_key": r.result_key,
                    "votes": int(r.votes),
                    "vtd10_geoid": vtd10,
                    "method": method,
                    "score": score,
                    "fallback_vtd20": fb,
                    "has_shares": bool(vtd10 and vtd10 in shares),
                }
            )
        report = {
            "year": year,
            "csv": str(args.csv),
            "fuzzy_threshold": args.fuzzy_threshold,
            "precincts": len(rows),
            "matched_vtd10_votes": sum(r["votes"] for r in rows if r["vtd10_geoid"] and r["has_shares"]),
            "fallback_only_votes": sum(
                r["votes"] for r in rows if (not r["vtd10_geoid"] or not r["has_shares"]) and r["fallback_vtd20"]
            ),
            "dropped_votes": sum(
                r["votes"] for r in rows if (not r["vtd10_geoid"] or not r["has_shares"]) and not r["fallback_vtd20"]
            ),
            "rows": rows,
        }
        args.write_match_report.parent.mkdir(parents=True, exist_ok=True)
        args.write_match_report.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"Wrote match report {args.write_match_report}")

    manifest: dict[str, object] = {
        "csv": str(args.csv),
        "year": year,
        "method": "vtd10_weighted_shares",
        "vtd10_to_vtd20": str(args.vtd10_to_vtd20),
        "fuzzy_threshold": args.fuzzy_threshold,
        "fallback": not args.no_fallback,
        "levels": ["vtd20"],
        "contests": [],
        "coverage": [],
    }

    level_dir = out_dir / "vtd20"
    if not args.dry_run:
        level_dir.mkdir(parents=True, exist_ok=True)

    for _, crow in contests.iterrows():
        office = str(crow["office"])
        district_raw = str(crow["district_raw"])
        district_for_slug = district_raw
        if office in {"U.S. House", "State House", "State Senate"} and district_raw:
            district_for_slug = bcj.normalize_district_for_join(office, district_raw)
        contest_slug = bcj.slugify(office) + ("__" + bcj.slugify(district_for_slug) if district_for_slug else "")

        sub = df[(df["office"] == office) & (df["district_raw"] == district_raw)][
            ["county_norm", "precinct_part", "result_key", "candidate", "party_norm", "votes"]
        ].copy()
        if sub.empty:
            continue

        results, stats = reallocate_contest(
            sub=sub,
            exact_map=exact_map,
            fuzzy_cands=fuzzy_cands,
            shares=shares,
            fuzzy_threshold=args.fuzzy_threshold,
            fallback_rekey=None if args.no_fallback else fallback_rekey,
            aliases=aliases,
        )
        print(
            f"- {office}|{district_raw} -> {contest_slug}: "
            f"src={stats['source_votes']} out={stats['output_votes']} "
            f"vtd10={stats['matched_vtd10_votes']} fb={stats['fallback_vtd20_votes']} "
            f"drop={stats['dropped_votes']} n_vtd20={stats['output_vtd20']}"
        )

        contest_entry = {
            "office": office,
            "district": district_raw,
            "slug": contest_slug,
            "outputs": {
                "vtd20": {"path": str(level_dir / f"{contest_slug}.json"), "rows": stats["output_vtd20"]}
            },
            "coverage": stats,
        }
        manifest["contests"].append(contest_entry)
        manifest["coverage"].append({"slug": contest_slug, **stats})

        if args.dry_run:
            continue

        payload = {
            "office": office,
            "district": district_raw,
            "level": "vtd20",
            "keys": (
                "Join on VTD20 GEOID20. Built by precinct->VTD10 match + "
                "vtd10_to_vtd20_crosswalk.csv share reallocation"
                + ("; unmatched precincts fall back to name/keymap VTD20" if not args.no_fallback else "")
                + "."
            ),
            "method": "vtd10_weighted_shares",
            "results": results,
        }
        (level_dir / f"{contest_slug}.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Wrote {out_dir / 'manifest.json'}")


if __name__ == "__main__":
    main()
