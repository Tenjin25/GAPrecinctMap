"""
Build per-contest JSON "result maps" (no geometry) from OpenElections-style CSVs.

This is useful when you already have county/VTD/precinct geometries (shapefiles/GeoJSON)
and just want contest results keyed for a client-side join.

Input CSV expected columns:
  county, office, candidate, party, district (optional), precinct (optional), votes/total_votes

Outputs:
  <out_dir>/<level>/<contest_slug>.json

Each output JSON looks like:
  {
    "office": "...",
    "district": "...",
    "level": "county|vtd|congressional|state_house|state_senate",
    "keys": "how to join",
    "results": {
      "<join_key>": { "total_votes":..., "dem_votes":..., "rep_votes":..., "other_votes":..., ... }
    }
  }

Usage:
  py scripts/build_contest_jsons.py --csv Data/20221108__ga__general__precinct.csv --out Data/derived_json
  py scripts/build_contest_jsons.py --csv Data/20221108__ga__general__county.csv --out Data/derived_json --levels county
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

Level = Literal["county", "vtd", "vtd20", "congressional", "state_house", "state_senate"]


def _pick_votes_col(columns: list[str]) -> str:
    cols = {c.strip(): c for c in columns}
    if "total_votes" in cols:
        return cols["total_votes"]
    if "votes" in cols:
        return cols["votes"]
    raise SystemExit("CSV must contain a 'votes' or 'total_votes' column.")


def normalize_county_loose(name: str) -> str:
    # Mirrors the normalization patterns used in index.html (allowing dots and hyphens).
    base = (name or "").replace("\u00a0", " ").strip().upper()
    base = re.sub(r"[^A-Z0-9 .\-]", "", base)
    return re.sub(r"\s+", " ", base).strip()


def normalize_party(party: str) -> str:
    p = (party or "").strip().upper()
    if not p:
        return "UNK"
    if p.startswith("DEM") or "DEMOCRAT" in p:
        return "DEM"
    if p.startswith("REP") or "REPUBLICAN" in p:
        return "REP"
    return "OTH"


def _safe_int_str(value: str) -> int | None:
    s = (value or "").strip()
    if not s:
        return None
    try:
        f = float(s)
        if f.is_integer():
            return int(f)
    except ValueError:
        pass
    m = re.match(r"^\s*(\d+)\s*$", s)
    return int(m.group(1)) if m else None


def normalize_district_for_join(office: str, district: str) -> str:
    o = (office or "").strip().upper()
    d = (district or "").strip()
    n = _safe_int_str(d)
    if n is None:
        return d
    if o == "U.S. HOUSE":
        return f"{n:02d}"
    if o in {"STATE HOUSE", "STATE SENATE"}:
        return f"{n:03d}"
    return str(n)


def extract_precinct_code(precinct_raw: str) -> str:
    # Matches index.html's extractPrecinctCode().
    if precinct_raw is None:
        return ""
    p = str(precinct_raw).strip()
    if not p or p.lower() == "nan":
        return ""
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
        return u
    return re.split(r"[_\s]+", u, maxsplit=1)[0].strip()


def normalize_name_loose(name: str) -> str:
    base = (name or "").replace("\u00a0", " ").strip().upper()
    base = re.sub(r"[^A-Z0-9 .\-]", "", base)
    return re.sub(r"\s+", " ", base).strip()


def normalize_precinct_code_loose(code_raw: str) -> str:
    code = (code_raw or "").strip().upper()
    if not code:
        return ""
    code = re.sub(r"\s+", "", code)

    def _strip_zeros(m: re.Match[str]) -> str:
        return str(int(m.group(0)))

    return re.sub(r"\d+", _strip_zeros, code)


def normalize_join_key_loose(join_key: str) -> str:
    base = (join_key or "").strip().upper()
    return re.sub(r"\s+", " ", base).strip()


def normalize_join_key_code_loose(join_key: str) -> str:
    raw = normalize_join_key_loose(join_key)
    if not raw:
        return ""
    m = re.match(r"^(.*?)\s*-\s*(.+)$", raw)
    if not m:
        return raw
    county = normalize_county_loose(m.group(1))
    code = normalize_precinct_code_loose(m.group(2))
    if not county or not code:
        return raw
    return f"{county} - {code}"


def extract_precinct_name(precinct_raw: str) -> str:
    if precinct_raw is None:
        return ""
    p = str(precinct_raw).strip()
    if not p or p.lower() == "nan":
        return ""
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

    # Try to strip a leading "code" token (e.g. "01_PATTERSON" -> "PATTERSON").
    parts = re.split(r"[_\s]+", u)
    parts = [x for x in parts if x]
    if not parts:
        return ""

    first = parts[0]
    looks_like_code = bool(
        re.fullmatch(r"\d{1,3}[A-Z]?", first)  # 1, 01, 2A, 123
        or re.fullmatch(r"[A-Z]{1,4}-\d{1,3}", first)  # LI-1
        or re.fullmatch(r"\d{2}[A-Z]{2,4}", first)  # 00TOCC style codes
    )

    remainder = parts[1:] if (looks_like_code and len(parts) > 1) else parts
    return normalize_name_loose(" ".join(remainder))


def slugify(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\s\-\.]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("._-") or "contest"

def normalize_office_loose(office: str) -> str:
    s = (office or "").replace("\u00a0", " ").strip().upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Normalize common OpenElections variants: "Secretary Of State" vs "Secretary of State"
    s = s.replace(" OF ", " ")
    return s


def _levels_from_csv(levels: Iterable[str]) -> list[Level]:
    out: list[Level] = []
    for raw in levels:
        s = raw.strip().lower()
        if not s:
            continue
        if s not in {"county", "vtd", "vtd20", "congressional", "state_house", "state_senate"}:
            raise SystemExit(f"Unknown level: {raw}")
        out.append(s)  # type: ignore[arg-type]
    return out


def _aggregate_contest(df: pd.DataFrame, *, level: Level, office: str, district_raw: str) -> pd.DataFrame:
    sub = df[(df["office"] == office) & (df["district_raw"] == district_raw)].copy()

    if level == "county":
        sub["_key"] = sub["county_norm"]
    elif level == "vtd":
        if "precinct" not in sub.columns:
            raise SystemExit("VTD/precinct-level join requested but CSV is missing 'precinct' column.")
        join_mode = sub.attrs.get("_vtd_join", "name")
        if join_mode == "code":
            sub["_prec_part"] = sub["precinct"].map(extract_precinct_code)
        else:
            sub["_prec_part"] = sub["precinct"].map(extract_precinct_name)
        sub = sub[sub["_prec_part"] != ""]
        sub["_key"] = (sub["county_norm"] + " - " + sub["_prec_part"]).str.replace(r"\s+", " ", regex=True).str.strip()
    elif level == "vtd20":
        # Build results on the same join key used for 'vtd' (COUNTY_NORM - PRECINCT_NAME_NORM),
        # then re-key onto VTD20 GEOID20 using the VTD20 geometry's join keys (optionally with a crosswalk).
        if "precinct" not in sub.columns:
            raise SystemExit("VTD20 join requested but CSV is missing 'precinct' column.")
        sub["_prec_part"] = sub["precinct"].map(extract_precinct_name)
        sub = sub[sub["_prec_part"] != ""]
        sub["_key"] = (sub["county_norm"] + " - " + sub["_prec_part"]).str.replace(r"\s+", " ", regex=True).str.strip()
    else:
        # district join key
        sub["_key"] = sub["district_join"]
        sub = sub[sub["_key"].astype(str).str.strip() != ""]

    if sub.empty:
        return pd.DataFrame(columns=["_key"])

    totals = sub.groupby("_key", dropna=False)["votes"].sum().rename("total_votes").to_frame()
    by_party = (
        sub.groupby(["_key", "party_norm"], dropna=False)["votes"]
        .sum()
        .unstack(fill_value=0)
    )
    if "DEM" not in by_party.columns:
        by_party["DEM"] = 0
    if "REP" not in by_party.columns:
        by_party["REP"] = 0
    by_party = by_party.rename(columns={"DEM": "dem_votes", "REP": "rep_votes"})

    cand_tot = (
        sub.groupby(["_key", "candidate"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values(["_key", "votes", "candidate"], ascending=[True, False, True], kind="mergesort")
    )
    cand_tot["_pos"] = cand_tot.groupby("_key", sort=False).cumcount()
    winner = (
        cand_tot[cand_tot["_pos"] == 0]
        .set_index("_key")[["candidate", "votes"]]
        .rename(columns={"candidate": "winner_candidate", "votes": "winner_votes"})
    )
    runner = (
        cand_tot[cand_tot["_pos"] == 1]
        .set_index("_key")[["votes"]]
        .rename(columns={"votes": "runnerup_votes"})
    )

    out = totals.join(by_party, how="left").join(winner, how="left")
    out = out.join(runner, how="left")
    if "runnerup_votes" not in out.columns:
        out["runnerup_votes"] = 0

    out["dem_votes"] = out["dem_votes"].fillna(0).astype(int)
    out["rep_votes"] = out["rep_votes"].fillna(0).astype(int)
    out["total_votes"] = out["total_votes"].fillna(0).astype(int)
    out["other_votes"] = (out["total_votes"] - out["dem_votes"] - out["rep_votes"]).astype(int)
    out["winner_votes"] = out["winner_votes"].fillna(0).astype(int)
    out["runnerup_votes"] = out["runnerup_votes"].fillna(0).astype(int)
    out["margin_votes"] = (out["winner_votes"] - out["runnerup_votes"]).astype(int)
    out["winner_candidate"] = out["winner_candidate"].fillna("")

    cand_party = (
        sub.groupby(["candidate", "party_norm"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values(["candidate", "votes", "party_norm"], ascending=[True, False, True], kind="mergesort")
        .groupby("candidate", sort=False)
        .nth(0)["party_norm"]
    )
    out["winner_party"] = out["winner_candidate"].map(cand_party).fillna("UNK")

    return out.reset_index().rename(columns={"index": "_key"})


def _load_geojson_props(path: Path) -> list[dict[str, object]]:
    g = json.loads(path.read_text(encoding="utf-8"))
    feats = g.get("features") or []
    out: list[dict[str, object]] = []
    for f in feats:
        props = (f or {}).get("properties") or {}
        if isinstance(props, dict):
            out.append(props)
    return out


def _rekey_results_to_vtd20_geoid(
    *,
    results_by_key: dict[str, dict[str, object]],
    vtd20_geojson: Path,
    vtd20_join_prop: str,
    crosswalk_path: Path | None,
    supplemental_keymap_path: Path | None,
) -> dict[str, dict[str, object]]:
    props = _load_geojson_props(vtd20_geojson)
    crosswalk: dict[str, dict[str, object]] = {}
    if crosswalk_path and crosswalk_path.exists():
        crosswalk = json.loads(crosswalk_path.read_text(encoding="utf-8"))
    supplemental_keymap: dict[str, str] = {}
    if supplemental_keymap_path and supplemental_keymap_path.exists():
        raw = json.loads(supplemental_keymap_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for k, v in raw.items():
                key = str(k or "").strip()
                if not key:
                    continue
                geoid = ""
                if isinstance(v, str):
                    geoid = v
                elif isinstance(v, dict):
                    geoid = str(v.get("vtd20_geoid") or v.get("to_vtd20_geoid") or "")
                geoid = str(geoid or "").strip()
                if geoid:
                    supplemental_keymap[key] = geoid

    results_by_key_norm: dict[str, dict[str, object]] = {}
    results_by_key_code_norm: dict[str, dict[str, object]] = {}
    for raw_key, row in results_by_key.items():
        k_norm = normalize_join_key_loose(str(raw_key))
        if k_norm and k_norm not in results_by_key_norm:
            results_by_key_norm[k_norm] = row
        k_code_norm = normalize_join_key_code_loose(str(raw_key))
        if k_code_norm and k_code_norm not in results_by_key_code_norm:
            results_by_key_code_norm[k_code_norm] = row

    crosswalk_norm: dict[str, dict[str, object]] = {}
    crosswalk_code_norm: dict[str, dict[str, object]] = {}
    if isinstance(crosswalk, dict):
        for raw_key, payload in crosswalk.items():
            if not isinstance(payload, dict):
                continue
            k_norm = normalize_join_key_loose(str(raw_key))
            if k_norm and k_norm not in crosswalk_norm:
                crosswalk_norm[k_norm] = payload
            k_code_norm = normalize_join_key_code_loose(str(raw_key))
            if k_code_norm and k_code_norm not in crosswalk_code_norm:
                crosswalk_code_norm[k_code_norm] = payload

    out: dict[str, dict[str, object]] = {}
    valid_geoids: set[str] = set()
    for p in props:
        geoid20 = str(p.get("GEOID20") or "").strip()
        if not geoid20:
            continue
        valid_geoids.add(geoid20)
        candidate_keys: list[str] = []
        for prop_name in (vtd20_join_prop, "join_key_name", "join_key_code"):
            key = str(p.get(prop_name) or "").strip()
            if key and key not in candidate_keys:
                candidate_keys.append(key)
        if not candidate_keys:
            continue

        row = None
        for key in candidate_keys:
            row = results_by_key.get(key)
            if row is None:
                row = results_by_key_norm.get(normalize_join_key_loose(key))
            if row is None:
                row = results_by_key_code_norm.get(normalize_join_key_code_loose(key))
            if row is not None:
                break

        if row is None:
            for key in candidate_keys:
                m = None
                if isinstance(crosswalk, dict):
                    m = crosswalk.get(key)
                if not isinstance(m, dict):
                    m = crosswalk_norm.get(normalize_join_key_loose(key))
                if not isinstance(m, dict):
                    m = crosswalk_code_norm.get(normalize_join_key_code_loose(key))
                to_key = str(m.get("to") or "").strip() if isinstance(m, dict) else ""
                if not to_key:
                    continue
                row = results_by_key.get(to_key)
                if row is None:
                    row = results_by_key_norm.get(normalize_join_key_loose(to_key))
                if row is None:
                    row = results_by_key_code_norm.get(normalize_join_key_code_loose(to_key))
                if row is not None:
                    break

        if row is not None:
            out[geoid20] = row

    # Optional fallback: map result keys directly to VTD20 GEOID20 (e.g., via VTD10->VTD20 bridge).
    if supplemental_keymap:
        for result_key, row in results_by_key.items():
            geoid20 = str(supplemental_keymap.get(str(result_key), "")).strip()
            if not geoid20 or geoid20 not in valid_geoids or geoid20 in out:
                continue
            out[geoid20] = row
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, type=Path, help="Input CSV path")
    ap.add_argument("--out", required=True, type=Path, help="Output base dir")
    ap.add_argument("--levels", default="county,vtd,vtd20,congressional,state_house,state_senate")
    ap.add_argument(
        "--vtd-join",
        choices=["name", "code"],
        default="name",
        help="For --levels vtd: join key uses precinct name (default) or precinct code",
    )
    ap.add_argument(
        "--vtd20-geojson",
        type=Path,
        default=Path("Data/tl_2020_13_vtd20.geojson"),
        help="VTD20 GeoJSON with GEOID20 and a join-key property (produced by scripts/prepare_tiger_2020_geojsons.py)",
    )
    ap.add_argument(
        "--vtd20-join-prop",
        default="join_key_name",
        help="Property in VTD20 GeoJSON to match against precinct-derived join keys (default: join_key_name)",
    )
    ap.add_argument(
        "--vtd20-crosswalk",
        type=Path,
        default=Path("Data/vtd20_crosswalk_2020.json"),
        help="Optional JSON mapping from VTD20 join_key -> results join_key (built by scripts/build_vtd20_crosswalk.py)",
    )
    ap.add_argument(
        "--vtd20-supplemental-keymap",
        type=Path,
        default=None,
        help="Optional JSON mapping from result key '<COUNTY_NORM> - <PRECINCT_PART>' -> VTD20 GEOID20.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Do not write files; just report what would be built")
    ap.add_argument("--only-office", default=None, help="Only build contests for this office (exact match)")
    ap.add_argument(
        "--only-offices",
        default=None,
        help="Comma-separated list of offices to build (exact match). Example: 'President,U.S. Senate,Governor'",
    )
    ap.add_argument("--only-contest", default=None, help="Only build one contest, formatted as 'office|district'")
    args = ap.parse_args()

    levels = _levels_from_csv(args.levels.split(","))

    df = pd.read_csv(args.csv, dtype=str)
    votes_col = _pick_votes_col(list(df.columns))
    required = {"county", "office", "candidate", votes_col}
    missing = required - set(df.columns)
    if missing:
        raise SystemExit(f"Missing required columns: {sorted(missing)}")

    df["county"] = df["county"].fillna("").astype(str)
    df["county_norm"] = df["county"].map(normalize_county_loose)
    df["office"] = df["office"].fillna("").astype(str).str.strip()
    df["office_norm"] = df["office"].map(normalize_office_loose)
    df["district_raw"] = df.get("district", "").fillna("").astype(str).str.strip()
    df["district_join"] = [
        normalize_district_for_join(o, d) for o, d in zip(df["office"].tolist(), df["district_raw"].tolist())
    ]
    df["party_norm"] = df.get("party", "").fillna("").astype(str).map(normalize_party)
    df["candidate"] = df["candidate"].fillna("").astype(str).str.strip()
    if "precinct" in df.columns:
        df["precinct"] = df["precinct"].fillna("").astype(str)
    df["votes"] = pd.to_numeric(df[votes_col], errors="coerce").fillna(0).astype(int)
    df.attrs["_vtd_join"] = args.vtd_join

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
        office_norms = {normalize_office_loose(x) for x in offices_filter if x.strip()}
        contests = contests[contests["office_norm"].isin(sorted(office_norms))].reset_index(drop=True)
    if args.only_contest:
        if "|" not in args.only_contest:
            raise SystemExit("--only-contest must look like 'office|district' (district may be empty)")
        o, d = args.only_contest.split("|", 1)
        contests = contests[(contests["office"] == o) & (contests["district_raw"] == d)].reset_index(drop=True)

    print(f"{args.csv}: {len(contests)} contests")

    manifest: dict[str, object] = {"csv": str(args.csv), "levels": levels, "contests": []}

    join_hints: dict[Level, str] = {
        "county": "Join on normalized county name (uppercased; punctuation stripped except '.' and '-').",
        "vtd": "Join on '<COUNTY_NORM> - <PRECINCT_NAME_NORM>' (default) or '<COUNTY_NORM> - <PRECINCT_CODE>' if --vtd-join code.",
        "vtd20": "Join on VTD20 GEOID20 (state+county+vtd id). Built by matching precinct names to VTD20 join keys; optionally uses --vtd20-crosswalk and --vtd20-supplemental-keymap.",
        "congressional": "Join on 2-digit district code (CD118FP-style). Only built for office 'U.S. House'.",
        "state_house": "Join on 3-digit district code (SLDLST-style). Only built for office 'State House'.",
        "state_senate": "Join on 3-digit district code (SLDUST-style). Only built for office 'State Senate'.",
    }

    for _, r in contests.iterrows():
        office = r["office"]
        district_raw = r["district_raw"]

        district_for_slug = district_raw
        if office in {"U.S. House", "State House", "State Senate"} and district_raw:
            district_for_slug = normalize_district_for_join(office, district_raw)
        contest_slug = slugify(office) + ("__" + slugify(district_for_slug) if district_for_slug else "")

        print(f"- {office}|{district_raw} -> {contest_slug}")
        contest_entry: dict[str, object] = {"office": office, "district": district_raw, "slug": contest_slug, "outputs": {}}

        for level in levels:
            if level == "congressional" and office != "U.S. House":
                continue
            if level == "state_house" and office != "State House":
                continue
            if level == "state_senate" and office != "State Senate":
                continue

            agg = _aggregate_contest(df, level=level, office=office, district_raw=district_raw)
            if agg.empty:
                continue

            out_path = args.out / level / f"{contest_slug}.json"
            contest_entry["outputs"][level] = {"path": str(out_path), "rows": int(len(agg))}
            if args.dry_run:
                continue

            out_path.parent.mkdir(parents=True, exist_ok=True)
            idx = agg.set_index("_key")
            results_map: dict[str, dict[str, object]] = {}
            for key, row in idx.iterrows():
                results_map[str(key)] = {
                    "total_votes": int(row["total_votes"]),
                    "dem_votes": int(row["dem_votes"]),
                    "rep_votes": int(row["rep_votes"]),
                    "other_votes": int(row["other_votes"]),
                    "winner_candidate": str(row["winner_candidate"]),
                    "winner_party": str(row["winner_party"]),
                    "winner_votes": int(row["winner_votes"]),
                    "margin_votes": int(row["margin_votes"]),
                }

            if level == "vtd20":
                if not args.vtd20_geojson.exists():
                    raise SystemExit(f"Missing VTD20 GeoJSON: {args.vtd20_geojson}")
                results_map = _rekey_results_to_vtd20_geoid(
                    results_by_key=results_map,
                    vtd20_geojson=args.vtd20_geojson,
                    vtd20_join_prop=args.vtd20_join_prop,
                    crosswalk_path=args.vtd20_crosswalk if args.vtd20_crosswalk and args.vtd20_crosswalk.exists() else None,
                    supplemental_keymap_path=(
                        args.vtd20_supplemental_keymap
                        if args.vtd20_supplemental_keymap and args.vtd20_supplemental_keymap.exists()
                        else None
                    ),
                )

            payload = {
                "office": office,
                "district": district_raw,
                "level": level,
                "keys": join_hints[level],
                "results": results_map,
            }
            out_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        manifest["contests"].append(contest_entry)

    if not args.dry_run:
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Wrote {args.out / 'manifest.json'}")


if __name__ == "__main__":
    main()
