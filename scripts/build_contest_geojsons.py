"""
Build per-contest GeoJSONs by geography level from OpenElections-style CSVs.

This script is designed for this repo's data layout:
  - Election results: Data/*__ga__general__precinct.csv, Data/*__ga__general__county.csv
  - Boundaries (GeoJSON): Data/tl_2022_13_cd118.geojson, Data/tl_2022_13_sldl.geojson, Data/tl_2022_13_sldu.geojson
  - Optional: Data/tl_2022_13_county.geojson, Data/Voting_Precincts.geojson

Outputs:
  Writes one GeoJSON per contest per requested level under:
    <out_dir>/<level>/<contest_slug>.geojson

Feature properties written (all levels):
  - office, district
  - total_votes, dem_votes, rep_votes, other_votes
  - winner_candidate, winner_party, winner_votes, margin_votes

Usage (dry-run):
  py scripts/build_contest_geojsons.py --csv Data/20221108__ga__general__precinct.csv --out Data/derived --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd


Level = Literal["county", "congressional", "state_house", "state_senate", "precinct"]


def _pick_votes_col(columns: list[str]) -> str:
    cols = {c.strip(): c for c in columns}
    if "total_votes" in cols:
        return cols["total_votes"]
    if "votes" in cols:
        return cols["votes"]
    raise SystemExit("CSV must contain a 'votes' or 'total_votes' column.")


def normalize_county(name: str) -> str:
    return (
        (name or "")
        .replace("\u00a0", " ")
        .strip()
        .upper()
    )


def normalize_county_loose(name: str) -> str:
    # Mirrors the normalization patterns used in index.html (allowing dots and hyphens).
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 .\-]", "", normalize_county(name))).strip()


def normalize_party(party: str) -> str:
    p = (party or "").strip().upper()
    if not p:
        return "UNK"
    if p.startswith("DEM") or "DEMOCRAT" in p:
        return "DEM"
    if p.startswith("REP") or "REPUBLICAN" in p:
        return "REP"
    if p.startswith("LIB") or "LIBERTARIAN" in p:
        return "LIB"
    if p.startswith("GRN") or "GREEN" in p:
        return "GRN"
    return "OTH"


def _safe_int_str(value: str) -> int | None:
    s = (value or "").strip()
    if not s:
        return None
    # Handles "123", "123.0", etc.
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
    if o == "STATE HOUSE":
        return f"{n:03d}"
    if o == "STATE SENATE":
        return f"{n:03d}"
    return str(n)


def extract_precinct_code(precinct_raw: str) -> str:
    # Mirrors index.html's extractPrecinctCode() for OpenElections precinct strings.
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
        return u
    return re.split(r"[_\s]+", u, maxsplit=1)[0].strip()


def slugify(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\s\-\.]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("._-") or "contest"


@dataclass(frozen=True)
class Boundaries:
    level: Level
    path: Path
    join_field: str


def _read_boundaries(b: Boundaries):
    try:
        import geopandas as gpd
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"geopandas is required to write GeoJSON outputs: {e}")

    gdf = gpd.read_file(b.path)
    if b.join_field not in gdf.columns:
        raise SystemExit(f"{b.level} boundaries missing join field '{b.join_field}' in {b.path}")
    gdf["_join_key"] = gdf[b.join_field].astype(str).str.strip()
    return gdf


def _infer_county_join_field(gdf) -> str:
    # Prefer newer TIGER fields, then common alternates.
    candidates = [
        "NAME20",
        "NAME",
        "County",
        "COUNTYNAME",
        "county_name",
        "name",
    ]
    cols_upper = {c.upper(): c for c in gdf.columns}
    for c in candidates:
        if c in gdf.columns:
            return c
        if c.upper() in cols_upper:
            return cols_upper[c.upper()]
    raise SystemExit("Could not infer county-name field from county GeoJSON properties.")


def _infer_precinct_fields(gdf) -> tuple[str, str]:
    # Returns (county_field, precinct_field) if possible.
    cols = list(gdf.columns)
    upper = {c.upper(): c for c in cols}

    county_candidates = [
        "COUNTY",
        "COUNTY_NAM",
        "COUNTYNAME",
        "COUNTY_NAME",
        "NAME20",  # sometimes counties-only; included as fallback
    ]
    precinct_candidates = [
        "PREC_ID",
        "PRECINCT",
        "PRECINCTID",
        "PRECINCT_ID",
        "PCT",
        "CODE",
    ]

    county_field = None
    for c in county_candidates:
        if c in upper:
            county_field = upper[c]
            break
    precinct_field = None
    for c in precinct_candidates:
        if c in upper:
            precinct_field = upper[c]
            break
    if not county_field or not precinct_field:
        raise SystemExit(
            "Could not infer precinct join fields. Provide --precinct-county-field and --precinct-code-field."
        )
    return county_field, precinct_field


def _build_results(df: pd.DataFrame, *, level: Level, office: str, district: str) -> pd.DataFrame:
    sub = df[(df["office"] == office) & (df["district_raw"] == district)].copy()

    if level == "county":
        sub["_geo_key"] = sub["county_norm"]
    elif level == "precinct":
        sub = sub[sub["precinct_code"] != ""]
        sub["_geo_key"] = (sub["county_norm"] + " - " + sub["precinct_code"]).str.replace(r"\s+", " ", regex=True).str.strip()
    else:
        # district-based levels
        sub = sub[sub["district_join"] != ""]
        sub["_geo_key"] = sub["district_join"]

    if sub.empty:
        return pd.DataFrame(columns=["_geo_key"])

    by_geo_party = (
        sub.groupby(["_geo_key", "party_norm"], dropna=False)["votes"]
        .sum()
        .unstack(fill_value=0)
    )
    for c in ["DEM", "REP"]:
        if c not in by_geo_party.columns:
            by_geo_party[c] = 0
    by_geo_party = by_geo_party.rename(columns={"DEM": "dem_votes", "REP": "rep_votes"})

    by_geo_total = sub.groupby(["_geo_key"], dropna=False)["votes"].sum().rename("total_votes").to_frame()

    cand_tot = (
        sub.groupby(["_geo_key", "candidate"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values(["_geo_key", "votes", "candidate"], ascending=[True, False, True], kind="mergesort")
    )
    # Winner / runner-up per geo (cand_tot is pre-sorted by votes desc within each geo).
    cand_tot["_pos"] = cand_tot.groupby("_geo_key", sort=False).cumcount()
    winner = (
        cand_tot[cand_tot["_pos"] == 0]
        .set_index("_geo_key")[["candidate", "votes"]]
        .rename(columns={"candidate": "winner_candidate", "votes": "winner_votes"})
    )
    runner = (
        cand_tot[cand_tot["_pos"] == 1]
        .set_index("_geo_key")[["votes"]]
        .rename(columns={"votes": "runnerup_votes"})
    )
    out = by_geo_total.join(by_geo_party, how="left")
    out = out.join(winner, how="left")
    out = out.join(runner, how="left")
    if "runnerup_votes" not in out.columns:
        out["runnerup_votes"] = 0
    out["runnerup_votes"] = out["runnerup_votes"].fillna(0).astype(int)
    out["margin_votes"] = (out["winner_votes"].fillna(0).astype(int) - out["runnerup_votes"]).astype(int)

    # Winner party (pick the party with highest votes for that candidate).
    cand_party = (
        sub.groupby(["candidate", "party_norm"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values(["candidate", "votes", "party_norm"], ascending=[True, False, True], kind="mergesort")
        .groupby("candidate", sort=False)
        .nth(0)["party_norm"]
    )
    out["winner_party"] = out["winner_candidate"].map(cand_party)

    out["dem_votes"] = out["dem_votes"].fillna(0).astype(int)
    out["rep_votes"] = out["rep_votes"].fillna(0).astype(int)
    out["total_votes"] = out["total_votes"].fillna(0).astype(int)
    out["other_votes"] = (out["total_votes"] - out["dem_votes"] - out["rep_votes"]).astype(int)
    out["winner_votes"] = out["winner_votes"].fillna(0).astype(int)
    out["winner_candidate"] = out["winner_candidate"].fillna("")
    out["winner_party"] = out["winner_party"].fillna("UNK")

    return out.reset_index().rename(columns={"index": "_geo_key"})


def _levels_from_csv(levels: Iterable[str]) -> list[Level]:
    out: list[Level] = []
    for raw in levels:
        s = raw.strip().lower()
        if not s:
            continue
        if s not in {"county", "congressional", "state_house", "state_senate", "precinct"}:
            raise SystemExit(f"Unknown level: {raw}")
        out.append(s)  # type: ignore[arg-type]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, type=Path, help="Input CSV path")
    ap.add_argument("--out", required=True, type=Path, help="Output base dir (created if missing)")
    ap.add_argument("--levels", default="county,congressional,state_house,state_senate,precinct")
    ap.add_argument("--dry-run", action="store_true", help="Do not write files; just report what would be built")

    ap.add_argument("--cd-geojson", type=Path, default=Path("Data/tl_2022_13_cd118.geojson"))
    ap.add_argument("--sldl-geojson", type=Path, default=Path("Data/tl_2022_13_sldl.geojson"))
    ap.add_argument("--sldu-geojson", type=Path, default=Path("Data/tl_2022_13_sldu.geojson"))
    ap.add_argument("--county-geojson", type=Path, default=Path("Data/tl_2022_13_county.geojson"))
    ap.add_argument("--precinct-geojson", type=Path, default=Path("Data/Voting_Precincts.geojson"))
    ap.add_argument("--precinct-county-field", default=None, help="County name field in precinct GeoJSON properties")
    ap.add_argument("--precinct-code-field", default=None, help="Precinct code/id field in precinct GeoJSON properties")

    ap.add_argument("--only-office", default=None, help="Only build contests for this office (exact match)")
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
    df["office"] = df["office"].fillna("").astype(str).str.strip()
    df["district_raw"] = df.get("district", "").fillna("").astype(str).str.strip()
    df["district_join"] = [
        normalize_district_for_join(o, d) for o, d in zip(df["office"].tolist(), df["district_raw"].tolist())
    ]
    df["party_norm"] = [normalize_party(p) for p in df.get("party", "").fillna("").astype(str).tolist()]
    df["candidate"] = df["candidate"].fillna("").astype(str).str.strip()
    df["county_norm"] = [normalize_county_loose(c) for c in df["county"].tolist()]
    df["precinct_raw"] = df.get("precinct", "").fillna("").astype(str)
    df["precinct_code"] = [extract_precinct_code(p) for p in df["precinct_raw"].tolist()]
    df["votes"] = pd.to_numeric(df[votes_col], errors="coerce").fillna(0).astype(int)

    contests = df[["office", "district_raw"]].drop_duplicates().sort_values(["office", "district_raw"], kind="mergesort")
    if args.only_office:
        contests = contests[contests["office"] == args.only_office]

    if args.only_contest:
        if "|" not in args.only_contest:
            raise SystemExit("--only-contest must look like 'office|district' (district may be empty)")
        office, district = args.only_contest.split("|", 1)
        contests = contests[(contests["office"] == office) & (contests["district_raw"] == district)]

    contests = contests.reset_index(drop=True)
    print(f"{args.csv}: {len(contests)} contests")

    boundary_cache: dict[Level, object] = {}

    def load_boundaries_for_level(level: Level):
        if level in boundary_cache:
            return boundary_cache[level]
        if level == "congressional":
            if not args.cd_geojson.exists():
                raise SystemExit(f"Missing congressional boundaries: {args.cd_geojson}")
            b = Boundaries(level=level, path=args.cd_geojson, join_field="CD118FP")
            boundary_cache[level] = _read_boundaries(b)
        elif level == "state_house":
            if not args.sldl_geojson.exists():
                raise SystemExit(f"Missing state house boundaries: {args.sldl_geojson}")
            b = Boundaries(level=level, path=args.sldl_geojson, join_field="SLDLST")
            boundary_cache[level] = _read_boundaries(b)
        elif level == "state_senate":
            if not args.sldu_geojson.exists():
                raise SystemExit(f"Missing state senate boundaries: {args.sldu_geojson}")
            b = Boundaries(level=level, path=args.sldu_geojson, join_field="SLDUST")
            boundary_cache[level] = _read_boundaries(b)
        elif level == "county":
            if not args.county_geojson.exists():
                raise SystemExit(f"Missing county boundaries: {args.county_geojson}")
            try:
                import geopandas as gpd
            except Exception as e:  # pragma: no cover
                raise SystemExit(f"geopandas is required to write GeoJSON outputs: {e}")
            gdf = gpd.read_file(args.county_geojson)
            county_field = _infer_county_join_field(gdf)
            gdf["_join_key"] = gdf[county_field].astype(str).map(normalize_county_loose)
            boundary_cache[level] = gdf
        else:
            raise AssertionError(level)
        return boundary_cache[level]

    # Precinct boundaries: handled separately since join requires 2 fields and varies by source.
    precinct_gdf = None
    if "precinct" in levels:
        if args.precinct_geojson.exists():
            try:
                import geopandas as gpd
            except Exception as e:  # pragma: no cover
                raise SystemExit(f"geopandas is required to write GeoJSON outputs: {e}")
            precinct_gdf = gpd.read_file(args.precinct_geojson)
            if args.precinct_county_field and args.precinct_code_field:
                county_field, code_field = args.precinct_county_field, args.precinct_code_field
            else:
                county_field, code_field = _infer_precinct_fields(precinct_gdf)
            precinct_gdf["_join_key"] = (
                precinct_gdf[county_field].astype(str).map(normalize_county_loose)
                + " - "
                + precinct_gdf[code_field].astype(str).str.strip().str.upper()
            ).str.replace(r"\s+", " ", regex=True).str.strip()
            boundary_cache["precinct"] = precinct_gdf
        else:
            print(f"WARNING: precinct level requested, but missing {args.precinct_geojson}; skipping precinct outputs.")
            levels = [l for l in levels if l != "precinct"]

    manifest: dict[str, object] = {"csv": str(args.csv), "levels": levels, "contests": []}

    for _, row in contests.iterrows():
        office = row["office"]
        district = row["district_raw"]

        contest_id = f"{office}|{district}"
        district_for_slug = district
        if office in {"U.S. House", "State House", "State Senate"} and district:
            district_for_slug = normalize_district_for_join(office, district)
        contest_slug = slugify(office) + ("__" + slugify(district_for_slug) if district_for_slug else "")
        print(f"- {contest_id} -> {contest_slug}")

        contest_entry = {"office": office, "district": district, "slug": contest_slug, "outputs": {}}

        for level in levels:
            # Gate district-based levels to matching office.
            if level == "congressional" and office != "U.S. House":
                continue
            if level == "state_house" and office != "State House":
                continue
            if level == "state_senate" and office != "State Senate":
                continue

            res = _build_results(df, level=level, office=office, district=district)
            if res.empty:
                continue

            if args.dry_run:
                contest_entry["outputs"][level] = {"path": str(args.out / level / f"{contest_slug}.geojson"), "rows": int(len(res))}
                continue

            # Load boundaries and merge.
            if level == "precinct":
                gdf = boundary_cache["precinct"]
            else:
                gdf = load_boundaries_for_level(level)

            merged = gdf.merge(res, left_on="_join_key", right_on="_geo_key", how="left")
            # Fill missing counts with zeros for convenience in styling.
            for c in ["total_votes", "dem_votes", "rep_votes", "other_votes", "winner_votes", "runnerup_votes", "margin_votes"]:
                if c in merged.columns:
                    merged[c] = merged[c].fillna(0).astype(int)
            if "winner_candidate" in merged.columns:
                merged["winner_candidate"] = merged["winner_candidate"].fillna("")
            if "winner_party" in merged.columns:
                merged["winner_party"] = merged["winner_party"].fillna("UNK")
            merged["office"] = office
            merged["district"] = district

            out_dir = args.out / level
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{contest_slug}.geojson"

            merged.to_file(out_path, driver="GeoJSON")
            contest_entry["outputs"][level] = {"path": str(out_path)}

        manifest["contests"].append(contest_entry)

    if not args.dry_run:
        args.out.mkdir(parents=True, exist_ok=True)
        manifest_path = args.out / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print(f"Wrote {manifest_path}")


if __name__ == "__main__":
    main()
