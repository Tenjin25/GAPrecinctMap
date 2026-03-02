"""
Ingest GA precinct-level results collected from county elections offices.

This is a local-file pipeline: you (or a browser) download each county's results file,
drop them under an input directory, then this script normalizes + merges them into a
single OpenElections-style CSV:

  county, precinct, office, district, party, candidate, total_votes

Supported inputs (best-effort heuristics):
  - CSVs that already look like OpenElections (votes/total_votes columns)
  - Excel exports that look like Clarity/SOS-style tables with columns like:
      Precinct, Office Name, Party, Ballot Name, Total
    or vote-method columns:
      Election Day Votes, Advance Voting Votes / Advanced Votes,
      Absentee by Mail Votes, Provisional Votes

Recommended directory layout (county inferred from folder name):
  Data/county_office/2021_runoff/Fulton/results.xlsx
  Data/county_office/2022_general/Cobb/results.csv

Usage:
  py scripts/ingest_county_precincts.py --in Data/county_office/2021_runoff --out Data/20210105__ga__runoff__from_counties.csv
  py scripts/ingest_county_precincts.py --in Data/county_office/2022_general --out Data/20221108__ga__general__precinct__from_counties.csv
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

try:
    from convert_to_openelections import parse_office_district, clean_candidate, clean_county, PARTY_MAP
except Exception:
    parse_office_district = None  # type: ignore[assignment]
    clean_candidate = None  # type: ignore[assignment]
    clean_county = None  # type: ignore[assignment]
    PARTY_MAP = {}  # type: ignore[assignment]


FINAL_COLS = ["county", "precinct", "office", "district", "party", "candidate", "total_votes"]


def _norm_col(c: str) -> str:
    return re.sub(r"\s+", " ", str(c or "").strip()).lower()


def _infer_county_from_path(path: Path) -> str | None:
    for part in reversed(path.parts):
        if part.lower() in {"2022", "20221108", "precinct", "results", "data", "raw"}:
            continue
        name = re.sub(r"[_\-]+", " ", part).strip()
        if not name:
            continue
        if clean_county:
            name = clean_county(name)
        return name
    return None


def _to_int_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0).astype(int)


def _compute_total_votes(df: pd.DataFrame, cols_by_norm: dict[str, str]) -> pd.Series:
    if "total_votes" in cols_by_norm:
        return _to_int_series(df[cols_by_norm["total_votes"]])
    if "votes" in cols_by_norm:
        return _to_int_series(df[cols_by_norm["votes"]])
    if "total" in cols_by_norm:
        return _to_int_series(df[cols_by_norm["total"]])

    # Vote-method columns (common in Clarity exports)
    ed = cols_by_norm.get("election day votes") or cols_by_norm.get("election_day_votes")
    adv = (
        cols_by_norm.get("advance voting votes")
        or cols_by_norm.get("advanced votes")
        or cols_by_norm.get("advanced_votes")
        or cols_by_norm.get("advance_votes")
    )
    abm = cols_by_norm.get("absentee by mail votes") or cols_by_norm.get("absentee_by_mail_votes")
    prov = cols_by_norm.get("provisional votes") or cols_by_norm.get("provisional_votes")
    if ed and adv and abm and prov:
        return _to_int_series(df[ed]) + _to_int_series(df[adv]) + _to_int_series(df[abm]) + _to_int_series(df[prov])

    raise ValueError("Could not find any usable vote columns (total/total_votes/votes or vote-method columns).")


def _pick_col(cols_by_norm: dict[str, str], *candidates: str) -> str | None:
    for key in candidates:
        if key in cols_by_norm:
            return cols_by_norm[key]
    return None


def _normalize_party_long(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if s in PARTY_MAP:
        return PARTY_MAP[s]
    u = s.upper()
    if u in PARTY_MAP:
        return PARTY_MAP[u]
    if u.startswith("REP"):
        return "Republican"
    if u.startswith("DEM"):
        return "Democrat"
    if u.startswith("LIB"):
        return "Libertarian"
    if u.startswith("GRN") or "GREEN" in u:
        return "Green"
    if u in {"IND", "INDEPENDENT"}:
        return "Independent"
    return s


def _coerce_office_district(raw_office: str) -> tuple[str, str]:
    if parse_office_district:
        office, district = parse_office_district(str(raw_office or ""))
        return (office or "").strip(), (district or "").strip()
    return (str(raw_office or "").strip(), "")


def _load_one(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str, low_memory=False)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        # Try common Clarity/SOS sheet names, then fall back to first sheet.
        for sheet in ["Precinct Results", "Precincts", "Precinct", "Detail", "Results"]:
            try:
                return pd.read_excel(path, sheet_name=sheet, dtype=str)
            except Exception:
                pass
        return pd.read_excel(path, sheet_name=0, dtype=str)
    raise ValueError(f"Unsupported file extension: {path.suffix}")


def _normalize_frame(df: pd.DataFrame, *, county_fallback: str, source_path: Path) -> pd.DataFrame:
    df = df.copy()
    cols_by_norm = {_norm_col(c): c for c in df.columns}

    county_col = _pick_col(cols_by_norm, "county", "county name")
    precinct_col = _pick_col(cols_by_norm, "precinct", "precinct name", "polling place", "polling place name")
    office_col = _pick_col(cols_by_norm, "office", "office name", "contest", "race")
    party_col = _pick_col(cols_by_norm, "party", "party name", "party abbreviation")
    cand_col = _pick_col(cols_by_norm, "candidate", "ballot name", "candidate name")

    if not office_col or not cand_col:
        raise ValueError(
            "Missing required columns for normalization. "
            f"Need office + candidate (got office={office_col!r}, candidate={cand_col!r})."
        )

    out = pd.DataFrame()

    if county_col:
        out["county"] = df[county_col].fillna("").astype(str).map(lambda x: clean_county(x) if clean_county else x)
    else:
        out["county"] = county_fallback

    if precinct_col:
        out["precinct"] = df[precinct_col].fillna("").astype(str).str.strip()
    else:
        out["precinct"] = ""

    office_district = df[office_col].fillna("").astype(str).map(_coerce_office_district)
    out["office"] = office_district.map(lambda t: t[0])
    out["district"] = office_district.map(lambda t: t[1])

    out["party"] = df[party_col].fillna("").astype(str).map(_normalize_party_long) if party_col else ""
    out["candidate"] = df[cand_col].fillna("").astype(str).map(lambda x: clean_candidate(x) if clean_candidate else str(x).strip())
    out["total_votes"] = _compute_total_votes(df, cols_by_norm)

    out = out[FINAL_COLS]

    # Drop summary rows (common: Party is blank, or candidate is 'Total Votes', etc.)
    out = out[out["candidate"].astype(str).str.strip().ne("")]
    out = out[out["office"].astype(str).str.strip().ne("")]

    # Ensure county fallback isn't empty
    if out["county"].astype(str).str.strip().eq("").any():
        raise ValueError(f"County missing for some rows (file={source_path}).")

    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", type=Path, required=True, help="Directory containing per-county files")
    ap.add_argument("--out", type=Path, required=True, help="Output merged OpenElections-style precinct CSV")
    ap.add_argument("--glob", default="**/*.*", help="Glob for input files (default: **/*.*)")
    args = ap.parse_args()

    in_dir: Path = args.in_dir
    files = sorted([p for p in in_dir.glob(args.glob) if p.is_file() and p.suffix.lower() in {".csv", ".xlsx", ".xls"}])
    if not files:
        raise SystemExit(f"No .csv/.xlsx/.xls files found under: {in_dir}")

    frames: list[pd.DataFrame] = []
    failures: list[tuple[Path, str]] = []

    for path in files:
        county = _infer_county_from_path(path) or ""
        try:
            raw = _load_one(path)
            norm = _normalize_frame(raw, county_fallback=county, source_path=path)
            frames.append(norm)
        except Exception as e:
            failures.append((path, f"{type(e).__name__}: {e}"))

    if failures:
        print("Some files could not be ingested:")
        for p, msg in failures[:50]:
            print(f"- {p}: {msg}")
        if len(failures) > 50:
            print(f"... and {len(failures) - 50} more")
        print("")

    if not frames:
        raise SystemExit("No files ingested successfully.")

    merged = pd.concat(frames, ignore_index=True)

    # Normalize county casing for consistency with the repo's precinct CSVs (Title Case).
    merged["county"] = merged["county"].astype(str).str.strip().str.title()
    merged["precinct"] = merged["precinct"].astype(str).str.strip()
    merged["office"] = merged["office"].astype(str).str.strip()
    merged["district"] = merged["district"].astype(str).str.strip()
    merged["party"] = merged["party"].astype(str).str.strip()
    merged["candidate"] = merged["candidate"].astype(str).str.strip()

    merged = merged[merged["total_votes"].astype(int) > 0].copy()
    merged.to_csv(args.out, index=False)

    print(f"Ingested {len(frames)} files -> {args.out}")
    print(f"Rows: {len(merged):,}")
    print(f"Counties: {merged['county'].nunique()}")


if __name__ == "__main__":
    main()
