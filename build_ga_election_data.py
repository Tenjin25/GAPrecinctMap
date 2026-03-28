"""
Build Georgia election data files for the precinct map app.

Reads OpenElections-format CSV files from Data/ and produces:
  Data/contests/manifest.json
  Data/contests/<contest_type>_<year>.json  (county-level slices)
  Data/ga_elections_aggregated.json          (legacy fallback)

Contest types produced (statewide D/R races only):
  president, us_senate, governor, lieutenant_governor, attorney_general,
  secretary_of_state, agriculture_commissioner, insurance_commissioner,
  labor_commissioner, superintendent
"""

import json
import re
import math
from pathlib import Path
from collections import defaultdict

import pandas as pd

DATA_DIR = Path("Data")
CONTESTS_DIR = DATA_DIR / "contests"

# ---------------------------------------------------------------------------
# Office string → (contest_type, is_district_based)
# ---------------------------------------------------------------------------
STATEWIDE_OFFICE_MAP = {
    "president":                       "president",
    "president of the united states":  "president",
    "presidential":                    "president",
    "u.s. senate":                     "us_senate",
    "us senate":                       "us_senate",
    "united states senate":            "us_senate",
    "united states senator":           "us_senate",
    # GA had two Senate special elections in 2020
    "u.s. senate (special)":          "us_senate_special",
    "us senate (special)":            "us_senate_special",
    "governor":                        "governor",
    "lieutenant governor":             "lieutenant_governor",
    "attorney general":                "attorney_general",
    "secretary of state":              "secretary_of_state",
    "commissioner of agriculture":     "agriculture_commissioner",
    "commissioner of insurance":       "insurance_commissioner",
    "commissioner of labor":           "labor_commissioner",
    "state school superintendent":     "superintendent",
    "superintendent of schools":       "superintendent",
    "superintendent":                  "superintendent",
}

INVALID_COUNTY_TOKENS = {
    "NOT AVAILABLE",
    "UNKNOWN",
    "N/A",
    "NA",
    "STATEWIDE",
    "STATE OF GEORGIA",
    "GEORGIA",
    "TOTAL",
}

# Files: (year, csv_path)
ELECTION_FILES = [
    (2014, DATA_DIR / "20141104__ga__general__precinct.csv"),
    (2016, DATA_DIR / "20161108__ga__general__precinct.csv"),
    (2018, DATA_DIR / "20181106__ga__general__precinct.csv"),
    (2020, DATA_DIR / "20201103__ga__general__precinct.csv"),
    (2022, DATA_DIR / "20221108__ga__general__precinct-total.csv"),
    (2024, DATA_DIR / "20241105__ga__general__precinct-level.csv"),
]

# County aggregates also from county-level files when available (more complete)
COUNTY_FILES = [
    (2000, DATA_DIR / "20001107__ga__general.csv"),
    (2002, DATA_DIR / "20021105__ga__general.csv"),
    (2004, DATA_DIR / "20041102__ga__general.csv"),
    (2006, DATA_DIR / "20061107__ga__general.csv"),
    (2008, DATA_DIR / "20081104__ga__general.csv"),
    (2010, DATA_DIR / "20101102__ga__general.csv"),
    (2012, DATA_DIR / "20121106__ga__general.csv"),
    (2014, DATA_DIR / "20141104__ga__general.csv"),
    (2016, DATA_DIR / "20161108__ga__general.csv"),
    (2022, DATA_DIR / "20221108__ga__general__county.csv"),
    (2024, DATA_DIR / "20241105__ga__general__county.csv"),
]


def normalize_office(raw: str) -> str | None:
    """Return a contest_type key, or None if not a tracked statewide race."""
    key = raw.strip().lower()
    return STATEWIDE_OFFICE_MAP.get(key)


def normalize_party(raw: str) -> str:
    p = (raw or "").strip().upper()
    if not p:
        return "O"  # other / unknown
    token = re.sub(r"[^A-Z]", "", p)
    if not token:
        return "O"

    # Some OpenElections exports encode incumbents as IR/ID (or RI/DI).
    if token.startswith("I") and len(token) > 1:
        token = token[1:]
    if token.endswith("I") and len(token) > 1:
        token = token[:-1]

    if token in ("DEMOCRAT", "DEMOCRATIC", "DEM", "D") or token.startswith("DEM") or "DEMOCRAT" in token:
        return "D"
    if token in ("REPUBLICAN", "REP", "R") or token.startswith("REP") or "REPUBLICAN" in token:
        return "R"
    return "O"  # other


def normalize_county(raw: str) -> str:
    return (raw or "").strip().upper()

def normalize_candidate_case(raw: str) -> str:
    name = re.sub(r"\s+", " ", (raw or "").strip())
    if not name:
        return ""
    # Preserve already mixed-case names; normalize legacy ALL-CAPS exports.
    if not any(ch.islower() for ch in name):
        name = name.title()
    # Keep common surname/suffix patterns readable after title-casing.
    name = re.sub(r"(?<=')([a-z])", lambda m: m.group(1).upper(), name)
    name = re.sub(r"\bMc([a-z])", lambda m: f"Mc{m.group(1).upper()}", name)
    name = re.sub(r"\b(Ii|Iii|Iv|Vi|Vii|Viii|Ix|Xi|Xii|Xiii|Xiv|Xv)\b", lambda m: m.group(1).upper(), name)
    name = re.sub(r"\bJr\.?\b", "Jr.", name)
    name = re.sub(r"\bSr\.?\b", "Sr.", name)
    return name

def decorate_candidate_label(candidate: str, party_short: str) -> str:
    """
    Adjust candidate display labels for the UI.

    OpenElections GA files sometimes mark incumbents as "(I)" in the candidate name.
    For the map UI + JSON outputs, convert incumbent markers like "(I)" (and legacy "(R*)")
    into a simple trailing "*" (e.g., "Jane Doe (I)" -> "Jane Doe*").
    """
    name = normalize_candidate_case(candidate)
    if not name:
        return ""
    incumbent = False
    if re.search(r"\(\s*I\s*\)", name, flags=re.IGNORECASE):
        incumbent = True
        name = re.sub(r"\(\s*I\s*\)", "", name, flags=re.IGNORECASE)

    # Back-compat: some older builds wrote incumbency as "(R*)" / "(D*)".
    if re.search(r"\(\s*[A-Z]{1,3}\s*\*\s*\)", name, flags=re.IGNORECASE):
        incumbent = True
        name = re.sub(r"\(\s*[A-Z]{1,3}\s*\*\s*\)", "", name, flags=re.IGNORECASE)

    name = re.sub(r"\s+", " ", name).strip()
    if incumbent and not name.endswith("*"):
        name = f"{name}*"
    return name


def votes_col(df: pd.DataFrame) -> str:
    """Return the votes column name (handles 'votes' vs 'total_votes')."""
    if "total_votes" in df.columns:
        return "total_votes"
    if "votes" in df.columns:
        return "votes"
    raise ValueError(
        "No votes column found (expected 'votes' or 'total_votes'). "
        f"Columns: {list(df.columns)}"
    )


def compute_votes(df: pd.DataFrame) -> pd.Series:
    """
    Return a numeric votes Series for a results dataframe.

    Supports:
      - OpenElections: 'votes' or 'total_votes'
      - GA SOS-style split methods: election_day/advance/absentee/provisional columns
    """
    if "total_votes" in df.columns:
        return pd.to_numeric(df["total_votes"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
    if "votes" in df.columns:
        return pd.to_numeric(df["votes"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)

    # Split vote-method exports (seen in GA 2021 runoff + some 2022/2024 precinct exports)
    method_cols = [
        "election_day_votes",
        "advanced_votes",
        "advance_votes",
        "absentee_by_mail_votes",
        "provisional_votes",
    ]
    present = [c for c in method_cols if c in df.columns]

    # Accept either advanced_votes or advance_votes (but not require both).
    required = {"election_day_votes", "absentee_by_mail_votes", "provisional_votes"}
    if required.issubset(df.columns) and ("advanced_votes" in df.columns or "advance_votes" in df.columns):
        adv = "advanced_votes" if "advanced_votes" in df.columns else "advance_votes"
        cols = ["election_day_votes", adv, "absentee_by_mail_votes", "provisional_votes"]
        parts = [
            pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0)
            for c in cols
        ]
        return sum(parts)

    raise ValueError(
        "No usable votes columns found. Expected 'votes', 'total_votes', or vote-method columns "
        "(election_day_votes + (advanced_votes|advance_votes) + absentee_by_mail_votes + provisional_votes). "
        f"Columns present: {list(df.columns)}"
    )


def build_margin_color(margin_pct: float, winner: str) -> str:
    """Category color matching the app's categoryColorForMargin() function."""
    m = abs(margin_pct)
    if winner == "R":
        if m >= 40: return "#67000d"
        if m >= 30: return "#a50f15"
        if m >= 20: return "#cb181d"
        if m >= 10: return "#ef3b2c"
        if m >= 5.5: return "#fb6a4a"
        if m >= 1.0: return "#fcae91"
        return "#fee8c8"
    else:  # D or T
        if m >= 40: return "#08306b"
        if m >= 30: return "#08519c"
        if m >= 20: return "#3182bd"
        if m >= 10: return "#6baed6"
        if m >= 5.5: return "#9ecae1"
        if m >= 1.0: return "#c6dbef"
        return "#e1f5fe"


def load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        print(f"  SKIP (not found): {path}")
        return None
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False)
    except Exception as err:
        print(f"  WARN (strict parse failed for {path.name}: {err}; retrying with tolerant parser)")
        df = pd.read_csv(path, dtype=str, engine="python", on_bad_lines="skip")
    df.columns = [c.strip().lower() for c in df.columns]
    return df


# ---------------------------------------------------------------------------
# Aggregate votes per county per contest from a precinct-level CSV
# ---------------------------------------------------------------------------
def aggregate_county_votes(df: pd.DataFrame):
    """
    Returns dict: { office_raw: { county: { party: [votes, candidate] } } }
    Uses vectorized groupby instead of iterrows for speed.
    """
    # Normalize columns
    df = df.copy()
    df["_office"] = df.get("office", pd.Series(dtype=str)).fillna("").str.strip()
    df["_county"] = df.get("county", pd.Series(dtype=str)).fillna("").str.strip().str.upper()
    df["_party_raw"] = df.get("party", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    df["_party"]  = df["_party_raw"].apply(normalize_party)
    df["_cand"]   = (
        df.get("candidate", pd.Series(dtype=str))
        .fillna("")
        .astype(str)
        .map(normalize_candidate_case)
    )
    df["_votes"] = compute_votes(df).astype(int)

    # Drop rows with no office/county or non-county rollups.
    df = df[df["_office"].ne("") & df["_county"].ne("")]
    df = df[~df["_county"].isin(INVALID_COUNTY_TOKENS)]

    # -----------------------------------------------------------------------
    # Fill missing party labels by candidate lookup (older OpenElections files
    # sometimes have blank 'party' for many counties but are consistent within
    # an office for candidate -> party elsewhere in the file).
    # -----------------------------------------------------------------------
    try:
        known = df[df["_party"].isin(["D", "R"]) & df["_cand"].ne("")][["_office", "_cand", "_party"]].copy()
        if not known.empty:
            known = known.drop_duplicates(subset=["_office", "_cand"], keep="first")
            df = df.merge(
                known.rename(columns={"_party": "_party_infer"}),
                on=["_office", "_cand"],
                how="left",
            )
            needs = df["_party_raw"].eq("") & df["_party"].eq("O") & df["_party_infer"].isin(["D", "R"])
            df.loc[needs, "_party"] = df.loc[needs, "_party_infer"]
            df = df.drop(columns=["_party_infer"])
    except Exception:
        # If anything goes sideways, keep original party classification.
        pass

    result = {}
    for office, grp in df.groupby("_office"):
        county_party = {}
        for (county, party), sub in grp.groupby(["_county", "_party"]):
            total_v = int(sub["_votes"].sum())
            cand = sub.loc[sub["_cand"].ne(""), "_cand"].iloc[0] if sub["_cand"].ne("").any() else ""
            if county not in county_party:
                county_party[county] = {}
            county_party[county][party] = [total_v, cand]
        result[office] = county_party

    return result


# ---------------------------------------------------------------------------
# Main build
# ---------------------------------------------------------------------------
def build_contest_rows(county_agg):
    """
    Given county_agg = { county: { party: [votes, candidate] } },
    return a list of row dicts for a single contest.
    """
    rows = []
    for county, parties in sorted(county_agg.items()):
        dem_votes = parties.get("D", [0, ""])[0]
        rep_votes = parties.get("R", [0, ""])[0]
        oth_votes = sum(v[0] for p, v in parties.items() if p not in ("D", "R"))
        total = dem_votes + rep_votes + oth_votes
        if total == 0:
            continue
        dem_cand = decorate_candidate_label(parties.get("D", [0, ""])[1], "D")
        rep_cand = decorate_candidate_label(parties.get("R", [0, ""])[1], "R")
        signed = (dem_votes - rep_votes) / total * 100 if total else 0
        winner = "DEM" if signed > 0 else ("REP" if signed < 0 else "TIE")
        margin_pct = signed  # signed: negative = R leads
        color = build_margin_color(abs(signed), "D" if signed > 0 else "R")
        rows.append({
            "county": county,
            "dem_votes": dem_votes,
            "rep_votes": rep_votes,
            "other_votes": oth_votes,
            "total_votes": total,
            "dem_candidate": dem_cand,
            "rep_candidate": rep_cand,
            "margin": dem_votes - rep_votes,
            "margin_pct": round(margin_pct, 4),
            "winner": winner,
            "color": color,
        })
    return rows


def main():
    CONTESTS_DIR.mkdir(exist_ok=True)

    # contest_type → year → { county_rows }
    all_slices = defaultdict(dict)  # all_slices[contest_type][year] = rows

    # -----------------------------------------------------------------------
    # Process county-level files first (more complete counts)
    # -----------------------------------------------------------------------
    county_overrides = defaultdict(dict)  # [contest_type][year] = rows
    for year, path in COUNTY_FILES:
        df = load_csv(path)
        if df is None:
            continue
        print(f"\nProcessing county file: {path.name}")
        agg = aggregate_county_votes(df)
        for office_raw, county_data in agg.items():
            ctype = normalize_office(office_raw)
            if not ctype:
                continue
            rows = build_contest_rows(county_data)
            if rows:
                county_overrides[ctype][year] = rows
                print(f"  {ctype} {year}: {len(rows)} counties")

    # -----------------------------------------------------------------------
    # Process precinct files (aggregate to county level)
    # -----------------------------------------------------------------------
    for year, path in ELECTION_FILES:
        df = load_csv(path)
        if df is None:
            continue
        print(f"\nProcessing: {path.name}")
        agg = aggregate_county_votes(df)
        for office_raw, county_data in agg.items():
            ctype = normalize_office(office_raw)
            if not ctype:
                continue
            # Don't overwrite with precinct data if we already have county-level data
            if year in county_overrides.get(ctype, {}):
                all_slices[ctype][year] = county_overrides[ctype][year]
                continue
            rows = build_contest_rows(county_data)
            if rows:
                all_slices[ctype][year] = rows
                print(f"  {ctype} {year}: {len(rows)} counties")

    # Apply county overrides
    for ctype, year_rows in county_overrides.items():
        for year, rows in year_rows.items():
            all_slices[ctype][year] = rows

    # -----------------------------------------------------------------------
    # Write individual slice files + manifest
    # -----------------------------------------------------------------------
    manifest_entries = []
    for ctype, year_slices in sorted(all_slices.items()):
        for year, rows in sorted(year_slices.items()):
            fname = f"{ctype}_{year}.json"
            out = CONTESTS_DIR / fname
            payload = {"contest_type": ctype, "year": year, "rows": rows}
            out.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
            manifest_entries.append({
                "contest_type": ctype,
                "year": year,
                "file": fname,
                "rows": len(rows),
            })
            print(f"  Wrote {out}  ({len(rows)} rows)")

    manifest_path = CONTESTS_DIR / "manifest.json"
    manifest_path.write_text(
        json.dumps({"files": manifest_entries}, indent=2), encoding="utf-8"
    )
    print(f"\nWrote {manifest_path}  ({len(manifest_entries)} entries)")

    # -----------------------------------------------------------------------
    # Write ga_elections_aggregated.json (legacy fallback)
    # -----------------------------------------------------------------------
    results_by_year = defaultdict(lambda: defaultdict(dict))
    office_group_map = {
        "president":              "presidential",
        "us_senate":              "us_senate",
        "us_senate_special":      "us_senate_special",
        "governor":               "governor",
        "lieutenant_governor":    "lieutenant_governor",
        "attorney_general":       "attorney_general",
        "secretary_of_state":     "secretary_of_state",
        "agriculture_commissioner": "agriculture_commissioner",
        "insurance_commissioner": "insurance_commissioner",
        "labor_commissioner":     "labor_commissioner",
        "superintendent":         "superintendent",
    }

    for ctype, year_slices in all_slices.items():
        group = office_group_map.get(ctype, ctype)
        for year, rows in year_slices.items():
            county_results = {}
            for r in rows:
                county_results[r["county"]] = {
                    "dem_votes": r["dem_votes"],
                    "rep_votes": r["rep_votes"],
                    "other_votes": r["other_votes"],
                    "total_votes": r["total_votes"],
                    "dem_candidate": r["dem_candidate"],
                    "rep_candidate": r["rep_candidate"],
                    "margin": r["margin"],
                    "margin_pct": r["margin_pct"],
                    "winner": r["winner"],
                    "competitiveness": {"color": r["color"]},
                }
            results_by_year[str(year)][group][ctype] = {"results": county_results}

    agg_path = DATA_DIR / "ga_elections_aggregated.json"
    agg_path.write_text(
        json.dumps({"results_by_year": dict(results_by_year)}, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"\nWrote {agg_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
