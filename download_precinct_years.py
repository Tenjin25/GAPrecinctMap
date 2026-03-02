"""
Download and merge per-county precinct files for 2016 and 2018 GA general elections,
then normalize all precinct files (2016, 2018, 2022) to match the 2024 format:
  county, precinct, office, district, party, candidate, total_votes
"""

import urllib.request
import io
import pandas as pd
from pathlib import Path

DATA_DIR = Path("Data")

GA_COUNTIES = [
    "appling", "atkinson", "bacon", "baker", "baldwin", "banks", "barrow", "bartow",
    "ben_hill", "berrien", "bibb", "bleckley", "brantley", "brooks", "bryan", "bulloch",
    "burke", "butts", "calhoun", "camden", "candler", "carroll", "catoosa", "charlton",
    "chatham", "chattahoochee", "chattooga", "cherokee", "clarke", "clay", "clayton",
    "clinch", "cobb", "coffee", "colquitt", "columbia", "cook", "coweta", "crawford",
    "crisp", "dade", "dawson", "decatur", "dekalb", "dodge", "dooly", "dougherty",
    "douglas", "early", "echols", "effingham", "elbert", "emanuel", "evans", "fannin",
    "fayette", "floyd", "forsyth", "franklin", "fulton", "gilmer", "glascock", "glynn",
    "gordon", "grady", "greene", "gwinnett", "habersham", "hall", "hancock", "haralson",
    "harris", "hart", "heard", "henry", "houston", "irwin", "jackson", "jasper",
    "jeff_davis", "jefferson", "jenkins", "johnson", "jones", "lamar", "lanier",
    "laurens", "lee", "liberty", "lincoln", "long", "lowndes", "lumpkin", "macon",
    "madison", "marion", "mcduffie", "mcintosh", "meriwether", "miller", "mitchell",
    "monroe", "montgomery", "morgan", "murray", "muscogee", "newton", "oconee",
    "oglethorpe", "paulding", "peach", "pickens", "pierce", "pike", "polk", "pulaski",
    "putnam", "quitman", "rabun", "randolph", "richmond", "rockdale", "schley",
    "screven", "seminole", "spalding", "stephens", "stewart", "sumter", "talbot",
    "taliaferro", "tattnall", "taylor", "telfair", "terrell", "thomas", "tift",
    "toombs", "towns", "treutlen", "troup", "turner", "twiggs", "union", "upson",
    "walker", "walton", "ware", "warren", "washington", "wayne", "webster", "wheeler",
    "white", "whitfield", "wilcox", "wilkes", "wilkinson", "worth",
]

FINAL_COLS = ["county", "precinct", "office", "district", "party", "candidate", "total_votes"]

ELECTIONS = [
    {
        "year": "2014",
        "date": "20141104",
        "out": "20141104__ga__general__precinct.csv",
        "vote_col": "votes",          # already a total
    },
]


def fetch_county(year, date, county):
    url = (
        f"https://raw.githubusercontent.com/openelections/openelections-data-ga"
        f"/master/{year}/{date}__ga__general__{county}__precinct.csv"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            return pd.read_csv(io.BytesIO(resp.read()))
    except Exception as e:
        print(f"  WARNING: could not fetch {county}: {e}")
        return None


def merge_year(election):
    year = election["year"]
    date = election["date"]
    out_path = DATA_DIR / election["out"]
    vote_col = election["vote_col"]

    print(f"\n=== {year} general election ===")
    frames = []
    for i, county in enumerate(GA_COUNTIES, 1):
        print(f"  [{i:3d}/159] {county}", end="\r")
        df = fetch_county(year, date, county)
        if df is not None:
            frames.append(df)

    print(f"\n  Merging {len(frames)} county files…")
    combined = pd.concat(frames, ignore_index=True)

    # Rename existing total column → total_votes
    combined = combined.rename(columns={vote_col: "total_votes"})

    # Keep only the 7 standard columns
    combined = combined[FINAL_COLS]

    combined.to_csv(out_path, index=False)
    print(f"  Saved {len(combined):,} rows → {out_path}")


def normalize_2022():
    """Sum the 4 vote-method columns in the 2022 file → total_votes."""
    path = DATA_DIR / "20221108__ga__general__precinct.csv"
    if not path.exists():
        print("2022 precinct file not found, skipping.")
        return

    df = pd.read_csv(path)
    if "total_votes" in df.columns:
        print("\n2022 file already normalized, skipping.")
        return

    print("\n=== Normalizing 2022 precinct file ===")
    vote_cols = ["election_day_votes", "advanced_votes", "absentee_by_mail_votes", "provisional_votes"]
    df["total_votes"] = df[vote_cols].fillna(0).sum(axis=1).astype(int)
    df = df[FINAL_COLS]
    df.to_csv(path, index=False)
    print(f"  Saved {len(df):,} rows → {path}")


if __name__ == "__main__":
    for election in ELECTIONS:
        merge_year(election)
    print("\nDone.")
