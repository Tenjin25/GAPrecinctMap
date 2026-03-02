"""
Convert Georgia SOS Clarity Excel exports to OpenElections CSV format.

Output columns (OpenElections standard):
  county, precinct, office, district, party, candidate, votes
"""

import re
import pandas as pd

# ---------------------------------------------------------------------------
# Mappings
# ---------------------------------------------------------------------------

PARTY_MAP = {
    'REP': 'Republican', 'Rep': 'Republican',
    'DEM': 'Democrat',   'Dem': 'Democrat',
    'LIB': 'Libertarian','Lib': 'Libertarian',
    'GRN': 'Green',      'Grn': 'Green',
    'IND': 'Independent','Ind': 'Independent',
    'NPA': 'Nonpartisan','NP':  'Nonpartisan',
}

OFFICE_MAP = {
    'President of the US':           ('President', ''),
    'US Senate':                      ('U.S. Senate', ''),
    'Governor':                       ('Governor', ''),
    'Lieutenant Governor':            ('Lieutenant Governor', ''),
    'Secretary of State':             ('Secretary of State', ''),
    'Attorney General':               ('Attorney General', ''),
    'Commissioner of Agriculture':    ('Commissioner of Agriculture', ''),
    'Commissioner of Insurance':      ('Commissioner of Insurance', ''),
    'State School Superintendent':    ('State School Superintendent', ''),
    'Commissioner of Labor':          ('Commissioner of Labor', ''),
}


def parse_office_district(raw: str):
    """Return (office, district) tuple from a raw Office Name string."""
    raw = raw.strip()

    # Direct lookup first
    if raw in OFFICE_MAP:
        return OFFICE_MAP[raw]

    # U.S. House  – various formats
    m = re.match(r'US House(?:\s+of\s+Representatives)?\s*[-–]\s*District\s+(\d+)', raw, re.I)
    if m:
        return ('U.S. House', m.group(1))
    m = re.match(r'US House\s+Dist(?:rict)?\s+(\d+)', raw, re.I)
    if m:
        return ('U.S. House', m.group(1))

    # State Senate
    m = re.match(r'State Senate(?:\s+[-–]\s*District|\s+Dist(?:rict)?)\s+(\d+)', raw, re.I)
    if m:
        return ('State Senate', m.group(1))

    # State House / State House of Representatives
    m = re.match(r'State House(?:\s+of\s+Representatives)?\s*[-–]\s*District\s+(\d+)', raw, re.I)
    if m:
        return ('State House', m.group(1))
    m = re.match(r'State House(?:\s+of\s+Representatives)?\s+Dist(?:rict)?\s+(\d+)', raw, re.I)
    if m:
        return ('State House', m.group(1))

    # District Attorney
    m = re.match(r'District Attorney\s*[-–]\s*(.+)', raw, re.I)
    if m:
        return ('District Attorney', m.group(1).strip())

    # Constitutional amendments / referendums – keep as-is, no district
    if re.match(r'(Proposed Constitutional|Statewide Referendum)', raw, re.I):
        return (raw, '')

    # Fallback
    return (raw, '')


def clean_candidate(name: str) -> str:
    """Strip trailing party abbreviation in parentheses, e.g. ' (Rep)'."""
    # Matches the last " (Xxx)" that looks like a short party tag (1-4 chars)
    cleaned = re.sub(r'\s*\([A-Za-z]{1,4}\)\s*$', '', name.strip())
    return cleaned.strip()


def clean_county(raw: str) -> str:
    """'Appling County' → 'Appling'"""
    return re.sub(r'\s+County$', '', raw.strip(), flags=re.I)


# ---------------------------------------------------------------------------
# Conversion function
# ---------------------------------------------------------------------------

def convert_file(filepath: str, county_col: str, sheet_name: str, out_path: str):
    df = pd.read_excel(filepath, sheet_name=sheet_name)

    # Keep only candidate rows (drop 'Total Votes' summary rows)
    df = df[df['Party'].notna()].copy()

    # Build output columns
    df['county']    = df[county_col].apply(clean_county)
    df['precinct']  = ''   # county-level data – no precinct breakdown
    df[['office', 'district']] = df['Office Name'].apply(
        lambda x: pd.Series(parse_office_district(x))
    )
    df['party']     = df['Party'].map(PARTY_MAP).fillna(df['Party'])
    df['candidate'] = df['Ballot Name'].apply(clean_candidate)
    df['votes']     = df['Total'].astype(int)

    result = df[['county', 'precinct', 'office', 'district', 'party', 'candidate', 'votes']]
    result = result.sort_values(['county', 'office', 'district', 'party', 'candidate'])
    result.to_csv(out_path, index=False)
    print(f"Wrote {len(result):,} rows → {out_path}")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    # 2022 General Election (county-level)
    df22 = convert_file(
        filepath    = 'Data/Total Votes Results_7ceee513-d4c4-4b68-9397-68a73d22dc4f.xlsx',
        county_col  = 'Precinct',   # SOS column name is "Precinct" but values are counties
        sheet_name  = 'Precinct Results',
        out_path    = 'Data/20221108__ga__general__county.csv',
    )

    # 2024 General Election (county-level)
    df24 = convert_file(
        filepath    = 'Data/Total Votes Results_3675bf50-5eeb-4407-b538-c04b909ccd08.xlsx',
        county_col  = 'County',
        sheet_name  = 'County Results',
        out_path    = 'Data/20241105__ga__general__county.csv',
    )

    # Quick sanity check
    print("\n--- 2022 sample ---")
    print(df22.head(10).to_string(index=False))

    print("\n--- 2024 sample ---")
    print(df24.head(10).to_string(index=False))

    print("\n--- Offices found in 2022 ---")
    print(df22[['office', 'district']].drop_duplicates().sort_values(['office','district']).to_string(index=False))
