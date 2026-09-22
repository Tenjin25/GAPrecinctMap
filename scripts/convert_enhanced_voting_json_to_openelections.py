"""Convert a Georgia Enhanced Voting JSON export to OpenElections-style CSV.

Output columns:
    county, precinct, office, district, party, candidate, votes
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path


PARTY_SUFFIXES = {
    "Dem": "DEM",
    "Rep": "REP",
    "Lib": "LIB",
}


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def split_party_suffix(name: str) -> tuple[str, str]:
    for suffix, party in PARTY_SUFFIXES.items():
        marker = f" - {suffix}"
        if name.endswith(marker):
            return name[: -len(marker)].strip(), party
    return name, ""


def normalize_office(raw_name: str) -> tuple[str, str, str]:
    """Return canonical office, district/seat, and party inferred from the title."""
    name, title_party = split_party_suffix(clean_text(raw_name))
    # Some county records use bilingual English/Spanish contest titles. Keep
    # the English title while retaining a trailing judicial seat designation.
    if "/" in name:
        seat_match = re.search(r"\s(\([^)]+\))$", name)
        name = name.split("/", 1)[0].strip()
        if seat_match and not name.endswith(seat_match.group(1)):
            name = f"{name} {seat_match.group(1)}"

    judicial_patterns = (
        (r"^Justice - Supreme Court of Georgia \((.+)\)$", "Supreme Court Justice"),
        (r"^Judge - Court of Appeals of Georgia \((.+)\)$", "Appeals Court Judge"),
        (r"^Judge - Supreme Court \((.+)\)$", "Supreme Court Justice"),
        (r"^Judge - Court of Appeals \((.+)\)$", "Appeals Court Judge"),
        (r"^Supreme Court - (.+)$", "Supreme Court Justice"),
        (r"^Court of Appeals - (.+)$", "Appeals Court Judge"),
        (r"^Supreme Court Justice, (.+)$", "Supreme Court Justice"),
        (r"^Appeals Court Judge, (.+)$", "Appeals Court Judge"),
        (r"^Judge - Superior Court - (.+?) \((.+)\)$", "Superior Court Judge"),
    )
    for pattern, office in judicial_patterns:
        match = re.match(pattern, name, flags=re.IGNORECASE)
        if not match:
            continue
        if office == "Superior Court Judge":
            return office, f"{match.group(1)}; {match.group(2)}", title_party
        return office, match.group(1), title_party

    office_patterns = (
        (r"^US Senate$", "U.S. Senate"),
        (r"^US House of Representatives - District (.+)$", "U.S. House"),
        (r"^State Senate - District (.+)$", "State Senate"),
        (r"^State House of Representatives - District (.+)$", "State House"),
        (r"^PSC - District (.+)$", "Public Service Commissioner"),
        (r"^District Attorney - (.+)$", "District Attorney"),
    )
    for pattern, office in office_patterns:
        match = re.match(pattern, name, flags=re.IGNORECASE)
        if match:
            district = match.group(1) if match.groups() else ""
            return office, district, title_party

    district_match = re.match(r"^(.+?) - (District .+)$", name, flags=re.IGNORECASE)
    if district_match:
        return district_match.group(1), district_match.group(2).removeprefix("District "), title_party

    return name, "", title_party


def normalize_party(option_party: object, inferred_party: str, office: str) -> str:
    party = clean_text(option_party).upper()
    if party:
        aliases = {
            "DEMOCRAT": "DEM",
            "DEMOCRATIC": "DEM",
            "REPUBLICAN": "REP",
            "LIBERTARIAN": "LIB",
            "NONPARTISAN": "NONPARTISAN",
        }
        return aliases.get(party, party)
    if inferred_party:
        return inferred_party
    if office in {"Supreme Court Justice", "Appeals Court Judge", "Superior Court Judge"}:
        return "NONPARTISAN"
    return ""


def convert(source: Path, output: Path) -> dict[str, int]:
    with source.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)

    local_results = payload.get("localResults") or []
    if not local_results:
        raise ValueError("The export does not contain localResults with precinct data")

    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["county", "precinct", "office", "district", "party", "candidate", "votes"]
    row_count = 0
    local_total_mismatches = 0
    missing_precinct_results = 0
    empty_precinct_results = 0
    statewide_rollups: dict[tuple[str, str], int] = {}

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()

        for locality in local_results:
            county = re.sub(r"\s+County$", "", clean_text(locality.get("name")), flags=re.IGNORECASE)
            for ballot_item in locality.get("ballotItems") or []:
                office, district, inferred_party = normalize_office(ballot_item.get("name") or "")
                contest_id = clean_text(ballot_item.get("id"))
                for option in ballot_item.get("ballotOptions") or []:
                    candidate = clean_text(option.get("name"))
                    party = normalize_party(option.get("politicalParty"), inferred_party, office)
                    precinct_results = option.get("precinctResults")
                    if precinct_results is None:
                        missing_precinct_results += 1
                        continue
                    if not precinct_results:
                        empty_precinct_results += 1

                    precinct_sum = 0
                    for result in precinct_results:
                        votes = int(result.get("voteCount") or 0)
                        precinct_sum += votes
                        writer.writerow(
                            {
                                "county": county,
                                "precinct": clean_text(result.get("name")),
                                "office": office,
                                "district": district,
                                "party": party,
                                "candidate": candidate,
                                "votes": votes,
                            }
                        )
                        row_count += 1

                    option_total = int(option.get("voteCount") or 0)
                    if precinct_sum != option_total:
                        local_total_mismatches += 1
                        # The SOS export occasionally includes county-level votes
                        # that are not assigned to a named precinct. Preserve them
                        # explicitly instead of silently understating the contest.
                        residual = option_total - precinct_sum
                        if residual < 0:
                            raise ValueError(
                                f"Precinct votes exceed county total for {county}, "
                                f"{ballot_item.get('name')}, {candidate}"
                            )
                        writer.writerow(
                            {
                                "county": county,
                                "precinct": "NOT AVAILABLE",
                                "office": office,
                                "district": district,
                                "party": party,
                                "candidate": candidate,
                                "votes": residual,
                            }
                        )
                        row_count += 1
                        precinct_sum += residual
                    statewide_rollups[(contest_id, candidate)] = (
                        statewide_rollups.get((contest_id, candidate), 0) + precinct_sum
                    )

    statewide_total_mismatches = 0
    checked_statewide_options = 0
    for ballot_item in (payload.get("results") or {}).get("ballotItems") or []:
        if clean_text(ballot_item.get("type")).lower() != "state":
            continue
        contest_id = clean_text(ballot_item.get("id"))
        for option in ballot_item.get("ballotOptions") or []:
            candidate = clean_text(option.get("name"))
            key = (contest_id, candidate)
            if key not in statewide_rollups:
                continue
            checked_statewide_options += 1
            if statewide_rollups[key] != int(option.get("voteCount") or 0):
                statewide_total_mismatches += 1

    return {
        "rows": row_count,
        "counties": len(local_results),
        "missing_precinct_results": missing_precinct_results,
        "empty_precinct_results": empty_precinct_results,
        "local_total_mismatches": local_total_mismatches,
        "checked_statewide_options": checked_statewide_options,
        "statewide_total_mismatches": statewide_total_mismatches,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    stats = convert(args.source, args.output)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
