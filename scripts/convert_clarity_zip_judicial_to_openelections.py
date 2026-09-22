"""Extract statewide Georgia appellate-court precinct results from SOS ZIPs.

The historical SOS downloads contain one nested ``*-detailtxt.zip`` per county.
Each nested archive contains a fixed-width ``detail.txt`` report.  This script
extracts only Supreme Court and Court of Appeals contests and writes the common
OpenElections columns used by this repository.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import zipfile
from pathlib import Path


FIELDS = ["county", "precinct", "office", "district", "party", "candidate", "votes"]
TITLE_PATTERNS = (
    re.compile(r"(?:Judge|Justice)\s*-\s*(Supreme Court|Court of Appeals)(?: of Georgia)?\s*\((.+?)\)(?:\s*\(Vote For \d+\))?\s*$", re.I),
    re.compile(r"(Supreme Court|Court of Appeals)\s*-\s*(.+?)(?:\s*\(Vote For \d+\))?\s*$", re.I),
    re.compile(r"(Supreme Court)\s+Justice,\s*(.+?)\s*$", re.I),
    re.compile(r"(Court of Appeals)\s+Judge,\s*(.+?)\s*$", re.I),
    re.compile(r"(Appeals Court)\s+Judge,\s*(.+?)\s*$", re.I),
)


def parse_title(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    for pattern in TITLE_PATTERNS:
        match = pattern.search(stripped)
        if match:
            court = match.group(1)
            seat = match.group(2).split("/", 1)[0].strip().rstrip(")").strip()
            if seat.casefold() == "lagura":
                seat = "LaGrua"
            if court.casefold() == "appeals court":
                court = "Court of Appeals"
            return court, seat
    return None


def fields_with_starts(line: str) -> list[tuple[int, str]]:
    return [(m.start(), m.group(0).strip()) for m in re.finditer(r"\S(?:.*?\S)?(?=\s{2,}|$)", line)]


def value_at(line: str, starts: list[int], index: int) -> str:
    end = starts[index + 1] if index + 1 < len(starts) else len(line)
    return line[starts[index] : end].strip()


def parse_int(value: str) -> int:
    cleaned = value.replace(",", "").strip()
    return int(cleaned) if cleaned and re.fullmatch(r"-?\d+", cleaned) else 0


def county_from_member(member: str) -> str:
    filename = Path(member).name
    county = re.sub(r"_\d+_\d+-detailtxt\.zip$", "", filename, flags=re.I)
    return county.replace("_", " ").strip()


def parse_report(text: str, county: str) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    lines = text.replace("\ufeff", "").splitlines()
    rows: list[dict[str, object]] = []
    audits: list[dict[str, object]] = []

    for i, raw_title in enumerate(lines):
        title = parse_title(raw_title)
        if not title:
            continue
        court, seat = title
        office = "Supreme Court Justice" if court.lower().startswith("supreme") else "Appeals Court Judge"

        # Candidate names are on the next nonblank line; the following line is
        # the fixed-width column header beginning with Precinct or County.
        j = i + 1
        while j < len(lines) and not lines[j].strip():
            j += 1
        if j + 1 >= len(lines):
            continue
        candidate_line = lines[j]
        header_index = j + 1
        while header_index < len(lines) and not re.match(r"\s*(Precinct|County)\s{2,}", lines[header_index]):
            header_index += 1
        if header_index >= len(lines):
            continue

        header_fields = fields_with_starts(lines[header_index])
        labels = [label for _, label in header_fields]
        starts = [start for start, _ in header_fields]
        choice_indexes = [k for k, label in enumerate(labels) if label.lower() == "choice total"]
        if not choice_indexes:
            continue

        block_starts: list[int] = []
        previous_choice = 1
        for choice_index in choice_indexes:
            block_start_index = previous_choice + 1
            block_starts.append(starts[block_start_index])
            previous_choice = choice_index

        candidates: list[str] = []
        for k, start in enumerate(block_starts):
            end = block_starts[k + 1] if k + 1 < len(block_starts) else len(candidate_line)
            candidate = re.sub(r"\s+", " ", candidate_line[start:end]).strip()
            candidates.append(candidate)
        if len(candidates) != len(choice_indexes) or any(not c for c in candidates):
            raise ValueError(f"Could not parse candidates in {county}: {raw_title.strip()}")

        section_sums = [0] * len(candidates)
        totals = None
        k = header_index + 1
        while k < len(lines):
            line = lines[k]
            stripped = line.strip()
            if not stripped:
                break
            if stripped.startswith("Totals:"):
                totals = [parse_int(value_at(line, starts, idx)) for idx in choice_indexes]
                break
            precinct = value_at(line, starts, 0)
            if not precinct:
                k += 1
                continue
            for candidate_index, (candidate, choice_index) in enumerate(zip(candidates, choice_indexes)):
                votes = parse_int(value_at(line, starts, choice_index))
                section_sums[candidate_index] += votes
                rows.append({
                    "county": county,
                    "precinct": precinct,
                    "office": office,
                    "district": seat,
                    "party": "NONPARTISAN",
                    "candidate": candidate,
                    "votes": votes,
                })
            k += 1

        audits.append({
            "county": county,
            "office": office,
            "district": seat,
            "candidates": candidates,
            "precinct_sums": section_sums,
            "reported_totals": totals,
            "matches": totals == section_sums,
        })
    return rows, audits


def convert(source: Path, output: Path, audit_output: Path | None = None) -> dict[str, object]:
    all_rows: list[dict[str, object]] = []
    audits: list[dict[str, object]] = []
    counties: set[str] = set()
    with zipfile.ZipFile(source) as outer:
        members = [n for n in outer.namelist() if n.lower().endswith("-detailtxt.zip")]
        for member in members:
            county = county_from_member(member)
            with zipfile.ZipFile(io.BytesIO(outer.read(member))) as inner:
                text_member = next((n for n in inner.namelist() if n.lower().endswith(".txt")), None)
                if not text_member:
                    continue
                text = inner.read(text_member).decode("utf-8-sig", errors="replace")
            rows, county_audits = parse_report(text, county)
            if rows:
                counties.add(county)
                all_rows.extend(rows)
                audits.extend(county_audits)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(all_rows)

    if audit_output:
        audit_output.parent.mkdir(parents=True, exist_ok=True)
        audit_output.write_text(json.dumps(audits, indent=2), encoding="utf-8")

    mismatches = [a for a in audits if not a["matches"]]
    contests = {(str(r["office"]), str(r["district"])) for r in all_rows}
    return {
        "source": str(source),
        "output": str(output),
        "rows": len(all_rows),
        "counties": len(counties),
        "contests": len(contests),
        "sections": len(audits),
        "section_total_mismatches": len(mismatches),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--audit-output", type=Path)
    args = parser.parse_args()
    print(json.dumps(convert(args.source, args.output, args.audit_output), indent=2))


if __name__ == "__main__":
    main()
