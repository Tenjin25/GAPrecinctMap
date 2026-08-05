"""Aggregate Clarity/SOS nested county summary archives into OpenElections CSVs."""

from __future__ import annotations

import argparse
import csv
import io
import re
import zipfile
from pathlib import Path


def county_from_entry(name: str) -> str:
    stem = Path(name).name
    stem = re.sub(r"-summary\.zip$", "", stem, flags=re.IGNORECASE)
    return re.sub(r"_\d+_\d+$", "", stem).replace("_", " ").strip()


def contest_mapping(raw: str) -> tuple[str, str] | None:
    s = (raw or "").strip().lower()
    if "us senate" in s:
        return ("U.S. Senate (Special)", "") if ("loeffler" in s or "special" in s) else ("U.S. Senate", "")
    names = {
        "governor": "Governor", "lieutenant governor": "Lieutenant Governor",
        "secretary of state": "Secretary of State", "attorney general": "Attorney General",
        "commissioner of agriculture": "Commissioner of Agriculture",
        "commissioner of insurance": "Commissioner of Insurance",
        "commissioner of labor": "Commissioner of Labor",
        "state school superintendent": "State School Superintendent",
    }
    for prefix, office in names.items():
        if s.startswith(prefix):
            return office, ""
    return None


def party_from_choice(choice: str) -> str:
    m = re.search(r"\((dem|rep|lib|green|ind)\)", choice or "", flags=re.IGNORECASE)
    return {"dem": "Democrat", "rep": "Republican", "lib": "Libertarian", "green": "Green", "ind": "Independent"}.get(m.group(1).lower(), "") if m else ""


def clean_choice(choice: str) -> str:
    return re.sub(r"\s*\((?:dem|rep|lib|green|ind)\)\s*", " ", choice or "", flags=re.IGNORECASE).strip()


def parse_archive(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with zipfile.ZipFile(path) as outer:
        entries = [e for e in outer.infolist() if re.search(r"/summary/[^/]+-summary\.zip$", e.filename, re.I)]
        for entry in entries:
            county = county_from_entry(entry.filename)
            with zipfile.ZipFile(io.BytesIO(outer.read(entry))) as inner:
                summary = next((e for e in inner.infolist() if e.filename.lower().endswith("summary.csv")), None)
                if summary is None:
                    continue
                text = inner.read(summary).decode("utf-8-sig", errors="replace")
                for record in csv.DictReader(io.StringIO(text)):
                    office_info = contest_mapping(record.get("contest name", ""))
                    if office_info is None:
                        continue
                    office, district = office_info
                    raw_candidate = (record.get("choice name", "") or "").strip()
                    candidate = clean_choice(raw_candidate)
                    votes = re.sub(r"[^0-9-]", "", record.get("total votes", "") or "") or "0"
                    rows.append({"county": county, "precinct": "", "office": office, "district": district,
                                 "party": party_from_choice(raw_candidate), "candidate": candidate, "total_votes": votes})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    rows = parse_archive(args.archive)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["county", "precinct", "office", "district", "party", "candidate", "total_votes"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {args.out} ({len(rows)} rows; {len({r['county'] for r in rows})} counties)")


if __name__ == "__main__":
    main()
