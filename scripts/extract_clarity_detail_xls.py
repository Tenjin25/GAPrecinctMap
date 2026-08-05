"""Extract official Georgia SOS SpreadsheetML detail workbooks from a nested archive."""

from __future__ import annotations

import argparse
import csv
import io
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

NS = "{urn:schemas-microsoft-com:office:spreadsheet}"


def party(raw: str) -> str:
    m = re.search(r"\((dem|rep|lib|green|ind)\)", raw or "", re.I)
    return {"dem": "Democrat", "rep": "Republican", "lib": "Libertarian", "green": "Green", "ind": "Independent"}.get(m.group(1).lower(), "") if m else ""


def candidate_name(raw: str) -> str:
    return re.sub(r"\s*\((?:dem|rep|lib|green|ind)\)\s*", " ", raw or "", flags=re.I).strip()


def office_info(title: str) -> tuple[str, str] | None:
    s = (title or "").strip()
    u = s.upper()
    if "US SENATE" in u:
        return ("U.S. Senate (Special)" if "SPECIAL" in u else "U.S. Senate", "")
    if u.startswith("PRESIDENT"):
        return "President", ""
    if u.startswith("GOVERNOR"):
        return "Governor", ""
    if u.startswith("LIEUTENANT GOVERNOR"):
        return "Lieutenant Governor", ""
    names = {
        "SECRETARY OF STATE": "Secretary of State", "ATTORNEY GENERAL": "Attorney General",
        "COMMISSIONER OF AGRICULTURE": "Commissioner of Agriculture",
        "COMMISSIONER OF INSURANCE": "Commissioner of Insurance",
        "COMMISSIONER OF LABOR": "Commissioner of Labor",
        "STATE SCHOOL SUPERINTENDENT": "State School Superintendent",
    }
    for prefix, office in names.items():
        if u.startswith(prefix):
            return office, ""
    m = re.search(r"US HOUSE(?: OF REPRESENTATIVES)?\s*-?\s*DISTRICT\s+(\d+)", u)
    if m:
        return "U.S. House", m.group(1)
    m = re.search(r"STATE HOUSE\s*-?\s*DISTRICT\s+(\d+)", u)
    if m:
        return "State House", m.group(1)
    m = re.search(r"STATE SENATE\s*-?\s*DISTRICT\s+(\d+)", u)
    if m:
        return "State Senate", m.group(1)
    m = re.search(r"PUBLIC SERVICE COMMISSION\s+DISTRICT\s+(\d+)", u)
    if m:
        return "Public Service Commission", m.group(1)
    return None


def values(row: ET.Element) -> list[str]:
    out: list[str] = []
    for cell in row.findall(NS + "Cell"):
        idx = cell.attrib.get(NS + "Index")
        if idx:
            while len(out) < int(idx) - 1:
                out.append("")
        data = cell.find(NS + "Data")
        out.append("" if data is None else "".join(data.itertext()).strip())
    return out


def extract_archive(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with zipfile.ZipFile(path) as outer:
        entries = [e for e in outer.infolist() if re.search(r"/detail/xls/[^/]+-detailxls\.zip$", e.filename, re.I)]
        for entry in entries:
            county = re.sub(r"_\d+_\d+$", "", Path(entry.filename).name.replace("-detailxls.zip", ""), flags=re.I).replace("_", " ")
            with zipfile.ZipFile(io.BytesIO(outer.read(entry))) as inner:
                workbook = next((e for e in inner.infolist() if e.filename.lower().endswith("detail.xls")), None)
                if workbook is None:
                    continue
                root = ET.fromstring(inner.read(workbook))
                for sheet in root.findall(NS + "Worksheet"):
                    sheet_rows = sheet.findall(".//" + NS + "Row")
                    if len(sheet_rows) < 4:
                        continue
                    title_vals = values(sheet_rows[0])
                    info = office_info(title_vals[0] if title_vals else "")
                    if info is None:
                        continue
                    office, district = info
                    header = values(sheet_rows[1])
                    candidate_positions = [(i, v) for i, v in enumerate(header[2:], start=2) if v.strip()]
                    if not candidate_positions:
                        continue
                    for data_row in sheet_rows[3:]:
                        vals = values(data_row)
                        precinct = vals[0].strip() if vals else ""
                        if not precinct or precinct.lower().startswith("total"):
                            continue
                        for pos, raw_candidate in candidate_positions:
                            # The candidate header occupies one cell, while each
                            # candidate's data occupies five cells: election day,
                            # advanced, absentee, provisional, and total. The old
                            # pos + 5 logic shifted every candidate by one cell.
                            candidate_number = pos - 2
                            total_idx = 6 + (candidate_number * 5)
                            if total_idx >= len(vals):
                                continue
                            total = re.sub(r"[^0-9-]", "", vals[total_idx]) or "0"
                            rows.append({"county": county, "precinct": precinct, "office": office, "district": district,
                                         "party": party(raw_candidate), "candidate": candidate_name(raw_candidate), "total_votes": total})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--archive", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    rows = extract_archive(args.archive)
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["county", "precinct", "office", "district", "party", "candidate", "total_votes"])
        writer.writeheader(); writer.writerows(rows)
    print(f"Wrote {args.out} ({len(rows)} rows; {len({r['county'] for r in rows})} counties)")


if __name__ == "__main__":
    main()
