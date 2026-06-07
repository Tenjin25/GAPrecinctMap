import argparse
import csv
from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET


def normalize_county_name(raw: str) -> str:
    name = str(raw or "").strip()
    if name.startswith("."):
        name = name[1:].strip()
    name = name.replace("County, Georgia", "").replace("County", "").replace(", Georgia", "").strip()
    return name


def normalize_county_token(raw: str) -> str:
    return normalize_county_name(raw).upper()


def clean_int(value) -> int:
    if value is None:
        return 0
    text = str(value).replace(",", "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return 0
    return int(round(float(text)))


def column_letters(cell_ref: str) -> str:
    return "".join(ch for ch in cell_ref if ch.isalpha())


def load_shared_strings(zf: ZipFile):
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    values = []
    for si in root.findall("x:si", ns):
        parts = [node.text or "" for node in si.findall(".//x:t", ns)]
        values.append("".join(parts))
    return values


def parse_sheet_rows(xlsx_path: Path):
    with ZipFile(xlsx_path) as zf:
        shared_strings = load_shared_strings(zf)
        root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))

    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rows = []
    for row in root.findall(".//x:sheetData/x:row", ns):
        values = {}
        for cell in row.findall("x:c", ns):
            ref = cell.attrib.get("r", "")
            col = column_letters(ref)
            cell_type = cell.attrib.get("t")
            value_node = cell.find("x:v", ns)
            if value_node is None:
                continue
            raw = value_node.text or ""
            if cell_type == "s":
                value = shared_strings[int(raw)]
            else:
                value = raw
            values[col] = value
        rows.append(values)
    return rows


def build_rows(sheet_rows):
    rows = []
    for record in sheet_rows[4:]:
        area = str(record.get("A", "")).strip()
        if not area:
            continue
        if area != "Georgia" and "County, Georgia" not in area:
            continue

        county_name = "Georgia" if area == "Georgia" else normalize_county_name(area)
        county_norm = "GEORGIA" if county_name == "Georgia" else normalize_county_token(county_name)

        base_2020 = clean_int(record.get("B"))
        population_2020 = clean_int(record.get("C"))
        population_2021 = clean_int(record.get("D"))
        population_2022 = clean_int(record.get("E"))
        population_2023 = clean_int(record.get("F"))
        population_2024 = clean_int(record.get("G"))
        population_2025 = clean_int(record.get("H"))

        change_2020_2025 = population_2025 - population_2020
        change_2024_2025 = population_2025 - population_2024
        change_2020_2025_pct = (change_2020_2025 / population_2020 * 100.0) if population_2020 else 0.0
        change_2024_2025_pct = (change_2024_2025 / population_2024 * 100.0) if population_2024 else 0.0

        rows.append(
            {
                "county_name": county_name,
                "county_norm": county_norm,
                "base_2020": base_2020,
                "population_2020": population_2020,
                "population_2021": population_2021,
                "population_2022": population_2022,
                "population_2023": population_2023,
                "population_2024": population_2024,
                "population_2025": population_2025,
                "change_2020_2025": change_2020_2025,
                "change_2020_2025_pct": round(change_2020_2025_pct, 4),
                "change_2024_2025": change_2024_2025,
                "change_2024_2025_pct": round(change_2024_2025_pct, 4),
            }
        )
    return rows


def main():
    parser = argparse.ArgumentParser(description="Convert Census CO-EST 2025 workbook to normalized CSV.")
    parser.add_argument("input_xlsx", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()

    rows = build_rows(parse_sheet_rows(args.input_xlsx))
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)

    with args.output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "county_name",
                "county_norm",
                "base_2020",
                "population_2020",
                "population_2021",
                "population_2022",
                "population_2023",
                "population_2024",
                "population_2025",
                "change_2020_2025",
                "change_2020_2025_pct",
                "change_2024_2025",
                "change_2024_2025_pct",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
