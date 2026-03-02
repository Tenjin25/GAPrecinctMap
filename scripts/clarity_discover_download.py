"""
Discover and optionally download structured Clarity (SOE Software) election results files.

Enhanced Voting often embeds/links to Clarity-hosted public results. Precinct-level data,
when available, is typically exposed via downloadable "detail" report ZIPs.

This helper uses the open-source `clarify` library to:
  - determine which structured report URLs exist (xml/xls/csv)
  - list sub-jurisdictions (e.g., counties under a statewide GA page)
  - optionally download the report ZIPs

Install:
  py -m pip install clarify

Examples:
  # Discover statewide report URLs + counties
  py scripts/clarity_discover_download.py --url "https://results.enr.clarityelections.com/GA/105369/264614/#/summary" --level state

  # Download the county detail XML zips for all counties (when available)
  py scripts/clarity_discover_download.py --url "https://results.enr.clarityelections.com/GA/105369/264614/#/summary" --level state --download --format xml --out Data/clarity/2020_general
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path


def _download(url: str, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url, timeout=60) as resp:
        out_path.write_bytes(resp.read())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="Any Clarity election URL (web.#/summary or en/summary.html)")
    ap.add_argument("--level", choices=["state", "county", "city"], default="state")
    ap.add_argument("--download", action="store_true", help="Download report zips")
    ap.add_argument("--format", choices=["xml", "xls", "csv"], default="xml", help="Which report ZIP to download")
    ap.add_argument("--out", type=Path, default=Path("Data/clarity"), help="Output directory (for downloads)")
    args = ap.parse_args()

    try:
        import clarify  # type: ignore
    except Exception as e:
        print("Missing dependency: clarify")
        print("Install with: py -m pip install clarify")
        print(f"Import error: {e}")
        raise SystemExit(2)

    j = clarify.Jurisdiction(url=args.url, level=args.level)

    print("=== Jurisdiction ===")
    print("Name:", getattr(j, "name", None))
    for fmt in ["xml", "xls", "csv"]:
        try:
            u = j.report_url(fmt)
        except Exception:
            u = None
        if u:
            print(f"{fmt}: {u}")

    subs = []
    try:
        subs = j.get_subjurisdictions()
    except Exception as e:
        print("\nNo sub-jurisdictions found (or failed to fetch):", e)

    if subs:
        print("\n=== Sub-jurisdictions ===")
        for s in subs:
            try:
                su = s.report_url(args.format)
            except Exception:
                su = None
            print(f"- {s.name}: {su or '(no report url)'}")

    if not args.download:
        return

    fmt = args.format

    # Download parent jurisdiction report zip (if present)
    try:
        base_url = j.report_url(fmt)
    except Exception:
        base_url = None
    if base_url:
        out_path = args.out / f"jurisdiction_detail_{fmt}.zip"
        print(f"\nDownloading: {base_url} -> {out_path}")
        _download(base_url, out_path)

    # Download each sub-jurisdiction report zip (if present)
    for s in subs:
        try:
            su = s.report_url(fmt)
        except Exception:
            su = None
        if not su:
            continue
        safe = "".join(ch for ch in s.name if ch.isalnum() or ch in (" ", "-", "_")).strip().replace(" ", "_")
        out_path = args.out / "subs" / f"{safe}_detail_{fmt}.zip"
        print(f"Downloading: {su} -> {out_path}")
        _download(su, out_path)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)

