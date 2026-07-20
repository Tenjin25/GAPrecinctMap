"""
Compare blockpath vs production (shortcut) vote-mass coverage for key contests.

Reports VTD20 contest output totals and state_house district rollup totals.

Usage:
  py scripts/compare_blockpath_vs_shortcut.py --years 2012,2014,2016,2018
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


KEY_SLUGS = (
    "President",
    "President_of_the_United_States",
    "Governor",
    "Lieutenant_Governor",
    "Secretary_Of_State",
    "Secretary_of_State",
    "Attorney_General",
    "Commissioner_Of_Agriculture",
    "Commissioner_of_Agriculture",
    "Commissioner_Of_Insurance",
    "Commissioner_of_Insurance",
    "Commissioner_Of_Labor",
    "Commissioner_of_Labor",
    "State_School_Superintendent",
    "STATE_SCHOOL_SUPERINTENDENT",
    "United_States_Senator",
    "U.S._Senate",
    "U.S._Senate__Special",
    "U.S._Senate_Special",
)


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _contest_coverage(manifest: dict, slug: str) -> dict | None:
    for row in manifest.get("coverage") or []:
        if row.get("slug") == slug:
            return row
    for row in manifest.get("contests") or []:
        if row.get("slug") == slug:
            cov = row.get("coverage")
            if isinstance(cov, dict):
                return {"slug": slug, **cov}
            # Shortcut path may only have output row counts.
            out = ((row.get("outputs") or {}).get("vtd20") or {})
            return {
                "slug": slug,
                "output_vtd20": out.get("rows"),
                "source_votes": None,
                "output_votes": None,
            }
    return None


def _sum_contest_votes(path: Path) -> int | None:
    if not path.exists():
        return None
    payload = _load_json(path)
    results = payload.get("results") or {}
    return int(sum(int((r or {}).get("total_votes") or 0) for r in results.values()))


def _district_total(path: Path) -> int | None:
    if not path.exists():
        return None
    payload = _load_json(path)
    meta = payload.get("meta") or {}
    if meta.get("matched_input_votes") is not None:
        return int(meta["matched_input_votes"])
    general = payload.get("general") or {}
    results = general.get("results") if isinstance(general, dict) else None
    if isinstance(results, dict):
        return int(sum(int((d or {}).get("total_votes") or 0) for d in results.values()))
    districts = payload.get("districts") or {}
    if isinstance(districts, dict):
        return int(sum(int((d or {}).get("total_votes") or 0) for d in districts.values()))
    return None


def _find_key_slugs(manifest: dict) -> list[str]:
    found = []
    for slug in KEY_SLUGS:
        if _contest_coverage(manifest, slug) is not None:
            found.append(slug)
    return found


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--years", default="2012,2014,2016,2018,2020,2022,2024")
    ap.add_argument("--blockpath-base", type=Path, default=Path("Data/derived_vtd20_blockpath"))
    ap.add_argument("--prod-base", type=Path, default=Path("Data/derived_vtd20"))
    ap.add_argument("--district-blockpath", type=Path, default=Path("Data/district_contests_blockpath"))
    ap.add_argument("--district-prod", type=Path, default=Path("Data/district_contests_2024"))
    ap.add_argument("--out", type=Path, default=Path("Data/derived_vtd20_blockpath/coverage_compare.json"))
    args = ap.parse_args()

    years = [y.strip() for y in args.years.split(",") if y.strip()]
    report: dict = {"years": {}}

    print(
        f"{'year':<6} {'contest':<34} {'src':>10} {'bp_out':>10} {'prod_out':>10} "
        f"{'bp%':>7} {'prod%':>7} {'bp-prod':>10} {'sh_bp':>10} {'sh_prod':>10} {'sh_d':>10}"
    )

    for year in years:
        bp_man = _load_json(args.blockpath_base / year / "contests" / "manifest.json")
        prod_man = _load_json(args.prod_base / year / "contests" / "manifest.json")
        if not bp_man and not prod_man:
            print(f"{year:<6} (missing both manifests)")
            continue

        slugs = _find_key_slugs(bp_man) or _find_key_slugs(prod_man)
        year_rows = []
        for slug in slugs:
            bp = _contest_coverage(bp_man, slug) or {}
            prod = _contest_coverage(prod_man, slug) or {}

            bp_path = args.blockpath_base / year / "contests" / "vtd20" / f"{slug}.json"
            prod_path = args.prod_base / year / "contests" / "vtd20" / f"{slug}.json"
            bp_votes = bp.get("output_votes")
            if bp_votes is None:
                bp_votes = _sum_contest_votes(bp_path)
            prod_votes = prod.get("output_votes")
            if prod_votes is None:
                prod_votes = _sum_contest_votes(prod_path)

            src = bp.get("source_votes")
            if src is None:
                # Infer source from prod file party totals if needed.
                src = prod_votes

            bp_pct = (100.0 * bp_votes / src) if src and bp_votes is not None else None
            prod_pct = (100.0 * prod_votes / src) if src and prod_votes is not None else None
            delta = (bp_votes - prod_votes) if bp_votes is not None and prod_votes is not None else None

            # Map contest slug to district rollup filename stem.
            contest_type = {
                "President": "president",
                "President_of_the_United_States": "president",
                "Governor": "governor",
                "Lieutenant_Governor": "lieutenant_governor",
                "Secretary_Of_State": "secretary_of_state",
                "Secretary_of_State": "secretary_of_state",
                "Attorney_General": "attorney_general",
                "Commissioner_Of_Agriculture": "agriculture_commissioner",
                "Commissioner_of_Agriculture": "agriculture_commissioner",
                "Commissioner_Of_Insurance": "insurance_commissioner",
                "Commissioner_of_Insurance": "insurance_commissioner",
                "Commissioner_Of_Labor": "labor_commissioner",
                "Commissioner_of_Labor": "labor_commissioner",
                "State_School_Superintendent": "superintendent",
                "STATE_SCHOOL_SUPERINTENDENT": "superintendent",
                "United_States_Senator": "us_senate",
                "U.S._Senate": "us_senate",
                "U.S._Senate__Special": "us_senate_special",
                "U.S._Senate_Special": "us_senate_special",
            }.get(slug, "")
            sh_bp = sh_prod = sh_delta = None
            if contest_type:
                sh_bp = _district_total(args.district_blockpath / f"state_house_{contest_type}_{year}.json")
                sh_prod = _district_total(args.district_prod / f"state_house_{contest_type}_{year}.json")
                if sh_bp is not None and sh_prod is not None:
                    sh_delta = sh_bp - sh_prod

            row = {
                "slug": slug,
                "source_votes": src,
                "blockpath_output_votes": bp_votes,
                "prod_output_votes": prod_votes,
                "blockpath_pct": bp_pct,
                "prod_pct": prod_pct,
                "blockpath_minus_prod": delta,
                "state_house_blockpath": sh_bp,
                "state_house_prod": sh_prod,
                "state_house_delta": sh_delta,
                "blockpath_matched_vtd10": bp.get("matched_vtd10_votes"),
                "blockpath_fallback": bp.get("fallback_vtd20_votes"),
                "blockpath_dropped": bp.get("dropped_votes"),
            }
            year_rows.append(row)

            def _fmt(v, w=10):
                if v is None:
                    return f"{'-':>{w}}"
                if isinstance(v, float):
                    return f"{v:>{w}.1f}"
                return f"{v:>{w},}"

            print(
                f"{year:<6} {slug:<34} {_fmt(src)} {_fmt(bp_votes)} {_fmt(prod_votes)} "
                f"{_fmt(bp_pct, 7)} {_fmt(prod_pct, 7)} {_fmt(delta)} "
                f"{_fmt(sh_bp)} {_fmt(sh_prod)} {_fmt(sh_delta)}"
            )

        report["years"][year] = year_rows

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
