"""Build 2024-to-2022 Georgia district presidential remap weights.

Georgia's SOS precinct export pools many advance-voting locations across
multiple ballot styles.  The complete 2024 district totals can therefore be
recovered from the matching district-contest buckets, but precinct-name matching
cannot reliably place every pooled vote on the 2022 House map.

This script overlays the New York Times' public 2024 presidential precinct
geography with Georgia's 2022 and 2024 House, Senate, or congressional maps.
Districts whose same-number polygons overlap by at least 99 percent are treated
as unchanged.  For the redrawn districts, it writes party-specific
2024-district -> 2022-district weights.  The repair script applies those weights
to the complete SOS 2024 district totals, so the final allocation conserves
every official district vote.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import tempfile
import urllib.request
from collections import defaultdict
from pathlib import Path

from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform
from shapely.strtree import STRtree
from shapely.validation import make_valid


ROOT = Path(__file__).resolve().parents[1]
SCOPE_CONFIGS = {
    "state_house": {
        "label": "State House",
        "map_2022": ROOT / "Data" / "tl_2022_13_sldl.geojson",
        "map_2024": ROOT / "Data" / "tl_2024_13_sldl.geojson",
        "id_2022": "SLDLST",
        "id_2024": "SLDLST",
        "district_count": 180,
        "output": ROOT / "Data" / "crosswalks" / "state_house_2024_to_2022_president_2024_weights.json",
    },
    "state_senate": {
        "label": "State Senate",
        "map_2022": ROOT / "Data" / "tl_2022_13_sldu.geojson",
        "map_2024": ROOT / "Data" / "tl_2024_13_sldu.geojson",
        "id_2022": "SLDUST",
        "id_2024": "SLDUST",
        "district_count": 56,
        "output": ROOT / "Data" / "crosswalks" / "state_senate_2024_to_2022_president_2024_weights.json",
    },
    "congressional": {
        "label": "Congressional",
        "map_2022": ROOT / "Data" / "tl_2022_13_cd118.geojson",
        "map_2024": ROOT / "Data" / "tl_2024_13_cd119.geojson",
        "id_2022": "CD118FP",
        "id_2024": "CD119FP",
        "district_count": 14,
        "output": ROOT / "Data" / "crosswalks" / "congressional_2024_to_2022_president_2024_weights.json",
    },
}
NYT_URL = (
    "https://int.nyt.com/newsgraphics/elections/map-data/2024/national/"
    "GA-precincts-with-results.geojson.gz"
)
NYT_CACHE_NAME = "GA-precincts-with-results.geojson.gz"
UNCHANGED_IOU_THRESHOLD = 0.99
FIELDS = {
    "total_votes": "votes_total",
    "dem_votes": "votes_dem",
    "rep_votes": "votes_rep",
    "other_votes": "other_votes",
}


def project_geometry(geometry: object, transformer: Transformer) -> object:
    return make_valid(transform(transformer.transform, geometry))


def load_districts(
    path: Path,
    id_property: str,
    district_count: int,
    transformer: Transformer,
) -> dict[int, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    districts: dict[int, object] = {}
    for feature in payload.get("features", []):
        props = feature.get("properties") or {}
        district = int(str(props.get(id_property, "0")))
        districts[district] = project_geometry(shape(feature["geometry"]), transformer)
    expected = set(range(1, district_count + 1))
    if set(districts) != expected:
        missing = sorted(expected - set(districts))
        raise RuntimeError(f"Missing House districts in {path}: {missing}")
    return districts


def resolve_nyt_source(explicit: Path | None) -> Path:
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(explicit)
    env_path = os.environ.get("GA_NYT_2024_PRECINCTS", "").strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(
        [
            ROOT / "Data" / "sources" / NYT_CACHE_NAME,
            Path(tempfile.gettempdir()) / NYT_CACHE_NAME,
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate

    target = Path(tempfile.gettempdir()) / NYT_CACHE_NAME
    print(f"Downloading {NYT_URL}")
    urllib.request.urlretrieve(NYT_URL, target)
    return target


def load_nyt_geojson(path: Path) -> dict[str, object]:
    if path.suffix.lower() == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalized_weights(values: dict[int, float], fallback: dict[int, float]) -> dict[str, float]:
    usable = {district: value for district, value in values.items() if value > 1e-12}
    total = sum(usable.values())
    if total <= 0:
        usable = {district: value for district, value in fallback.items() if value > 1e-12}
        total = sum(usable.values())
    if total <= 0:
        raise RuntimeError("Cannot normalize an empty district remap")
    return {
        str(district): value / total
        for district, value in sorted(usable.items())
    }


def build_weights(nyt_source: Path, scope: str) -> dict[str, object]:
    config = SCOPE_CONFIGS[scope]
    district_count = int(config["district_count"])
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:5070", always_xy=True)
    old = load_districts(
        config["map_2022"],
        str(config["id_2022"]),
        district_count,
        transformer,
    )
    current = load_districts(
        config["map_2024"],
        str(config["id_2024"]),
        district_count,
        transformer,
    )

    unchanged: set[int] = set()
    overlap_iou: dict[str, float] = {}
    for district in range(1, district_count + 1):
        intersection = old[district].intersection(current[district]).area
        union = old[district].union(current[district]).area
        iou = intersection / union if union else 0.0
        overlap_iou[str(district)] = iou
        if iou >= UNCHANGED_IOU_THRESHOLD:
            unchanged.add(district)
    changed = sorted(set(range(1, district_count + 1)) - unchanged)

    current_ids = list(range(1, district_count + 1))
    current_geometries = [current[district] for district in current_ids]
    current_tree = STRtree(current_geometries)
    old_changed_geometries = [old[district] for district in changed]
    old_changed_tree = STRtree(old_changed_geometries)

    raw: dict[int, dict[str, dict[int, float]]] = {
        district: {
            field: defaultdict(float)
            for field in FIELDS
        }
        for district in changed
    }
    source_totals = {field: 0 for field in FIELDS}
    official_boundaries = 0
    features = load_nyt_geojson(nyt_source).get("features", [])

    for number, feature in enumerate(features, start=1):
        props = feature.get("properties") or {}
        dem = int(props.get("votes_dem") or 0)
        rep = int(props.get("votes_rep") or 0)
        total = int(props.get("votes_total") or 0)
        values = {
            "total_votes": total,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": max(0, total - dem - rep),
        }
        for field, value in values.items():
            source_totals[field] += value
        if props.get("official_boundary") is True:
            official_boundaries += 1

        precinct = project_geometry(shape(feature["geometry"]), transformer)
        if precinct.is_empty or precinct.area <= 0:
            continue

        current_parts: list[tuple[int, object, float]] = []
        for tree_index in current_tree.query(precinct):
            index = int(tree_index)
            district = current_ids[index]
            fragment = precinct.intersection(current_geometries[index])
            if not fragment.is_empty and fragment.area > 0:
                current_parts.append((district, fragment, fragment.area))
        current_coverage = sum(area for _, _, area in current_parts)
        if current_coverage <= 0:
            continue

        for current_district, current_fragment, current_area in current_parts:
            if current_district in unchanged:
                continue
            old_parts: list[tuple[int, float]] = []
            for tree_index in old_changed_tree.query(current_fragment):
                index = int(tree_index)
                old_district = changed[index]
                triple = current_fragment.intersection(old_changed_geometries[index])
                if not triple.is_empty and triple.area > 0:
                    old_parts.append((old_district, triple.area))
            old_coverage = sum(area for _, area in old_parts)
            if old_coverage <= 0:
                continue

            current_share = current_area / current_coverage
            for old_district, triple_area in old_parts:
                share = current_share * triple_area / old_coverage
                for field, value in values.items():
                    raw[current_district][field][old_district] += value * share

        if number % 500 == 0:
            print(f"Processed {number:,} NYT precinct features")

    weights: dict[str, dict[str, dict[str, float]]] = {}
    for current_district in changed:
        total_fallback = raw[current_district]["total_votes"]
        weights[str(current_district)] = {
            field: normalized_weights(raw[current_district][field], total_fallback)
            for field in FIELDS
        }

    return {
        "meta": {
            "source": "New York Times 2024 presidential precinct results and geography",
            "source_url": NYT_URL,
            "source_sha256": sha256_file(nyt_source),
            "scope": scope,
            "method": (
                "Area overlay of NYT precinct geography with the 2024 and 2022 Georgia "
                f"{config['label']} maps; party-specific weights are constrained to complete "
                f"SOS 2024 {config['label']} district totals when applied"
            ),
            "generated_by": "scripts/build_2024_state_house_remap_weights.py",
            "district_map_2022": str(config["map_2022"].relative_to(ROOT)).replace("\\", "/"),
            "district_map_2024": str(config["map_2024"].relative_to(ROOT)).replace("\\", "/"),
            "unchanged_iou_threshold": UNCHANGED_IOU_THRESHOLD,
            "unchanged_districts": sorted(unchanged),
            "changed_districts": changed,
            "same_number_iou": overlap_iou,
            "source_features": len(features),
            "official_boundary_features": official_boundaries,
            "source_totals": source_totals,
        },
        "weights": weights,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--nyt-source",
        type=Path,
        default=None,
        help="Optional local NYT Georgia GeoJSON or .geojson.gz source",
    )
    parser.add_argument("--scope", choices=sorted(SCOPE_CONFIGS), default="state_house")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    source = resolve_nyt_source(args.nyt_source)
    config = SCOPE_CONFIGS[args.scope]
    output = args.output or config["output"]
    print(f"Using {source}")
    payload = build_weights(source, args.scope)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {output}")
    print(f"Unchanged districts: {len(payload['meta']['unchanged_districts'])}")
    print(f"Spatially remapped districts: {len(payload['meta']['changed_districts'])}")


if __name__ == "__main__":
    main()
