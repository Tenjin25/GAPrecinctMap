# Peach State Pulse (2000-2024)

Interactive Georgia elections atlas focused on county, precinct, congressional, and legislative results with trend-aware summaries and weighted district overlays.

This repository is deployed as a static GitHub Pages site. This README intentionally documents data, methodology, and maintenance only, and omits local startup/run instructions.

## Project Overview

Peach State Pulse is a single-page map application (`index.html`) that loads precomputed result slices and boundary data from `Data/`.

Core capabilities:

- County, congressional, state house, and state senate map views.
- Precinct overlay with polygon rendering at higher zoom and centroid fallback behavior for interaction.
- Visualization modes:
  - Margins
  - Winners
  - Shift (change vs nearest prior cycle for the same contest)
  - Flips (party control change vs nearest prior cycle)
- Context panels that show:
  - Statewide race summary for selected contest/year
  - Focused geography summary
  - Historical trend cards with consistent margin math
- Click-to-zoom interaction for county and district geographies.

UI notes:

- County hover tooltip follows the NCMap layout pattern (desktop-collapsed quickline, mobile compact → expandable details). The Close button only appears when a tooltip is pinned.
- District hover tooltips keep the original rendering system/hooks but use a more legible hierarchy and compact chips. District result bars retain a visible 50% marker.
- Tooltips can be pinned so you can pan/zoom without losing the hovered summary.
- A source badge indicates how precinct results were matched (VTD20-derived join vs fallback matching).
- Precinct centroids log match stats to the console when the overlay is active (total/matched-by-norm/id/fallback/unmatched) to help validate coverage.
- Map overlays (county/district/precinct layers) are inserted below basemap symbol layers so town/place labels remain readable and consistent.

## Geographic Layers

Primary boundaries in use:

- Counties: `Data/tl_2020_13_county20.geojson`
- Precinct/VTD base: `Data/tl_2020_13_vtd20.geojson`
- Precinct overlay polygons: `Data/Voting_Precincts.geojson`
- Precinct centroids: `Data/precinct_centroids.geojson`
- Congressional (CD118 reference): `Data/tl_2022_13_cd118.geojson`
- State House (2022 geometry): `Data/tl_2022_13_sldl.geojson`
- State Senate (2022 geometry): `Data/tl_2022_13_sldu.geojson`

Supporting metadata:

- Congressional district info: `Data/ga_congressional_districts.csv`
- State House district info: `Data/ga_state_house_districts.csv`
- State Senate district info: `Data/ga_state_senate_districts.csv`
- District descriptions: `Data/ga_district_descriptions.json`
- County demographics summary (optional): `Data/ga_county_demographics.json`
- County population estimates (CO-EST, 2020-2025): `Data/CO-EST2025-POP-13.csv`

## Election Coverage

- Time window in app experience: 2000 through 2024 (contest-dependent coverage).
- County/statewide slices: generated from OpenElections-style GA CSV exports.
- Precinct-keyed derived slices: VTD20-keyed outputs (modern join keys) for years where precinct-level source data is available.
- District overlays: produced by reallocating precinct results into district lines using weighted crosswalks.

Not every contest exists in every year/scope. Availability is controlled by manifest files and source data completeness.

## Data Products

### 1) County/Statewide Contest Slices

- Manifest: `Data/contests/manifest.json`
- Files: `Data/contests/<contest_type>_<year>.json`
- Primary builder: `build_ga_election_data.py`

Each file contains county-level rows with fields such as:

- `county`
- `dem_votes`
- `rep_votes`
- `other_votes`
- `total_votes`
- `dem_candidate`
- `rep_candidate`
- `margin`
- `margin_pct`
- `winner`
- `color`

### 2) District Contest Slices

- Manifest: `Data/district_contests/manifest.json`
- Files: `Data/district_contests/<scope>_<contest_type>_<year>.json`
- Primary builder: `scripts/build_district_contests_from_derived_vtd20.py`

Scopes:

- `congressional`
- `state_house`
- `state_senate`

Each file contains:

- `meta` (source, year, coverage, matched vote totals, input files)
- `general.results` keyed by district number

### 3) Derived Precinct/VTD20 Contest Slices

- Year manifest: `Data/derived_vtd20/<year>/contests/manifest.json`
- Files: `Data/derived_vtd20/<year>/contests/vtd20/*.json`
- Primary pipeline:
  - `scripts/batch_build_vtd20_years.py`
  - `scripts/build_vtd20_crosswalk.py`
  - `scripts/build_vtd10_key_to_vtd20_keymap.py` (pre-2020 support)
  - `scripts/build_contest_jsons.py`

File shape (high level):

- Top-level metadata: `office`, `district`, `level`, `keys`
- `results`: object keyed by VTD20 join key (typically `GEOID20` / `id`)
  - vote totals: `total_votes`, `dem_votes`, `rep_votes`, `other_votes`
  - winner fields: `winner_candidate`, `winner_party`, `winner_votes`, `margin_votes`

### 4) Precinct-to-District Crosswalks

- `Data/crosswalks/precinct_to_cd118.csv`
- `Data/crosswalks/precinct_to_2022_state_house.csv`
- `Data/crosswalks/precinct_to_2024_state_house.csv`
- `Data/crosswalks/precinct_to_2022_state_senate.csv`
- `Data/crosswalks/precinct_to_2024_state_senate.csv`

Primary builder:

- `scripts/build_precinct_district_crosswalks_from_blockassign.py`

## Selected County Popup Analysis

When a county is selected, the top-right selected popup (vote counter) includes three county-level analysis sections that are computed from data already in this repo (no external fetches):

- **Trajectory Snapshot**: labels the county’s electoral direction using the selected year result, shift since 2020, long-run shift, flip history, and streaks (contest-specific).
- **Census Check**: compares post-2020 population change (CO-EST) against the county’s vote movement to label reinforcement / realignment pressure / mixed / limited change.
- **County Census Insight**: a short, Georgia-specific plain-English growth context sentence (Atlanta core/suburban/exurban, Black Belt rural, regional metros, etc.) without claiming causation.

Georgia-specific context is assigned via a lightweight county-type classifier inside `index.html` (name-driven buckets with optional demographic refinement when available).

## Methodology

### Precinct Join Strategy

The app prioritizes stable ID-based joins and then uses normalized fallback matching only when necessary.

- Preferred key for precinct detail: VTD20 `GEOID20`/`id` (stable geography identifier).
- Supplemental key support for older years can bridge VTD10-era naming to VTD20 keys.
- Normalized county+precinct matching is used as fallback for records with inconsistent naming.

### Precinct Rendering and Centroids

Precinct interaction uses a mix of polygons and centroids:

- `Data/Voting_Precincts.geojson` provides higher-zoom precinct polygons for rendering.
- `Data/precinct_centroids.geojson` provides point features used for fast hit-testing and as a fallback when polygons are suppressed.
- Centroids are intended to be *visually inside* their polygons (not just mathematical centroids), improving hover/click behavior for thin/concave shapes.

### Overlay Allocation Strategy

District overlays are weighted reallocations, not winner-take-all assignment.

- Crosswalk rows contain `precinct_key`, `district_num`, and `area_weight`.
- A precinct can contribute votes fractionally to multiple districts.
- Current default crosswalk generation is geometry-overlap based, with optional block-assignment inputs to improve edge cases.

### Modern-Line Reference

District visualizations are aligned to modern district references used by the map:

- Congressional: CD118
- Legislative overlays: 2024-targeted house/senate crosswalk outputs

This supports consistent longitudinal comparison but means historical results are interpreted on modern line systems.

## Precision and Display Policy

To prevent visible 0.01-point drift between displayed shares and displayed lead:

- Candidate shares are rounded to two decimals for display.
- Signed and absolute margin labels are derived from those rounded shares.
- Focus cards and "Trend at a glance" use the same rounded-share basis.
- Color bins still map from margin magnitude categories for visual comparability.

Relevant app functions in `index.html`:

- `roundedVoteSharePct(...)`
- `roundedSignedMarginPctValue(...)`
- `roundedMarginPctValue(...)`
- `focusLeadMarginLabel(...)`
- `winnerMarginLabelShort(...)`

## Candidate Label and Party Normalization

Normalization logic exists in both county/statewide and contest-build pipelines.

Key behavior:

- Party tokens normalize to major buckets (`D`/`R`/other variants).
- Legacy incumbent-coded party fields like `IR`, `RI`, `ID`, `DI` are normalized.
- Candidate names are converted from all-caps source forms to regular case where needed.
- Incumbent marker `(I)` (and legacy `(R*)`) is rewritten as a trailing `*` in JSON candidate labels (e.g. `Jane Doe (I)` → `Jane Doe*`).

Primary normalization locations:

- `build_ga_election_data.py`
- `scripts/build_contest_jsons.py`

## Repository Layout

- `index.html`: complete front-end application, map logic, UI, and data loading.
- `Data/`: source CSV/GeoJSON assets and generated JSON/crosswalk outputs.

## County Population Estimates (CO-EST)

The app loads county population estimates from `Data/CO-EST2025-POP-13.csv`.

This repo keeps that file in a normalized, analysis-friendly format:

- Columns: `county, base_2020, est_2020..est_2025, change_abs, change_pct, yoy_abs, yoy_pct`
- `change_pct` is computed from `est_2020` → `est_2025`
- `yoy_pct` is computed from `est_2024` → `est_2025`

Note: the in-app parser is resilient and can also read the original Census export shape (header rows + dotted county lines), but the cleaned format is preferred for maintenance.
- `scripts/`: data prep and transformation scripts.
- `build_ga_election_data.py`: top-level statewide/county contest builder.
- Utility scripts in root:
  - `convert_to_openelections.py`
  - `download_precinct_years.py`
  - `inspect_excel.py`
  - `shp_to_geojson.py`

## Data Lineage (High Level)

1. Raw election CSVs are ingested from `Data/` (county and precinct variants by year).
2. County/statewide contest slices are built into `Data/contests`.
3. Precinct files are mapped to VTD20 keys and emitted into `Data/derived_vtd20/<year>/contests`.
4. Precinct-to-district weighted crosswalk CSVs are produced in `Data/crosswalks`.
5. District contest slices are aggregated into `Data/district_contests` from derived VTD20 outputs.
6. The front-end loads manifests and slices dynamically based on selected contest/year/view mode.

## Validation and QA Focus

Recommended checks during data refreshes:

- Manifest integrity:
  - `Data/contests/manifest.json` references existing files.
  - `Data/district_contests/manifest.json` references existing files.
- Spot-check derived VTD20 manifests:
  - `Data/derived_vtd20/<year>/contests/manifest.json` should only reference files that exist in `Data/derived_vtd20/<year>/contests/vtd20/`.
- Join coverage:
  - Use `scripts/check_join_coverage.py` to compare geometry keys vs results keys for targeted contests.
- Vote conservation:
  - Confirm district-reallocated totals match statewide input totals within expected rounding tolerance.
- Overlay spot checks:
  - Manually verify metro Atlanta and coastal/north Georgia district boundaries against known patterns.
- Precision consistency:
  - Validate displayed shares and lead labels are internally consistent in county, statewide, and trend cards.

## Known Constraints and Caveats

- Historical races are viewed through modern boundary references for district overlays.
- Some source files include non-geographic buckets (for example absentee/provisional-style records) that may count in totals but not map cleanly to a polygon.
- Precinct naming variability across years/files can still produce edge-case fallback mismatches.
- Contest availability by year depends on source completeness and may vary by office/scope.

## Maintenance Guide (No Runtime Steps)

When updating the project for new source files or corrected joins:

1. Normalize/prepare boundary inputs and precinct geometry products.
2. Rebuild county/statewide contest slices.
3. Rebuild VTD20-derived precinct contest slices for affected years/offices.
4. Rebuild precinct-to-district crosswalks if boundary assumptions changed.
5. Rebuild district contest slices from updated derived VTD20 outputs.
6. Validate manifest entries, join coverage, and representative county/district spot checks.
7. Commit regenerated artifacts and script changes together so provenance is clear.

## Deployment Note

GitHub Pages serves this project as static assets. Deployment/runtime hosting is handled by Pages, so this README deliberately excludes local run instructions.

## Basemap Notes

This app uses Mapbox Light (`mapbox://styles/mapbox/light-v11`) and matches the NCMap basemap runtime settings:

- Mapbox telemetry is disabled (to reduce ad-blocker console noise).
- `preserveDrawingBuffer: true` and `performanceMetricsCollection: false` are set on the map instance.
