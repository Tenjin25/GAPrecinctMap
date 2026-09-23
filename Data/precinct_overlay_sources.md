# Dated Georgia precinct overlays

The map uses the precinct boundary vintage for each supported election year. Shapes are matched to precinct result rows by exact name, unique precinct code, reviewed alias, or a conservative unique name match. Unmatched shapes remain uncolored so the map does not invent or duplicate votes.

| Year | Boundary file | Election results | Matched shapes |
| --- | --- | --- | ---: |
| 2012 | `voting-precinct-2012.zip` | `20121106__ga__general__precinct.csv` | 2,810 / 2,815 |
| 2014 | `vtd2014-shape.zip` | `20141104__ga__general__precinct.csv` | 2,564 / 2,729 |
| 2016 | `vtd2016-shape.zip` | `20161108__ga__general__precinct.csv` | 2,497 / 2,664 |
| 2018 | `vtd2018-shapefile.zip` | `20181106__ga__general__precinct.csv` | 2,490 / 2,630 |
| 2020 | `vtd2020-shape-4-20-21.zip` | `20201103__ga__general__precinct.csv` | 2,621 / 2,654 |
| 2022 | `ga-precincts2022-shape.zip` | `20221108__ga__general__official__precinct.csv` | 2,710 / 2,711 |
| 2024 | `Data/_external/gaprec_2024-website-shapefile.zip` | `20241105__ga__general__precinct-level.csv` | 2,673 / 2,697 |

The 2024 boundary source is the [Georgia General Assembly precinct shapefile](https://www.legis.ga.gov/api/document/docs/default-source/reapportionment-document-library/precincts/gaprec_2024-website-shapefile.zip?sfvrsn=7b38ca99_2). The shapefile archives and election exports for older years were already present in this project. The 2022 official export supplies statewide Gwinnett results that are absent from the project's other 2022 export.

`precinct_<year>_match_audit.csv` records each match method or an unmatched shape. `precinct_2024_aliases.csv` contains manually reviewed 2024 source names. `precinct_friendly_names_latest.json` keeps useful county/code to label mappings, preferring the latest boundary name and expanding unambiguous abbreviations. Case-only repeats are omitted. The dated polygon and centroid files retain friendly labels even when a mapping is omitted.

Different precinct codes may intentionally share a polling-place label. Those separate code mappings are retained.

Precinct CVAP is rebuilt for every dated boundary set in `cvap_aggregates/precinct_shapes_<year>__cvap24.csv`. The source is `ga_cvap_2024_2020_b_csv.zip` on 2020 Census blocks. Each block's Census internal point assigns its full 2020–2024 ACS CVAP estimate to one precinct in the same county; 170–318 blocks per year required nearest-precinct assignment because their points fell outside labeled precinct shapes. This preserves each county's sum of block estimates, but a block divided by a precinct boundary is allocated as one unit. Independently published county CVAP estimates can differ slightly from sums of rounded blocks. The map uses these dated CVAP rows for precinct demographic colors and totals. The legacy 2020 precinct VAP colors are used only with the legacy geometry.

Rebuild with `py scripts/build_2024_precinct_overlay.py`, `py scripts/build_historical_precinct_overlays.py`, and `py scripts/rebuild_precinct_cvap.py` from the repository root. The 2024 builder needs the General Assembly ZIP at `Data/_external/gaprec_2024-website-shapefile.zip`.
