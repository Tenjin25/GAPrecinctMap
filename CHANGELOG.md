# Change history

## 2026-11-05

- Add preliminary November 3, 2026 Georgia general and special election results from the Secretary of State election-night export.
- Preserve precinct identifiers in a dedicated lookup while keeping the results CSV in seven-column OpenElections format.
- Add 2026 county aggregates for the statewide contests and label the source as preliminary pending certified results.
- Add preliminary 2026 statewide-contest aggregates for all 14 congressional, 180 State House, and 56 State Senate districts using direct same-election precinct ballot buckets.
- Add 2022-lines congressional, State House, and State Senate versions of the 2026 statewide contests using the repository's constrained old-lines remap weights; actual U.S. House election contests remain excluded from that old-lines remap.
- Add the directly reported 2026 U.S. House elections to the current-lines congressional layer and expose them in the congressional contest picker; keep them out of the 2022-lines remap.
- Remove incumbent asterisks from generated candidate names; incumbency markers are treated as source metadata rather than part of the display name.

## 2026-10-06

- Unlock the contest selector as soon as its manifest is ready instead of waiting for optional district geometry and demographic files.
- Begin fetching the contest manifest immediately instead of waiting for map-idle and browser-idle delays.
- Automatically collapse the mobile legend after selected election results finish loading.
- Start atlas hydration as soon as the Mapbox map is ready, avoiding mobile delays caused by waiting for every window asset.
- Preserve two-decimal margin precision for close results and competitiveness thresholds (for example, 0.45%).
- Refresh application and data cache tokens, plus shared-module asset tokens where present; repair shared module loading where needed.

## March–October 2026

**Last updated:** October 4, 2026

### Consistent District Demographic Pill Sizing (October 4, 2026)

- Applied the populated Demographics-mode compact pill styling to district demographic cards before election results load.
- Preserved the original natural popup dimensions and complete highest-to-lowest demographic layout; only the pill scale and spacing are unified.

### Descending Demographic Percentages (October 4, 2026)

- Sorted demographic race/ethnicity percentages from highest to lowest across the atlas's demographic cards.
- Kept unavailable Hispanic data at the end of the list so known percentages remain easy to scan.
