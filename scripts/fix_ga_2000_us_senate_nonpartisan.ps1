param(
  [string]$AggregatePath = 'Data/ga_elections_aggregated.json',
  [string]$SourceCsvPath = 'Data/20001107__ga__general.csv'
)

function Normalize-County([string]$name) {
  if (-not $name) { return '' }
  return (($name -replace '[^A-Za-z0-9 .\-]', '') -replace '\s+', ' ').Trim().ToUpperInvariant()
}

function To-Int([object]$v) {
  $s = ("$v" -replace '[^0-9\-]', '').Trim()
  if (-not $s) { return 0 }
  try { return [int]$s } catch { return 0 }
}

function Competitiveness-Color([double]$marginPct, [string]$winner) {
  $abs = [Math]::Abs($marginPct)
  if ($winner -eq 'DEM') {
    if ($abs -ge 40) { return '#08306b' }
    if ($abs -ge 30) { return '#08519c' }
    if ($abs -ge 20) { return '#3182bd' }
    if ($abs -ge 10) { return '#6baed6' }
    if ($abs -ge 5.5) { return '#9ecae1' }
    if ($abs -ge 1) { return '#c6dbef' }
    if ($abs -ge 0.5) { return '#e1f5fe' }
    return '#bfdbfe'
  }
  if ($winner -eq 'REP') {
    if ($abs -ge 40) { return '#67000d' }
    if ($abs -ge 30) { return '#a50f15' }
    if ($abs -ge 20) { return '#cb181d' }
    if ($abs -ge 10) { return '#ef3b2c' }
    if ($abs -ge 5.5) { return '#fb6a4a' }
    if ($abs -ge 1) { return '#fcae91' }
    if ($abs -ge 0.5) { return '#fee8c8' }
    return '#fecaca'
  }
  return '#9ca3af'
}

$lines = Get-Content -Path $SourceCsvPath
$countyMap = @{}

for ($i = 1; $i -lt $lines.Count; $i++) {
  $line = $lines[$i]
  if (-not $line) { continue }
  $parts = $line.Split(',')
  if ($parts.Count -lt 6) { continue }

  $countyRaw = $parts[0]
  $office = ($parts[1]).Trim()
  $candidateRaw = $parts[4]
  $votesRaw = $parts[5]
  if ($office -ne 'U.S. Senate') { continue }

  $county = Normalize-County $countyRaw
  if (-not $county) { continue }

  if (-not $countyMap.ContainsKey($county)) {
    $countyMap[$county] = [ordered]@{ dem = 0; rep = 0; other = 0 }
  }

  $candidate = ("$candidateRaw").Trim().ToUpperInvariant()
  $votes = To-Int $votesRaw

  if ($candidate -eq 'ZELL MILLER') {
    $countyMap[$county].dem += $votes
  } elseif ($candidate -eq 'MACK MATTINGLY') {
    $countyMap[$county].rep += $votes
  } else {
    $countyMap[$county].other += $votes
  }
}

$jsonText = Get-Content -Path $AggregatePath -Raw
$data = $jsonText | ConvertFrom-Json
$results = $data.results_by_year.'2000'.us_senate.us_senate.results

$patched = 0
foreach ($countyProp in $results.PSObject.Properties) {
  $countyRaw = $countyProp.Name
  $countyNorm = Normalize-County $countyRaw
  if (-not $countyMap.ContainsKey($countyNorm)) { continue }

  $fix = $countyMap[$countyNorm]
  $dem = [int]$fix.dem
  $rep = [int]$fix.rep
  $other = [int]$fix.other
  $total = $dem + $rep + $other
  if ($total -le 0) { continue }

  $margin = $dem - $rep
  $marginPct = [Math]::Round(($margin / $total) * 100.0, 4)
  $winner = if ($margin -gt 0) { 'DEM' } elseif ($margin -lt 0) { 'REP' } else { 'TIE' }

  $node = $countyProp.Value
  $node.dem_votes = $dem
  $node.rep_votes = $rep
  $node.other_votes = $other
  $node.total_votes = $total
  $node.dem_candidate = 'Zell Miller'
  $node.rep_candidate = 'Mack Mattingly'
  $node.margin = $margin
  $node.margin_pct = $marginPct
  $node.winner = $winner
  $node.competitiveness = [ordered]@{ color = (Competitiveness-Color -marginPct $marginPct -winner $winner) }

  $patched++
}

$data | ConvertTo-Json -Depth 100 | Set-Content -Path $AggregatePath -Encoding UTF8
Write-Output "Patched counties: $patched"
