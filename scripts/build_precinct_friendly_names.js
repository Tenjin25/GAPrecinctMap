/* eslint-disable no-console */
/**
 * Build Data/precinct_friendly_names.json for GAPrecinctMap.
 *
 * Mirrors NCPrecinctMap/scripts/build_precinct_friendly_names.js:
 *   county -> { canonical_prec_id -> display name }
 *
 * Sources (later wins on ties via scoring / overrides):
 *   1. Alias-index name extraction
 *   2. Voting_Precincts.geojson prec_id when it looks like a real name
 *   3. Data/crosswalks/precinct_name_aliases.json seed (code -> expanded name)
 *
 * Usage:
 *   node scripts/build_precinct_friendly_names.js
 *   node scripts/build_precinct_friendly_names.js [aliasIndex] [out] [votingGeojson] [seedAliases]
 */
const fs = require('fs');
const path = require('path');

function toTitleCaseName(raw) {
  const s = String(raw || '').trim().toLowerCase();
  if (!s) return '';
  return s.replace(/\b([a-z])/g, (m, c) => c.toUpperCase());
}

function splitGluedDirectionSuffix(raw) {
  const s = String(raw || '').trim();
  if (!s) return '';
  if (/^(north|south)(east|west)$/i.test(s)) return s;
  if (/^(east|west|north|south|central)$/i.test(s)) return s;
  return s.replace(
    /\b([A-Za-z]{3,}?)(east|west|north|south|central)\b/gi,
    (full, place, dir) => {
      const f = String(full || '').toLowerCase();
      if (['northeast', 'northwest', 'southeast', 'southwest'].includes(f)) return full;
      if (['east', 'west', 'north', 'south', 'central'].includes(f)) return full;
      return `${place} ${dir}`;
    }
  );
}

function formatDisplayName(raw) {
  let s = String(raw || '').trim();
  if (!s) return '';
  s = s.replace(/([a-z])([A-Z])/g, (full, a, b, offset, str) => {
    const lead2 = str.slice(Math.max(0, offset - 1), offset + 1);
    const lead3 = str.slice(Math.max(0, offset - 2), offset + 1);
    if (/mc$/i.test(lead2) || /mac$/i.test(lead3) || /o'$/i.test(lead2)) return full;
    return `${a} ${b}`;
  });
  s = splitGluedDirectionSuffix(s);
  s = toTitleCaseName(s);
  if (!s) return '';
  s = splitGluedDirectionSuffix(s);
  s = s.replace(/'S\b/g, "'s");
  s = s.replace(/\bMc([a-z])/g, (m, c) => `Mc${c.toUpperCase()}`);
  s = s.replace(/\bSt (?=[A-Z])/g, 'St. ');
  s = s.replace(/\bMt (?=[A-Z])/g, 'Mt. ');
  s = s.replace(/\bNw\b/g, 'NW');
  s = s.replace(/\bNe\b/g, 'NE');
  s = s.replace(/\bSe\b/g, 'SE');
  s = s.replace(/\bSw\b/g, 'SW');
  s = s.replace(/\b([A-Za-z])\.([A-Za-z])\./g, (m, a, b) => `${a.toUpperCase()}.${b.toUpperCase()}.`);
  s = s.replace(/\b([A-Za-z]{4,})-([A-Za-z])\b/g, '$1 $2');
  s = s.replace(/\s+/g, ' ').trim();
  s = s.replace(/\bAme\b/g, 'AME');
  s = s.replace(/\bCme\b/g, 'CME');
  s = s.replace(/\bUmc\b/g, 'UMC');
  s = s.replace(/\bVfd\b/g, 'VFD');
  s = s.replace(/\bIi\b/g, 'II');
  s = s.replace(/\bIii\b/g, 'III');
  s = s.replace(/\bIv\b/g, 'IV');
  s = s.replace(/\bElem\b/g, 'Elem');
  return s;
}

function normalizeAliasNameCandidate(raw) {
  const s = String(raw || '').trim().toUpperCase();
  if (!s) return '';
  const cleaned = s.replace(/[_]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (!cleaned) return '';
  if (/VOTING\s*DISTRICT/i.test(cleaned)) return '';
  if (/^\d+$/.test(cleaned)) return '';
  return cleaned;
}

function collapseRedundantLeadingToken(raw) {
  const cleaned = String(raw || '').replace(/\s+/g, ' ').trim();
  if (!cleaned) return '';
  const parts = cleaned.split(/\s+/).filter(Boolean);
  if (parts.length < 2) return cleaned;
  const first = parts[0].replace(/[^A-Za-z0-9]/g, '').toUpperCase();
  const second = parts[1].replace(/[^A-Za-z0-9]/g, '').toUpperCase();
  if (first && first === second) return parts.slice(1).join(' ').trim();
  return cleaned;
}

function collapseRedundantCodePrefix(nameRaw, codeRaw) {
  const cleaned = String(nameRaw || '').replace(/\s+/g, ' ').trim();
  const code = String(codeRaw || '').trim().toUpperCase();
  if (!cleaned || !code) return cleaned;
  const codeParts = code
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  const compareTokens = cleaned
    .replace(/\./g, ' ')
    .replace(/-/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(/\s+/)
    .filter(Boolean);

  if (codeParts.length && compareTokens.length > codeParts.length) {
    let matches = true;
    for (let i = 0; i < codeParts.length; i += 1) {
      if (compareTokens[i] !== codeParts[i]) {
        matches = false;
        break;
      }
    }
    if (matches) {
      const rest = compareTokens.slice(codeParts.length).join(' ').trim();
      if (rest) return collapseRedundantLeadingToken(rest);
    }
  }
  return collapseRedundantLeadingToken(cleaned);
}

function isCodeLikeToken(raw) {
  const s = String(raw || '').trim().toUpperCase();
  if (!s) return true;
  const compact = s.replace(/[^A-Z0-9]/g, '');
  if (!compact) return true;
  if (/[0-9]/.test(compact)) return true;
  if (compact.length <= 4 && /^[A-Z]+$/.test(compact)) return true;
  return false;
}

/** True for VTD/code labels (02A, SC16A, 208) — false for place names that happen to include digits (Harrison 01). */
function looksLikeCodeOnlyLabel(raw) {
  const s = String(raw || '').trim().toUpperCase();
  if (!s) return true;
  const compact = s.replace(/[^A-Z0-9]/g, '');
  if (!compact) return true;
  if (/^\d+$/.test(compact)) return true;
  // Compact letter+digit codes without separators: 02A, 1B, 3A1, SC16A, CP053
  if (!/[\s/\-]/.test(s)) {
    if (/^\d+[A-Z]{0,3}$/.test(compact)) return true;
    if (/^[A-Z]{1,3}\d+[A-Z0-9]*$/.test(compact)) return true;
    // 1–2 letter abbreviations only (BA); keep 3–4 letter place names (LULA, FORK).
    if (/^[A-Z]+$/.test(compact) && compact.length <= 2) return true;
  }
  return false;
}

function normalizeCounty(raw) {
  return String(raw || '').trim().toUpperCase();
}

function normalizeCode(raw) {
  return String(raw || '').trim().toUpperCase();
}

function scoreNameCandidate(raw) {
  const s = normalizeAliasNameCandidate(raw);
  if (!s) return -1e9;
  const letters = (s.match(/[A-Z]/g) || []).length;
  const digits = (s.match(/[0-9]/g) || []).length;
  const spaces = (s.match(/\s/g) || []).length;
  let score = 0;
  score += letters * 2.2;
  score -= digits * 3.5;
  score += spaces * 6.0;
  score += Math.min(24, s.length);
  if (/VOTING\s*DISTRICT/i.test(s)) score -= 1000;
  if (/^(EARLY|ABSENTEE|PROVISIONAL|ONE\s+STOP|MAIL)/i.test(s)) score -= 20;
  if (!spaces && letters >= 10) score -= 8;
  return score;
}

function splitCompactDirectionName(raw) {
  const s = String(raw || '').trim().toUpperCase().replace(/[^A-Z0-9]+/g, '');
  if (!s) return '';
  const m = s.match(/^(.*?)(NORTH|SOUTH|EAST|WEST|CENTRAL)$/);
  if (!m || !m[1] || m[1].length < 3) return s;
  const full = s.toLowerCase();
  if (['northeast', 'northwest', 'southeast', 'southwest'].includes(full)) return s;
  return `${m[1]} ${m[2]}`;
}

function isUsableFriendlyCandidate(raw, codeRaw = '') {
  const s = normalizeAliasNameCandidate(raw);
  if (!s) return false;
  if (/\s-\s/.test(s) || s.includes(' - ')) return false;
  if (/^[A-Z ]+\s-\s/.test(s)) return false;
  // Reject compact smash tokens (ABUNDANTLIFEABUNDANTLIFEFELLOWSHIP).
  if (!/\s/.test(s) && s.replace(/[^A-Z0-9]/g, '').length >= 18) return false;
  const code = normalizeCode(codeRaw);
  if (code) {
    const codeCompact = code.replace(/[^A-Z0-9]/g, '');
    const nameCompact = s.replace(/[^A-Z0-9]/g, '');
    // Reject exact doubled names: NAME+NAME
    if (codeCompact && nameCompact === codeCompact + codeCompact) return false;
  }
  return true;
}

function extractNameFromAlias(aliasRaw, codeRaw) {
  const alias = String(aliasRaw || '').trim().toUpperCase();
  const code = String(codeRaw || '').trim().toUpperCase();
  if (!alias || !code) return '';
  if (alias === code) return '';
  if (alias.includes(' - ')) return '';

  const codeCompact = code.replace(/[^A-Z0-9]/g, '');
  const aliasCompact = alias.replace(/[^A-Z0-9]/g, '');

  let rest = '';
  if (alias.startsWith(code)) {
    const after = alias.slice(code.length);
    if (/^[\s_-]+/.test(after)) {
      rest = after.replace(/^[_\s-]+/, '').trim();
    }
  }
  if (
    !rest &&
    codeCompact.length >= 2 &&
    aliasCompact.startsWith(codeCompact) &&
    aliasCompact.length > codeCompact.length + 2
  ) {
    const remainder = aliasCompact.slice(codeCompact.length);
    if (/(NORTH|SOUTH|EAST|WEST|CENTRAL)$/.test(remainder) && remainder.length >= 5) {
      rest = splitCompactDirectionName(remainder);
    }
  }
  if (!rest) return '';
  rest = rest.replace(/[_]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (!rest) return '';
  if (/VOTING\s*DISTRICT/i.test(rest)) return '';

  const restCompact = rest.replace(/[^A-Z0-9]/g, '');
  if (restCompact === codeCompact) return '';
  if (restCompact.endsWith(codeCompact) && restCompact.length <= codeCompact.length + 2) return '';

  if (!/\s/.test(rest)) {
    const compactOnly = rest.replace(/[^A-Z0-9]/g, '');
    if (compactOnly.length <= 6 && /^[A-Z0-9]+$/.test(compactOnly)) return '';
    const split = splitCompactDirectionName(compactOnly);
    if (split && /\s/.test(split)) rest = split;
  }
  const cleaned = normalizeAliasNameCandidate(rest);
  if (!isUsableFriendlyCandidate(cleaned, code)) return '';
  return cleaned;
}

function setBestNameForCode(perCounty, code, nameCandidate) {
  if (!perCounty || !code || !nameCandidate) return;
  const cand = collapseRedundantCodePrefix(nameCandidate, code);
  if (!cand) return;
  const prev = perCounty.get(code) || '';
  if (!prev) {
    perCounty.set(code, cand);
    return;
  }
  const prevNorm = collapseRedundantCodePrefix(prev, code) || prev;
  const prevScore = scoreNameCandidate(prevNorm);
  const candScore = scoreNameCandidate(cand);
  const prevCompact = prevNorm.replace(/[^A-Z0-9]/g, '');
  const candCompact = cand.replace(/[^A-Z0-9]/g, '');
  if (candCompact === prevCompact && cand.length <= prevNorm.length && candScore + 1e-6 >= prevScore) {
    perCounty.set(code, cand);
    return;
  }
  if (candScore > prevScore + 1e-6) {
    perCounty.set(code, cand);
    return;
  }
  if (Math.abs(candScore - prevScore) < 1e-6 && cand.length > prevNorm.length) {
    perCounty.set(code, cand);
  }
}

function buildFriendlyNamesIndex(aliasIndexPayload) {
  const counties = aliasIndexPayload?.counties || {};
  const out = {};

  for (const [countyRaw, aliasObj] of Object.entries(counties)) {
    if (!aliasObj || typeof aliasObj !== 'object') continue;
    const perCounty = new Map();

    for (const [aliasRaw, codesRaw] of Object.entries(aliasObj)) {
      const alias = String(aliasRaw || '').trim().toUpperCase();
      const codes = Array.isArray(codesRaw)
        ? Array.from(new Set(codesRaw.map(v => String(v || '').trim().toUpperCase()).filter(Boolean)))
        : [];
      if (!alias || !codes.length) continue;

      for (const code of codes) {
        const extracted = extractNameFromAlias(alias, code);
        if (extracted) {
          setBestNameForCode(perCounty, code, extracted);
          continue;
        }
        if (
          codes.length === 1 &&
          !isCodeLikeToken(alias) &&
          !alias.startsWith(String(code || '').trim().toUpperCase()) &&
          isUsableFriendlyCandidate(alias, code)
        ) {
          setBestNameForCode(perCounty, code, alias);
        }
      }
    }

    if (!perCounty.size) continue;
    const outCounty = {};
    for (const [code, name] of perCounty.entries()) {
      outCounty[code] = formatDisplayName(name);
    }
    out[countyRaw] = outCounty;
  }

  return out;
}

function normalizeDirectGeoName(raw) {
  const s = String(raw || '').trim().toUpperCase();
  if (!s) return '';
  const cleaned = s
    .replace(/VOTING\s*DISTRICT/gi, ' ')
    .replace(/[_]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!cleaned) return '';
  if (/^\d+$/.test(cleaned)) return '';
  return cleaned;
}

function shouldUseDirectName(raw, codeRaw) {
  const name = normalizeDirectGeoName(raw);
  const code = normalizeCode(codeRaw);
  if (!name || !code) return false;
  const nameCompact = name.replace(/[^A-Z0-9]/g, '');
  const codeCompact = code.replace(/[^A-Z0-9]/g, '');
  if (!nameCompact || !codeCompact) return false;
  // When geo NAME20 equals prec_id, still accept human place names (Harrison 01, Lula).
  if (nameCompact === codeCompact && looksLikeCodeOnlyLabel(name)) return false;
  const letters = (name.match(/[A-Z]/g) || []).length;
  if (!letters) return false;
  if (!/[0-9]/.test(nameCompact)) return letters >= 3;
  if (/[\/\s-]/.test(name)) return true;
  return !looksLikeCodeOnlyLabel(name);
}

function mergeGeoJsonNames(out, votingGeoJsonPayload) {
  const features = Array.isArray(votingGeoJsonPayload?.features) ? votingGeoJsonPayload.features : [];
  for (const feature of features) {
    const props = feature?.properties || {};
    const county = normalizeCounty(props.county_norm || props.county_nam);
    const code = normalizeCode(props.prec_id);
    // GA Voting_Precincts uses prec_id as the primary label (no enr_desc).
    const geoName = normalizeDirectGeoName(props.prec_id);
    if (!county || !code || !shouldUseDirectName(geoName, code)) continue;
    if (!out[county]) out[county] = {};
    // Geo NAME20 / prec_id is authoritative for already-named VTDs.
    out[county][code] = formatDisplayName(geoName);
  }
  return out;
}

function pickBestSeedName(names) {
  let best = '';
  let bestScore = -1e9;
  for (const raw of names || []) {
    const cand = normalizeAliasNameCandidate(raw);
    // Allow seeded place names that include digits (Oak 1, Precinct 5, CP053 facility names).
    if (!cand || looksLikeCodeOnlyLabel(cand)) continue;
    const score = scoreNameCandidate(cand);
    if (score > bestScore) {
      bestScore = score;
      best = cand;
    }
  }
  return best;
}

function applySeedOverrides(out, seedPayload, displayCodesByCounty) {
  if (!seedPayload || typeof seedPayload !== 'object') return out;
  for (const [countyRaw, mapping] of Object.entries(seedPayload)) {
    if (String(countyRaw).startsWith('_') || !mapping || typeof mapping !== 'object') continue;
    const county = normalizeCounty(countyRaw);
    const allowed = displayCodesByCounty.get(county);
    if (!allowed) continue;
    if (!out[county]) out[county] = {};

    for (const [tokenRaw, namesRaw] of Object.entries(mapping)) {
      const token = normalizeCode(tokenRaw);
      if (!token || !allowed.has(token)) continue;
      const names = Array.isArray(namesRaw) ? namesRaw : [namesRaw];
      const best = pickBestSeedName(names);
      if (!best) continue;
      // Seed wins for abbreviation-style codes (Houston HEFS, etc.).
      out[county][token] = formatDisplayName(best);
    }
  }
  return out;
}

function collectDisplayCodes(votingGeoJsonPayload) {
  const features = Array.isArray(votingGeoJsonPayload?.features) ? votingGeoJsonPayload.features : [];
  const out = new Map();
  for (const feature of features) {
    const props = feature?.properties || {};
    const county = normalizeCounty(props.county_norm || props.county_nam);
    const code = normalizeCode(props.prec_id);
    if (!county || !code) continue;
    if (!out.has(county)) out.set(county, new Set());
    out.get(county).add(code);
  }
  return out;
}

function pruneToDisplayCodes(counties, displayCodesByCounty) {
  const out = {};
  for (const [county, codeMap] of Object.entries(counties || {})) {
    const allowedCodes = displayCodesByCounty.get(normalizeCounty(county));
    if (!allowedCodes || !codeMap || typeof codeMap !== 'object') continue;
    const kept = {};
    for (const [code, name] of Object.entries(codeMap)) {
      const normalizedCode = normalizeCode(code);
      if (allowedCodes.has(normalizedCode)) {
        kept[normalizedCode] = name;
      }
    }
    if (Object.keys(kept).length) out[county] = kept;
  }
  return out;
}

function sortCountyCodeMap(counties) {
  const sorted = {};
  for (const county of Object.keys(counties || {}).sort((a, b) => a.localeCompare(b))) {
    const codeMap = counties[county] || {};
    const sortedCodes = {};
    for (const code of Object.keys(codeMap).sort((a, b) => a.localeCompare(b))) {
      sortedCodes[code] = codeMap[code];
    }
    sorted[county] = sortedCodes;
  }
  return sorted;
}

function stableStringify(value, level = 0) {
  const pad = '  '.repeat(level);
  const childPad = '  '.repeat(level + 1);
  if (Array.isArray(value)) {
    if (!value.length) return '[]';
    return `[\n${value.map(item => `${childPad}${stableStringify(item, level + 1)}`).join(',\n')}\n${pad}]`;
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort((a, b) => a.localeCompare(b));
    if (!keys.length) return '{}';
    return `{\n${keys.map(key => `${childPad}${JSON.stringify(key)}: ${stableStringify(value[key], level + 1)}`).join(',\n')}\n${pad}}`;
  }
  return JSON.stringify(value);
}

function stringifyFriendlyPayload(payload) {
  return `{
  "version": ${JSON.stringify(payload.version)},
  "generated_at": ${JSON.stringify(payload.generated_at)},
  "generated_from": ${stableStringify(payload.generated_from, 1)},
  "counties": ${stableStringify(payload.counties, 1)}
}
`;
}

function loadJsonIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function main() {
  const repoRoot = path.resolve(__dirname, '..');
  const inputPath = process.argv[2]
    ? path.resolve(process.argv[2])
    : path.join(repoRoot, 'Data', 'precinct_alias_index.json');
  const outputPath = process.argv[3]
    ? path.resolve(process.argv[3])
    : path.join(repoRoot, 'Data', 'precinct_friendly_names.json');
  const votingGeoJsonPath = process.argv[4]
    ? path.resolve(process.argv[4])
    : path.join(repoRoot, 'Data', 'Voting_Precincts.geojson');
  const seedAliasesPath = process.argv[5]
    ? path.resolve(process.argv[5])
    : path.join(repoRoot, 'Data', 'crosswalks', 'precinct_name_aliases.json');

  const payload = JSON.parse(fs.readFileSync(inputPath, 'utf8'));
  const votingGeoJson = JSON.parse(fs.readFileSync(votingGeoJsonPath, 'utf8'));
  const seedPayload = loadJsonIfExists(seedAliasesPath);
  const displayCodesByCounty = collectDisplayCodes(votingGeoJson);

  let counties = buildFriendlyNamesIndex(payload);
  counties = mergeGeoJsonNames(counties, votingGeoJson);
  counties = applySeedOverrides(counties, seedPayload, displayCodesByCounty);
  counties = sortCountyCodeMap(pruneToDisplayCodes(counties, displayCodesByCounty));

  const generatedFrom = [
    path.relative(repoRoot, inputPath).replace(/\\/g, '/'),
    path.relative(repoRoot, votingGeoJsonPath).replace(/\\/g, '/')
  ];
  if (seedPayload) {
    generatedFrom.push(path.relative(repoRoot, seedAliasesPath).replace(/\\/g, '/'));
  }

  const out = {
    version: 1,
    generated_at: new Date().toISOString(),
    generated_from: generatedFrom,
    counties
  };

  fs.writeFileSync(outputPath, stringifyFriendlyPayload(out), 'utf8');
  console.log(`Wrote ${Object.keys(counties).length} counties -> ${path.relative(repoRoot, outputPath)}`);
}

if (require.main === module) main();
