# Part 2: MVP – California County Officials

## Why California

The California Secretary of State publishes a 2025 California Roster as a single PDF covering all 58 counties in a consistent format. It is an official state source (trust tier 1 in the Part 1 model), covers both elected and appointed officials, and is current as of 2025. The structured layout — county name, address block, Board of Supervisors, then Elected and Appointed Officials — is consistent enough to parse programmatically with pdfplumber.

In Part 1, I defined workflows based on extraction strategy rather than file type. In practice, source structure still matters: structured sources (APIs, CSVs) map to Workflow 1, semi-structured sources (HTML pages, PDFs) typically map to Workflow 2, and highly unstructured or inconsistent sources map to Workflow 3 (LLM-assisted extraction). The California Roster falls into the semi-structured category — consistent enough in layout to parse deterministically — so I used pdfplumber and a state machine parser rather than an LLM-based approach.

Two alternatives I considered and passed on:
- A Secretary of State API: CA's SOS does not expose county officials via a public API.
- Scraping an individual county website: a state-wide source is more appropriate for an MVP — a single county would demonstrate the parsing approach but wouldn't produce a dataset of meaningful breadth.

## Source

**California Secretary of State — 2025 California Roster (counties)**  
`https://admin.cdn.sos.ca.gov/ca-roster/2025/counties2.pdf`

- Source type: `official_state`
- Trust tier: 1 (state government, self-reported by each county to the SOS)
- The document itself notes: *"The data herein is provided to the Secretary of State's Office by local jurisdictions."* This is important — accuracy depends on counties keeping their SOS submissions current.

## Deviations from the Part 1 data model

The script produces a simplified but structurally compatible schema. Deviations are noted in the file header and summarized here:

| Part 1 field | Status |
|---|---|
| `party_id` | Not available in this source (a roster, not election results) |
| `start_date` / `end_date` | Not available; would require cross-referencing election results |
| `confidence_score` | Assigned by source type (0.9) and lowered to 0.6 for flagged records; no full rubric yet |
| Person deduplication | Not implemented — people table is a pass-through; same individual under different name formats will appear as separate records |
| `source_coverage` junction table | Not needed for a single-source MVP |
| Analytics view (`current_officials`) | Implemented as a SQLite view |

## Running the script

```bash
pip install -r requirements.txt
python collect_ca_officials.py
```

Output is written to `output/ca_officials.csv` and `output/ca_officials.db`. Each run overwrites prior output.

## Output summary (2025 run)

- **58/58 counties** parsed
- **~1,600 officials** collected
- **~730 elected** officials (Board of Supervisors + known elected roles)
- **~175 flagged records** (see validation flags below)

## Validation flags

| Flag | Meaning |
|---|---|
| `missing_name` | Name is empty or a known placeholder (e.g. "Unk", "Vacant") |
| `single_word_name` | Name is a single word — may be truncated or a title |
| `interim_or_qualified` | Name contains a parenthetical qualifier, e.g. "(Interim)", "(CEO)" |
| `selection_method_unknown` | Role couldn't be classified from the known elected/appointed lists |
| `ambiguous_elected` | Role is elected in some CA counties, appointed in others (e.g. Auditor-Controller, Public Defender) |

Records with any flag get `confidence_score: 0.6`; clean records get `confidence_score: 0.9`.

---

## What I would do next with 4 more hours

**1. Add election results for start dates and party affiliation.**  
The roster lists officials but not when they took office or their party. Cross-referencing with election results would add `start_date`, `party_id`, and confirm elected/appointed classification from an authoritative source rather than a lookup list. The California Elections Data Archive (CEDA) — a joint project of CSU Sacramento's Center for California Studies and Institute for Social Research, in cooperation with the Secretary of State — compiles local election results including county offices and publishes annual reports each July. Note: the SOS does not certify or compile local election results itself; CEDA is the right source for this.

**2. Define the confidence score rubric properly.**  
Right now confidence scores are assigned by source type (0.9) and reduced for flags (0.6). A real rubric would include: decay logic as records age without reverification, source corroboration bonuses (record confirmed by two sources → higher score), and specific flag penalties rather than a binary drop.

**3. Implement person deduplication.**  
The current `people` table is a pass-through — every official gets a new row regardless of whether they already exist. This means the same person can accumulate multiple `person_id` values across runs or if they appear under slightly different name formats in the source (e.g. "Jim Smith", "James Smith", "James R. Smith"). A deduplication step is needed before the dataset can support longitudinal tracking (e.g. "this person held this office from year X to year Y").

The right approach is not just a UNIQUE constraint on `full_name` — "John Smith" in Alameda and "John Smith" in Alpine are probably different people, so deduplication needs to be scoped by county at minimum, and ideally cross-referenced against a second identifying attribute (office, party affiliation, or term dates) to resolve ambiguous cases. A fuzzy name match within county would handle the common case of name format variation; a separate `person_aliases` table would let the canonical record persist while preserving how each source spelled the name.

**4. Handle county metadata from the same PDF.**  
The roster includes population, incorporated date, legislative districts, and county seat for each county. These map directly to the `governing_entities` table and would make the dataset more useful for analysts.

**5. Robust multi-page continuation handling.**  
The PDF uses a two-column layout, and some counties span columns and pages in ways the parser handles with heuristics. A more principled approach — tracking county sections by character font size (county headers are 12pt, body text is 9pt) rather than column position — would be more reliable.

---

## Where I would be nervous about data quality in production

**Source staleness.** The SOS document notes that data is provided by local jurisdictions. The PDF has no per-county update timestamps. A county that hasn't updated its submission to the SOS in 18 months will show stale officials with no signal in the data. Last-verified-at timestamps and a reverification cadence (aligned with election cycles) would help surface this.

**Appointed vs. elected classification.**  
The source does not distinguish elected from appointed — it lists both under "Elected and Appointed Officials." The script infers selection method from a best-guess roles list (not from any authoritative source), and some roles vary by county. Records where the classification is uncertain are flagged `ambiguous_elected` but not resolved without a verified per-county source.

**Multi-page county data.**  
Large counties whose data spans multiple PDF pages require the parser to correctly stitch content across page and column boundaries. The script handles the most common patterns but uses heuristics that could misattribute officials for counties with unusual layouts. In production, this would need ground-truth validation against a manually verified sample.

**Obvious data errors from the source.**  
One record in the raw data shows `County Clerk-Recorder: Different Offices` for Siskiyou County — a clear data quality issue originating from the county's submission to the SOS, not a parsing error. At scale, automated completeness checks (valid name format, role not a placeholder) and human review of flagged records are necessary.

**No change detection.**  
A one-time pull is a snapshot. Mid-term changes (appointments, resignations, deaths in office) appear first in local news, not in the SOS roster. Without a scheduled recollection workflow and change detection, the dataset will drift from ground truth over time.

---

## AI usage

I used Claude Code as a development collaborator for Part 2: iterating on the PDF extraction approach, debugging layout edge cases, and drafting the README. I verified suggestions by running the script against the full PDF and checking county counts, record counts, and flagged records at each iteration — catching and correcting several parsing bugs along the way (e.g. false county boundaries, missed counties due to PDF overflow layout). The source selection, data model mapping, validation flag design, and written reasoning are my own.
