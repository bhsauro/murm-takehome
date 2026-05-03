#!/usr/bin/env python3
"""
California County Officials MVP Collector
Source: California Secretary of State - 2025 California Roster
URL:    https://admin.cdn.sos.ca.gov/ca-roster/2025/counties2.pdf

Deviations from the Part 1 data model (noted):
- No party affiliation: this is a staff roster, not election results.
- No start/end dates: the source doesn't include them.
- No person deduplication: people table is a pass-through here.
- selection_method is inferred from a known-roles list, not sourced directly.
  Records where inference is uncertain are flagged.
- Single source, so no source_coverage junction table needed.
- confidence_score is assigned by source type (official_state → 0.9),
  not a full rubric. Flagged records get 0.6.
"""

import io
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber
import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_URL = "https://admin.cdn.sos.ca.gov/ca-roster/2025/counties2.pdf"
SOURCE_TYPE = "official_state"
COLLECTED_AT = datetime.now(timezone.utc).isoformat()

CA_COUNTIES = {
    "Alameda", "Alpine", "Amador", "Butte", "Calaveras", "Colusa",
    "Contra Costa", "Del Norte", "El Dorado", "Fresno", "Glenn",
    "Humboldt", "Imperial", "Inyo", "Kern", "Kings", "Lake", "Lassen",
    "Los Angeles", "Madera", "Marin", "Mariposa", "Mendocino", "Merced",
    "Modoc", "Mono", "Monterey", "Napa", "Nevada", "Orange", "Placer",
    "Plumas", "Riverside", "Sacramento", "San Benito", "San Bernardino",
    "San Diego", "San Francisco", "San Joaquin", "San Luis Obispo",
    "San Mateo", "Santa Barbara", "Santa Clara", "Santa Cruz", "Shasta",
    "Sierra", "Siskiyou", "Solano", "Sonoma", "Stanislaus", "Sutter",
    "Tehama", "Trinity", "Tulare", "Tuolumne", "Ventura", "Yolo", "Yuba",
}

# Roles that are structurally elected in California counties.
# Source: California Government Code / CA Constitution.
# Roles not on either list get selection_method = "unknown" and are flagged.
#
# NOTE: In a production system these three constants (ELECTED_ROLES,
# APPOINTED_ROLES, OFFICE_NORMALIZATION) would be pulled from the database
# rather than hardcoded here — specifically from the office_types table and
# a per-state selection_method reference table. Hardcoded for the MVP because
# the reference data doesn't exist yet.
#
# The same applies to governing_entities and offices: in production these would
# be pre-existing reference rows representing known counties and eligible offices.
# Records that don't match a known entity or office should be flagged for review
# rather than silently inserted. The MVP uses INSERT OR IGNORE, which is fine for
# a single-source bootstrap but would hide conflicts in an ongoing pipeline.
ELECTED_ROLES = {
    "Assessor",
    "Auditor-Controller",
    "Auditor",
    "Board of Supervisors",
    "Clerk-Recorder",
    "County Clerk-Recorder",
    "District Attorney",
    "Recorder-Clerk",
    "Registrar of Voters",  # elected in some CA counties, appointed in others → flagged
    "Sheriff",
    "Sheriff-Coroner",
    "Superintendent of Schools",
    "Treasurer-Tax Collector",
    "Treasurer/Tax Collector",
}

APPOINTED_ROLES = {
    "Animal Services",
    "Behavioral Health",
    "Chief Administrative Officer",
    "Chief Probation Officer",
    "Child Support Services Director",
    "Clerk of the Board",
    "Community Development Director",
    "County Administrator",
    "County Counsel",
    "County Librarian",
    "Developmental Services Director",
    "Emergency Services",
    "Fire Chief",
    "General Services Agency Director",
    "Health Care Services Director",
    "Human Resource Director",
    "Information Technology Director",
    "Public Defender",
    "Public Works Agency Director",
    "Social Services Director",
}

# Normalized office type vocabulary (mirrors Part 1 office_types table).
OFFICE_NORMALIZATION = {
    "Assessor": "County Assessor",
    "Auditor": "County Auditor-Controller",
    "Auditor-Controller": "County Auditor-Controller",
    "Board of Supervisors": "County Commissioner",
    "Chief Administrative Officer": "County Executive",
    "Chief Probation Officer": "Chief Probation Officer",
    "Clerk-Recorder": "County Clerk-Recorder",
    "County Administrator": "County Executive",
    "County Clerk-Recorder": "County Clerk-Recorder",
    "County Counsel": "County Counsel",
    "County Librarian": "County Librarian",
    "District Attorney": "District Attorney",
    "Fire Chief": "Fire Chief",
    "Public Defender": "Public Defender",
    "Recorder-Clerk": "County Clerk-Recorder",
    "Registrar of Voters": "Registrar of Voters",
    "Sheriff": "Sheriff",
    "Sheriff-Coroner": "Sheriff",
    "Superintendent of Schools": "County Superintendent of Schools",
    "Treasurer-Tax Collector": "County Treasurer",
    "Treasurer/Tax Collector": "County Treasurer",
}

# Lines that are page-level headers/footers, not county data.
PAGE_NOISE = {"County Officials", "California Roster 2025", "California Roster"}

# Metadata fields that appear in the county header block — skip when parsing officials.
HEADER_FIELDS = {
    "Address", "Mailing Address", "Telephone", "Fax", "Website",
    "Business Hours", "Incorporated", "Legislative Districts", "Population",
    "County Seat", "BOE",
}

# Placeholder names that indicate a vacancy or data gap.
PLACEHOLDER_NAMES = {"vacant", "unk", "tbd", "n/a", "none", "unknown", ""}


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def fetch_pdf(url: str) -> bytes:
    print(f"Fetching {url} ...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    print(f"  {len(resp.content) / 1024:.0f} KB downloaded")
    return resp.content


def extract_document_text(pdf_bytes: bytes) -> str:
    """
    Extract all text from the PDF in two-column reading order:
      left column (page 0) → right column (page 0) → left column (page 1) → …

    Sorting by (page, column, top, x0) means county content that spans columns
    or pages is naturally concatenated in sequence — no per-page overflow
    detection needed.
    """
    all_words: list[tuple] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        mid = pdf.pages[0].width / 2
        for page_num, page in enumerate(pdf.pages):
            for w in page.extract_words(x_tolerance=3, y_tolerance=3):
                col = 0 if (w["x0"] + w["x1"]) / 2 < mid else 1
                all_words.append(
                    (page_num, col, round(w["top"] / 4) * 4, w["x0"], w["text"])
                )

    all_words.sort(key=lambda w: w[:4])

    lines: list[str] = []
    current_line: list[str] = []
    prev_key: tuple | None = None

    for page_num, col, top, x0, text in all_words:
        key = (page_num, col, top)
        if key == prev_key or prev_key is None:
            current_line.append(text)
        else:
            if current_line:
                lines.append(" ".join(current_line))
            current_line = [text]
        prev_key = key

    if current_line:
        lines.append(" ".join(current_line))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def classify_selection_method(role: str) -> str:
    role_clean = role.strip()
    for r in ELECTED_ROLES:
        if r.lower() == role_clean.lower() or r.lower() in role_clean.lower():
            return "elected"
    for r in APPOINTED_ROLES:
        if r.lower() == role_clean.lower() or r.lower() in role_clean.lower():
            return "appointed"
    return "unknown"


def normalize_office(role: str) -> str:
    role_clean = role.strip()
    if role_clean in OFFICE_NORMALIZATION:
        return OFFICE_NORMALIZATION[role_clean]
    for key, val in OFFICE_NORMALIZATION.items():
        if key.lower() in role_clean.lower():
            return val
    return role_clean


def is_header_field(line: str) -> bool:
    """Return True if this line is county metadata, not an official record."""
    field = line.split(":")[0].strip() if ":" in line else line.strip()
    return field in HEADER_FIELDS


def parse_county_block(text: str, county_name: str) -> list[dict]:
    """Extract official records from one county's column text."""
    records = []
    mode = None  # "supervisors" | "officials" | None

    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line or line in PAGE_NOISE:
            continue

        # Strip county name prefix that may be glued to a section header due to
        # PDF layout (e.g. "Solano Elected and Appointed Officials").
        for _county in CA_COUNTIES:
            for _suffix in _SECTION_SUFFIXES:
                if line == _county + _suffix:
                    line = _suffix.strip()
                    break

        # Section transitions
        if "Board of Supervisors" in line:
            mode = "supervisors"
            continue
        if "Elected and Appointed Officials" in line:
            mode = "officials"
            continue
        if line.startswith("Superior Court") or line.startswith("Additional Elected"):
            mode = None
            continue

        if mode == "supervisors":
            m = re.match(r"District\s+(\d+)\s*:\s*(.+)", line, re.IGNORECASE)
            if m:
                records.append(_make_record(
                    county=county_name,
                    local_title=f"Board of Supervisors District {m.group(1)}",
                    name=m.group(2).strip(),
                    selection_method="elected",
                    normalized_type="County Commissioner",
                ))

        elif mode == "officials":
            if ":" not in line or is_header_field(line):
                # A colon-free line in officials mode is likely a wrapped name
                # continuation (e.g. "Ray\nHodges" split across PDF lines).
                # If the previous record has a single-word name, append this line.
                if (
                    records
                    and len(records[-1]["full_name"].split()) == 1
                    and line
                    and line[0].isupper()
                ):
                    records[-1]["full_name"] += " " + line
                continue
            role, _, name = line.partition(":")
            role, name = role.strip(), name.strip()
            if not role or not name:
                continue
            records.append(_make_record(
                county=county_name,
                local_title=role,
                name=name,
                selection_method=classify_selection_method(role),
                normalized_type=normalize_office(role),
            ))

    return records


def _make_record(
    county: str,
    local_title: str,
    name: str,
    selection_method: str,
    normalized_type: str,
) -> dict:
    return {
        "county": county,
        "state": "California",
        "local_office_title": local_title,
        "normalized_office_type": normalized_type,
        "full_name": name,
        "selection_method": selection_method,
        "status": "current",
        "source_url": SOURCE_URL,
        "source_type": SOURCE_TYPE,
        "collected_at": COLLECTED_AT,
        "confidence_score": None,  # assigned in validate_records()
        "validation_flag": None,
    }


def clean_column_text(text: str) -> str:
    """Strip page-level headers and footers from column text."""
    return "\n".join(
        ln for ln in text.split("\n") if ln.strip() not in PAGE_NOISE
    )


# Section header phrases that can appear on the same line as a county name
# when the county name and section header happen to share the same y-coordinate
# in the PDF (a layout artifact in the 2025 CA SOS Roster).
_SECTION_SUFFIXES = (" Board of Supervisors", " Elected and Appointed Officials")


def _extract_county_from_line(line: str) -> str | None:
    """Return the CA county name if this line IS exactly one."""
    candidate = line.strip()
    return candidate if candidate in CA_COUNTIES else None


def find_county_name(text: str) -> str | None:
    """Return the first California county name found in the text."""
    for line in text.split("\n"):
        county = _extract_county_from_line(line)
        if county:
            return county
    return None


def all_county_names_with_positions(text: str) -> list[tuple[str, int]]:
    """Return (county_name, line_index) for every CA county name found in text."""
    results = []
    for i, line in enumerate(text.split("\n")):
        county = _extract_county_from_line(line)
        if county:
            results.append((county, i))
    return results


def parse_column(text: str) -> list[dict]:
    """Clean column text and extract all records."""
    cleaned = "\n".join(
        ln for ln in text.split("\n") if ln.strip() not in PAGE_NOISE
    )
    county = find_county_name(cleaned)
    if not county:
        return []
    return parse_county_block(cleaned, county)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_records(records: list[dict]) -> list[dict]:
    """
    Add validation flags and assign confidence scores.

    Flags:
      missing_name           — name is empty or a known placeholder
      single_word_name       — may be truncated or a title/honorific
      interim_or_qualified   — name contains a parenthetical qualifier
      selection_method_unknown — role couldn't be classified from known lists
      ambiguous_elected      — role is elected in some CA counties, appointed in others
    """
    AMBIGUOUS = {"Registrar of Voters", "Public Defender", "Auditor-Controller"}

    for rec in records:
        flags = []
        name = rec["full_name"].strip()

        if name.lower() in PLACEHOLDER_NAMES:
            flags.append("missing_name")
        elif len(name.split()) == 1:
            flags.append("single_word_name")

        if re.search(r"\(.+\)", name):
            flags.append("interim_or_qualified")

        if rec["selection_method"] == "unknown":
            flags.append("selection_method_unknown")

        role = rec["local_office_title"]
        if any(a.lower() in role.lower() for a in AMBIGUOUS):
            flags.append("ambiguous_elected")

        rec["validation_flag"] = "; ".join(flags) if flags else None

        # Confidence: official state source is 0.9 baseline; flagged records drop to 0.6
        rec["confidence_score"] = 0.6 if flags else 0.9

    return records


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)
    print(f"  CSV  → {path}  ({len(df)} rows)")


def write_sqlite(records: list[dict], path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript("""
        DROP VIEW  IF EXISTS current_officials;
        DROP TABLE IF EXISTS office_holders;
        DROP TABLE IF EXISTS offices;
        DROP TABLE IF EXISTS office_types;
        DROP TABLE IF EXISTS people;
        DROP TABLE IF EXISTS governing_entities;
        DROP TABLE IF EXISTS sources;

        CREATE TABLE IF NOT EXISTS sources (
            source_id   INTEGER PRIMARY KEY,
            url         TEXT NOT NULL,
            source_type TEXT NOT NULL,
            trust_tier  INTEGER NOT NULL,
            collected_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS governing_entities (
            entity_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL UNIQUE,
            entity_type TEXT NOT NULL DEFAULT 'county',
            primary_state TEXT NOT NULL DEFAULT 'California',
            is_active   INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS office_types (
            office_type_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            normalized_office_type  TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS offices (
            office_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id           INTEGER NOT NULL REFERENCES governing_entities(entity_id),
            office_type_id      INTEGER REFERENCES office_types(office_type_id),
            local_office_title  TEXT NOT NULL,
            selection_method    TEXT,
            is_active           INTEGER NOT NULL DEFAULT 1,
            UNIQUE (entity_id, local_office_title)
        );

        CREATE TABLE IF NOT EXISTS people (
            person_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS office_holders (
            office_holder_id INTEGER PRIMARY KEY AUTOINCREMENT,
            office_id        INTEGER NOT NULL REFERENCES offices(office_id),
            person_id        INTEGER NOT NULL REFERENCES people(person_id),
            status           TEXT NOT NULL DEFAULT 'current',
            source_id        INTEGER REFERENCES sources(source_id),
            confidence_score REAL,
            validation_flag  TEXT,
            collected_at     TEXT
        );

        CREATE VIEW IF NOT EXISTS current_officials AS
        SELECT
            oh.office_holder_id,
            p.full_name,
            ge.primary_state AS state,
            ge.name          AS county_name,
            ot.normalized_office_type,
            o.local_office_title,
            o.selection_method,
            oh.status,
            s.source_type,
            oh.confidence_score,
            oh.validation_flag,
            oh.collected_at
        FROM office_holders oh
        JOIN offices          o  ON oh.office_id  = o.office_id
        JOIN governing_entities ge ON o.entity_id = ge.entity_id
        JOIN people           p  ON oh.person_id  = p.person_id
        LEFT JOIN office_types ot ON o.office_type_id = ot.office_type_id
        LEFT JOIN sources      s  ON oh.source_id  = s.source_id;
    """)

    # Source
    conn.execute(
        "INSERT OR IGNORE INTO sources (source_id, url, source_type, trust_tier, collected_at) "
        "VALUES (1, ?, ?, 1, ?)",
        (SOURCE_URL, SOURCE_TYPE, COLLECTED_AT),
    )

    # Pre-populate reference tables and build in-memory id maps
    conn.executemany(
        "INSERT OR IGNORE INTO governing_entities (name) VALUES (?)",
        {(rec["county"],) for rec in records},
    )
    entity_id_map = {
        row[0]: row[1]
        for row in conn.execute("SELECT name, entity_id FROM governing_entities")
    }

    conn.executemany(
        "INSERT OR IGNORE INTO office_types (normalized_office_type) VALUES (?)",
        {(rec["normalized_office_type"],) for rec in records},
    )
    office_type_id_map = {
        row[0]: row[1]
        for row in conn.execute("SELECT normalized_office_type, office_type_id FROM office_types")
    }

    conn.executemany(
        "INSERT OR IGNORE INTO offices (entity_id, office_type_id, local_office_title, selection_method) "
        "VALUES (?, ?, ?, ?)",
        {
            (entity_id_map[rec["county"]], office_type_id_map[rec["normalized_office_type"]],
             rec["local_office_title"], rec["selection_method"])
            for rec in records
        },
    )
    office_id_map = {
        (row[0], row[1]): row[2]
        for row in conn.execute("SELECT entity_id, local_office_title, office_id FROM offices")
    }

    for rec in records:
        entity_id = entity_id_map[rec["county"]]
        office_id = office_id_map[(entity_id, rec["local_office_title"])]

        # people (no deduplication — known gap, noted in README)
        conn.execute("INSERT INTO people (full_name) VALUES (?)", (rec["full_name"],))
        person_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # office_holders
        conn.execute(
            "INSERT INTO office_holders "
            "(office_id, person_id, status, source_id, confidence_score, validation_flag, collected_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?)",
            (office_id, person_id, rec["status"],
             rec["confidence_score"], rec["validation_flag"], rec["collected_at"]),
        )

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM office_holders").fetchone()[0]
    counties = conn.execute("SELECT COUNT(*) FROM governing_entities").fetchone()[0]
    flagged = conn.execute(
        "SELECT COUNT(*) FROM office_holders WHERE validation_flag IS NOT NULL"
    ).fetchone()[0]
    elected = conn.execute(
        "SELECT COUNT(*) FROM offices WHERE selection_method = 'elected'"
    ).fetchone()[0]

    conn.close()
    print(f"  SQLite → {path}")
    print(f"    {counties} counties  |  {total} officials  |  {elected} elected  |  {flagged} flagged")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("California County Officials — 2025 CA SOS Roster")
    print("=" * 60)

    pdf_bytes = fetch_pdf(SOURCE_URL)

    all_records: list[dict] = []
    seen_counties: set[str] = set()

    def add_records(recs: list[dict], county_name: str) -> None:
        if not recs:
            return
        all_records.extend(recs)
        if county_name not in seen_counties:
            seen_counties.add(county_name)
            print(f"  {county_name}: {len(recs)} officials")

    print("\nExtracting document text ...")
    full_text = extract_document_text(pdf_bytes)
    full_clean = clean_column_text(full_text)

    # Split on county name boundaries — works regardless of column/page layout.
    county_positions = all_county_names_with_positions(full_clean)
    full_lines = full_clean.split("\n")

    print(f"Parsing {len(county_positions)} county sections ...")
    for i, (county_name, start_idx) in enumerate(county_positions):
        end_idx = (
            county_positions[i + 1][1]
            if i + 1 < len(county_positions)
            else len(full_lines)
        )
        block = "\n".join(full_lines[start_idx:end_idx])
        recs = parse_county_block(block, county_name)
        add_records(recs, county_name)

    missing = CA_COUNTIES - seen_counties
    print(f"\nParsed {len(seen_counties)}/58 counties")
    if missing:
        print(f"Missing: {sorted(missing)}")

    all_records = validate_records(all_records)

    flagged_count = sum(1 for r in all_records if r["validation_flag"])
    print(f"Total officials: {len(all_records)}  |  Flagged: {flagged_count}")

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    print("\nWriting output ...")
    df = pd.DataFrame(all_records)
    write_csv(df, str(output_dir / "ca_officials.csv"))
    write_sqlite(all_records, str(output_dir / "ca_officials.db"))

    # Flagged record sample
    flagged_df = df[df["validation_flag"].notna()][
        ["county", "local_office_title", "full_name", "validation_flag"]
    ].head(15)
    if not flagged_df.empty:
        print("\nSample flagged records:")
        print(flagged_df.to_string(index=False))

    print("\nDone.")


if __name__ == "__main__":
    main()
