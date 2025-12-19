"""
Create a normalized SQLite database for the Crime Incidents dataset.

Usage:
  python data/create_db.py --db-path data/crime.db
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS parent_types (
  parent_type_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS day_of_week_dim (
  day_of_week_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS police_district_dim (
  police_district_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS neighborhood_dim (
  neighborhood_id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- Main fact table (incident_id is TEXT because many datasets use non-numeric IDs)
CREATE TABLE IF NOT EXISTS incidents (
  incident_id TEXT PRIMARY KEY,

  case_number TEXT,
  incident_datetime TEXT,             -- keep as ISO-ish string from CSV
  hour_of_day INTEGER,

  latitude REAL,
  longitude REAL,

  zip_code TEXT,
  council_district TEXT,

  parent_type_id INTEGER NOT NULL,
  day_of_week_id INTEGER,
  police_district_id INTEGER,
  neighborhood_id INTEGER,

  FOREIGN KEY (parent_type_id) REFERENCES parent_types(parent_type_id),
  FOREIGN KEY (day_of_week_id) REFERENCES day_of_week_dim(day_of_week_id),
  FOREIGN KEY (police_district_id) REFERENCES police_district_dim(police_district_id),
  FOREIGN KEY (neighborhood_id) REFERENCES neighborhood_dim(neighborhood_id)
);

-- Keep label separate (normalized)
CREATE TABLE IF NOT EXISTS labels (
  incident_id TEXT PRIMARY KEY,
  violent INTEGER NOT NULL CHECK (violent IN (0,1)),
  FOREIGN KEY (incident_id) REFERENCES incidents(incident_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_incidents_parent_type ON incidents(parent_type_id);
CREATE INDEX IF NOT EXISTS idx_incidents_day_of_week ON incidents(day_of_week_id);
CREATE INDEX IF NOT EXISTS idx_incidents_police_district ON incidents(police_district_id);
CREATE INDEX IF NOT EXISTS idx_incidents_neighborhood ON incidents(neighborhood_id);
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True, help="Path to sqlite db file, e.g. data/crime.db")
    args = ap.parse_args()

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    try:
        con.executescript(DDL)
        con.commit()
        print(f"✅ Created schema at: {db_path.resolve()}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
