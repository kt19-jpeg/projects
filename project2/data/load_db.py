"""
Load Crime Incidents CSV into normalized SQLite tables.

Fixes:
- Uses Case Number as fallback primary key when Incident ID is empty/NaN.
- Robust to column header differences (spaces/case/underscores).
- incident_id stored as TEXT.
- Builds dims + inserts incidents + labels.

Usage:
  python3 data/load_db.py --csv-path data/Crime_Incidents_20251014.csv --db-path data/crime.db --debug
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import pandas as pd

VIOLENT_SET = {
    "Assault",
    "Robbery",
    "Sexual Assault",
    "Homicide",
    "Other Sexual Offense",
    "Sexual Offense",
    "SODOMY",
}

def norm_col(c: str) -> str:
    return (
        str(c)
        .replace("\ufeff", "")
        .strip()
        .lower()
        .replace("-", " ")
        .replace("/", " ")
        .replace("’", "'")
        .replace(" ", "_")
    )

def norm_val(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    return s

def find_col(cols_norm_to_orig: dict[str, str], candidates: list[str]) -> str | None:
    for cand in candidates:
        key = norm_col(cand)
        if key in cols_norm_to_orig:
            return cols_norm_to_orig[key]
    return None

def upsert_dim(con: sqlite3.Connection, table: str, id_col: str, values: list[str | None]) -> dict[str, int]:
    uniq = sorted({v for v in values if v is not None})
    con.executemany(f"INSERT OR IGNORE INTO {table} (name) VALUES (?)", [(v,) for v in uniq])
    cur = con.execute(f"SELECT {id_col}, name FROM {table}")
    return {name: _id for _id, name in cur.fetchall()}

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-path", required=True)
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--chunksize", type=int, default=50000)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    csv_path = Path(args.csv_path)
    db_path = Path(args.db_path)
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    CANDS = {
        "incident_id": ["Incident ID", "incident_id", "incidentid", "objectid", "id"],
        "case_number": ["Case Number", "case_number", "case_no", "casenumber"],
        "parent_type": ["Parent Incident Type", "parent_incident_type", "parent_type", "category", "offense_category"],
        "incident_datetime": ["Incident Datetime", "incident_datetime", "incident_date_time", "datetime", "incident_date"],
        "hour_of_day": ["Hour of Day", "hour_of_day", "hour", "occur_hour"],
        "day_of_week": ["Day of Week", "day_of_week", "weekday", "dow"],
        "police_district": ["Police District", "police_district", "district"],
        "neighborhood": ["neighborhood", "Neighborhood", "neighbourhood", "area"],
        "council_district": ["Council District", "council_district", "council"],
        "zip_code": ["zip_code", "zipcode", "zip", "postal_code"],
        "latitude": ["Latitude", "lat", "y"],
        "longitude": ["Longitude", "lon", "lng", "x"],
    }

    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON;")

    total_rows = 0
    inserted = 0
    mapping_printed = False

    try:
        for chunk in pd.read_csv(csv_path, chunksize=args.chunksize, low_memory=False):
            total_rows += len(chunk)

            cols_norm_to_orig = {norm_col(c): c for c in chunk.columns}
            resolved = {k: find_col(cols_norm_to_orig, v) for k, v in CANDS.items()}

            if args.debug and not mapping_printed:
                print("Detected column mapping (canonical -> CSV header):")
                for k, v in resolved.items():
                    print(f"  {k:16s} -> {v}")
                print("First chunk headers:", list(chunk.columns))
                mapping_printed = True

            # Require: parent_type AND either incident_id or case_number
            if resolved["parent_type"] is None:
                raise ValueError("Could not find Parent Incident Type column.")
            if resolved["incident_id"] is None and resolved["case_number"] is None:
                raise ValueError("Could not find Incident ID or Case Number column (need one as primary key).")

            rename_map = {resolved[k]: k for k in resolved if resolved[k] is not None}
            chunk = chunk.rename(columns=rename_map)

            # Ensure required cols exist
            if "incident_id" not in chunk.columns:
                chunk["incident_id"] = None
            if "case_number" not in chunk.columns:
                chunk["case_number"] = None

            # Clean values
            chunk["incident_id"] = chunk["incident_id"].map(norm_val)
            chunk["case_number"] = chunk["case_number"].map(norm_val)
            chunk["parent_type"] = chunk["parent_type"].map(norm_val)

            # 🔥 Key fix: fallback to case_number when incident_id is missing/empty
            # If incident_id is mostly null, use case_number as incident_id
            nonnull_incident_id = chunk["incident_id"].notna().sum()
            if nonnull_incident_id == 0:
                chunk["incident_id"] = chunk["case_number"]

            # Now require incident_id + parent_type
            chunk = chunk.dropna(subset=["incident_id", "parent_type"])

            # Optional columns
            for col in [
                "incident_datetime", "day_of_week", "police_district",
                "neighborhood", "council_district", "zip_code"
            ]:
                if col not in chunk.columns:
                    chunk[col] = None
                chunk[col] = chunk[col].map(norm_val)

            # Numeric columns
            for col in ["hour_of_day", "latitude", "longitude"]:
                if col not in chunk.columns:
                    chunk[col] = pd.NA
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

            # Label
            chunk["violent"] = chunk["parent_type"].isin(VIOLENT_SET).astype(int)

            # Upsert dims
            parent_map = upsert_dim(con, "parent_types", "parent_type_id", chunk["parent_type"].tolist())
            dow_map = upsert_dim(con, "day_of_week_dim", "day_of_week_id", chunk["day_of_week"].tolist())
            pd_map = upsert_dim(con, "police_district_dim", "police_district_id", chunk["police_district"].tolist())
            nb_map = upsert_dim(con, "neighborhood_dim", "neighborhood_id", chunk["neighborhood"].tolist())

            incident_rows = []
            label_rows = []

            for r in chunk.itertuples(index=False):
                incident_id = str(getattr(r, "incident_id"))
                parent_type = getattr(r, "parent_type")
                parent_type_id = parent_map.get(parent_type)
                if parent_type_id is None:
                    continue

                day = getattr(r, "day_of_week")
                district = getattr(r, "police_district")
                neighborhood = getattr(r, "neighborhood")

                day_id = dow_map.get(day) if day else None
                pd_id = pd_map.get(district) if district else None
                nb_id = nb_map.get(neighborhood) if neighborhood else None

                case_number = getattr(r, "case_number")
                incident_datetime = getattr(r, "incident_datetime")
                hour = getattr(r, "hour_of_day")
                lat = getattr(r, "latitude")
                lon = getattr(r, "longitude")
                zip_code = getattr(r, "zip_code")
                council = getattr(r, "council_district")
                violent = int(getattr(r, "violent"))

                incident_rows.append((
                    incident_id,
                    case_number,
                    incident_datetime,
                    int(hour) if hour is not None and pd.notna(hour) else None,
                    float(lat) if lat is not None and pd.notna(lat) else None,
                    float(lon) if lon is not None and pd.notna(lon) else None,
                    zip_code,
                    council,
                    int(parent_type_id),
                    int(day_id) if day_id is not None else None,
                    int(pd_id) if pd_id is not None else None,
                    int(nb_id) if nb_id is not None else None,
                ))
                label_rows.append((incident_id, violent))

            con.executemany(
                """
                INSERT OR REPLACE INTO incidents (
                  incident_id, case_number, incident_datetime,
                  hour_of_day, latitude, longitude, zip_code, council_district,
                  parent_type_id, day_of_week_id, police_district_id, neighborhood_id
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                incident_rows,
            )
            con.executemany(
                "INSERT OR REPLACE INTO labels (incident_id, violent) VALUES (?,?)",
                label_rows,
            )

            con.commit()
            inserted += len(incident_rows)
            print(f"Loaded chunk: {len(incident_rows)} rows (total inserted: {inserted})")

        cur = con.execute("SELECT COUNT(*) FROM incidents")
        n_inc = cur.fetchone()[0]
        cur = con.execute("SELECT COALESCE(SUM(violent), 0), COUNT(*) FROM labels")
        v_sum, n_lab = cur.fetchone()
        v_sum = int(v_sum or 0)
        pct = (v_sum / n_lab * 100) if n_lab else 0.0

        print(f"✅ Done. incidents={n_inc:,} labels={n_lab:,} violent={v_sum:,} ({pct:.1f}%)")
        print(f"Total CSV rows processed: {total_rows:,}")

    finally:
        con.close()

if __name__ == "__main__":
    main()
