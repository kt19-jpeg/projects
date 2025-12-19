"""
Read training DataFrame from the normalized DB.

Usage:
  python data/read_from_db.py --db-path data/crime.db --out data/training.parquet
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
import pandas as pd

SQL = """
SELECT
  i.incident_id,
  i.hour_of_day,
  i.latitude,
  i.longitude,
  i.zip_code,
  i.council_district,
  dow.name AS day_of_week,
  pd.name  AS police_district,
  nb.name  AS neighborhood,
  l.violent
FROM incidents i
JOIN labels l ON l.incident_id = i.incident_id
LEFT JOIN day_of_week_dim dow ON dow.day_of_week_id = i.day_of_week_id
LEFT JOIN police_district_dim pd ON pd.police_district_id = i.police_district_id
LEFT JOIN neighborhood_dim nb ON nb.neighborhood_id = i.neighborhood_id;
"""

def load_training_df(db_path: str | Path) -> pd.DataFrame:
    con = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(SQL, con)
    finally:
        con.close()
    return df
