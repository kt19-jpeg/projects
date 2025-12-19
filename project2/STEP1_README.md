# Step 1: Database-first ingestion (Crime Incidents → SQLite)

## Files
- `data/create_db.py` — creates normalized schema in SQLite
- `data/load_db.py` — loads `Crime_Incidents_20251014.csv` into the schema + creates `violent` label
- `data/read_from_db.py` — joins dims and returns a training DataFrame

## Run locally (from repo root)
```bash
python data/create_db.py --db-path data/crime.db
python data/load_db.py --csv-path data/Crime_Incidents_20251014.csv --db-path data/crime.db
python data/read_from_db.py --db-path data/crime.db --out data/training.parquet
```

## Notes
- Target label: `violent` is derived from `Parent Incident Type` (see `VIOLENT_SET` in `load_db.py`).
- Features exported by `read_from_db.py` exclude the offense/type columns to avoid leakage.
