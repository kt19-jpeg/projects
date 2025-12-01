import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

# Get paths from environment
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH")
EXPORT_DIR = os.getenv("EXPORT_DIR")

# Create export directory if it doesn't exist
os.makedirs(EXPORT_DIR, exist_ok=True)

# PostgreSQL connection
pg_conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD")
)
pg_cur = pg_conn.cursor()

# SQLite connection
sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
sqlite_cur = sqlite_conn.cursor()

# Rest of your code...
sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in sqlite_cur.fetchall()]

print("Tables detected:", tables)

txt_paths = {}

for t in tables:
    print(f"Exporting {t}...")
    df = pd.read_sql(f"SELECT * FROM {t}", sqlite_conn)
    df = df.astype(str)

    txt_path = os.path.join(EXPORT_DIR, f"{t}.txt")
    df.to_csv(txt_path, index=False, sep=",", quoting=1)
    txt_paths[t] = txt_path

print("All tables exported to TXT.")




sqlite_engine = create_engine(f"sqlite:///{SQLITE_DB_PATH}")

for t in tables:
    print(f"Creating table {t} as TEXT columns...")

# read empty DF to get column names
    df = pd.read_sql(f'SELECT * FROM "{t}" LIMIT 0;', sqlite_engine)

    cols = [f'"{col}" TEXT' for col in df.columns]
    create_sql = f"""
            
            DROP TABLE IF EXISTS "{t}" CASCADE;
            CREATE TABLE "{t}" ({", ".join(cols)});
    
        """

    pg_cur.execute(create_sql)
    pg_conn.commit()

print("All Postgres tables created with TEXT columns.")
for t, path in txt_paths.items():
    print(f"one time data dump in {t}...")

    # Step 1: delete existing data
    pg_cur.execute(f'TRUNCATE TABLE "{t}" RESTART IDENTITY CASCADE;')
    # - RESTART IDENTITY resets SERIAL / AUTOINCREMENT counters
    # - CASCADE ensures dependent tables are also truncated if needed

    # Step 2: load new data from TXT/CSV
    with open(path, "r", encoding="utf-8") as f:
        copy_sql = f"""
        COPY "{t}"
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE,
            QUOTE '"',
            ESCAPE '"'
        )
        """
        pg_cur.copy_expert(copy_sql, f)

pg_conn.commit()
print("All tables loaded successfully!")

