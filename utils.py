import os
from dotenv import load_dotenv


load_dotenv()  # reads variables from a .env file and sets them in os.environ



def get_db_url():
    POSTGRES_USERNAME = os.environ["PG_USER"]
    POSTGRES_PASSWORD = os.environ["PG_PASSWORD"]
    POSTGRES_SERVER = os.environ["PG_HOST"]
    POSTGRES_DATABASE = os.environ["PG_DB"]

    DATABASE_URL = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:5432/{POSTGRES_DATABASE}"

    return DATABASE_URL
