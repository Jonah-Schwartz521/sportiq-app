import os
from dotenv import load_dotenv

load_dotenv(override=True)

POSTGRES_USER = os.getenv("POSTGRES_USER", "sportiq")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sportiq")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sportiq")

# Raw DSN for psycopg (v3) direct connections
POSTGRES_DSN = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# SQLAlchemy DSN that *forces* the psycopg v3 driver, not psycopg2
SQLALCHEMY_DSN = (
    f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)