import os
from dotenv import load_dotenv
load_dotenv()

POSTGRES_DSN = f"postgresql://{os.getenv('POSTGRES_USER','sportiq')}:{os.getenv('POSTGRES_PASSWORD','sportiq')}@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','sportiq')}"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
ENV = os.getenv("ENV","dev")
