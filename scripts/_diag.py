import sys, os
sys.path.insert(0, os.path.abspath('.'))
from backend.pipeline.settings import settings
import psycopg2

conn = psycopg2.connect(
    host=settings.postgres_host, port=settings.postgres_port,
    dbname=settings.postgres_db,
    user=settings.postgres_readonly_user,
    password=settings.postgres_readonly_password
)
cur = conn.cursor()

print("=== silver.fundamentals ALL rows - metric_names ===")
cur.execute("SELECT symbol, metric_name, metric_value FROM silver.fundamentals ORDER BY symbol, metric_name")
for r in cur.fetchall():
    print(f"  {r[0]:12s} | {r[1]:25s} | {r[2]}")

conn.close()
