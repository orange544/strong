import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent
SQL_FILE = BASE / "movie_sqlite.sql"
DB_FILE = BASE / "movie_sqlite.db"

schema = SQL_FILE.read_text(encoding="utf-8")
if DB_FILE.exists():
    DB_FILE.unlink()

conn = sqlite3.connect(DB_FILE)
try:
    conn.executescript(schema)
    conn.commit()
finally:
    conn.close()

print(f"SQLite initialized: {DB_FILE}")
