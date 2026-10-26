import os

import sqlite3
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("MOVIE_DB_PATH", str(PROJECT_DIR / "movie_data.db")))


def main():
    if not DB_PATH.exists():
        print("Database not found. Run etl.py first.")
        return

    with sqlite3.connect(DB_PATH) as connection:
        connection.execute("PRAGMA foreign_keys = ON")

        print("\nDATABASE VERIFICATION")
        print("=" * 40)

        tables = [
            "movies",
            "ratings",
            "genres",
            "movie_genres",
            "movie_details",
            "directors",
            "movie_directors",
        ]

        for table in tables:
            count = connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

            print(f"{table}: {count:,} rows")

        print("\nENRICHMENT STATUS")
        print("-" * 40)

        statuses = connection.execute(
            """
            SELECT api_status, COUNT(*)
            FROM movie_details
            GROUP BY api_status
            ORDER BY api_status
            """
        ).fetchall()

        for status, count in statuses:
            print(f"{status}: {count:,}")

        errors = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()

        if errors:
            print("\nForeign-key errors:", errors[:5])
        else:
            print("\nForeign-key validation passed.")


if __name__ == "__main__":
    main()
