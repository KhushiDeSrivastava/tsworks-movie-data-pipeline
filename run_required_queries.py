
import sqlite3
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
DB_FILE = PROJECT_DIR / "movie_data.db"
SQL_FILE = PROJECT_DIR / "queries.sql"


def main():
    if not DB_FILE.exists():
        raise FileNotFoundError(f"Database not found: {DB_FILE}")

    if not SQL_FILE.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")

    sql_text = SQL_FILE.read_text(encoding="utf-8")
    queries = [
        query.strip()
        for query in sql_text.split(";")
        if query.strip()
    ]

    with sqlite3.connect(DB_FILE) as connection:
        for number, query in enumerate(queries, start=1):
            print(f"\n{'=' * 60}")
            print(f"REQUIRED ASSIGNMENT QUERY {number}")
            print("=" * 60)

            try:
                cursor = connection.execute(query)
                columns = [column[0] for column in cursor.description]
                print(" | ".join(columns))

                rows = cursor.fetchall()
                for row in rows:
                    print(" | ".join(str(value) for value in row))

                if not rows:
                    print("No results returned.")

                print(f"\nRows returned: {len(rows)}")

            except sqlite3.Error as error:
                print(f"SQL ERROR: {error}")

    print("\nFinished running required assignment queries.")


if __name__ == "__main__":
    main()
