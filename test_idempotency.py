
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import pandas as pd

from etl import load_data


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        test_db = Path(temp_dir) / "test_movies.db"

        movies = pd.DataFrame([
            {
                "movieId": 1,
                "title": "Toy Story (1995)",
                "release_year": 1995,
                "genre_list": ["Animation", "Children"],
            },
            {
                "movieId": 2,
                "title": "Jumanji (1995)",
                "release_year": 1995,
                "genre_list": ["Adventure", "Children"],
            },
        ])

        ratings = pd.DataFrame([
            {
                "userId": 10,
                "movieId": 1,
                "rating": 4.0,
                "timestamp": 900000000,
            },
            {
                "userId": 10,
                "movieId": 2,
                "rating": 3.5,
                "timestamp": 900000100,
            },
        ])

        # First load into the temporary database.
        load_data(movies, ratings, db_file=test_db)

        # Simulate an already enriched movie.
        with closing(sqlite3.connect(test_db)) as connection:
            with connection:
                connection.execute("""
                    UPDATE movie_details
                    SET api_status = 'success',
                        plot = 'Test plot'
                    WHERE movie_id = 1
                """)

        tables = [
            "movies",
            "ratings",
            "genres",
            "movie_genres",
            "movie_details",
        ]

        # Capture counts before the second load.
        with closing(sqlite3.connect(test_db)) as connection:
            before = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in tables
            }

        # Load identical data a second time.
        load_data(movies, ratings, db_file=test_db)

        # Verify counts and saved enrichment.
        with closing(sqlite3.connect(test_db)) as connection:
            after = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in tables
            }

            saved_detail = connection.execute("""
                SELECT api_status, plot
                FROM movie_details
                WHERE movie_id = 1
            """).fetchone()

        assert before == after, (
            f"Row counts changed: before={before}, after={after}"
        )

        assert saved_detail == ("success", "Test plot"), (
            f"Enrichment was not preserved: {saved_detail}"
        )

        print("PASS: rerunning ETL did not add duplicate rows.")
        print("PASS: existing enrichment was preserved.")
        print("Row counts:", after)


if __name__ == "__main__":
    main()
