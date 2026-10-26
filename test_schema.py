
from pathlib import Path
import sqlite3


PROJECT_DIR = Path(__file__).resolve().parent
SCHEMA_FILE = PROJECT_DIR / "schema.sql"


def expect_failure(connection, description, sql, values):
    """Check that invalid data is rejected by the database."""
    try:
        connection.execute(sql, values)
        connection.commit()
        print(f"FAIL: {description} was accepted")
    except sqlite3.IntegrityError:
        connection.rollback()
        print(f"PASS: {description} was rejected")


def main():
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(
            f"Schema file not found: {SCHEMA_FILE}"
        )

    # Use an in-memory database so testing won't alter
    # your real project database.
    connection = sqlite3.connect(":memory:")

    # SQLite foreign keys must be enabled per connection.
    connection.execute("PRAGMA foreign_keys = ON")

    schema = SCHEMA_FILE.read_text(encoding="utf-8")
    
    connection.executescript(schema)
    print("PASS: Schema can be executed repeatedly")

    print("\n1. Checking that all seven tables exist")

    tables = connection.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """).fetchall()

    table_names = {row[0] for row in tables}

    expected_tables = {
        "movies",
        "ratings",
        "genres",
        "movie_genres",
        "movie_details",
        "directors",
        "movie_directors",
    }

    assert table_names == expected_tables, (
        f"Unexpected tables: {table_names}"
    )

    print("PASS: All seven tables exist")

    print("\n2. Inserting valid sample data")

    connection.execute(
        """
        INSERT INTO movies (movie_id, title, release_year)
        VALUES (?, ?, ?)
        """,
        (1, "Toy Story (1995)", 1995),
    )

    connection.execute(
        """
        INSERT INTO ratings
            (user_id, movie_id, rating, rated_at)
        VALUES (?, ?, ?, ?)
        """,
        (1, 1, 4.0, 964982703),
    )

    connection.execute(
        "INSERT INTO genres (genre_name) VALUES (?)",
        ("Animation",),
    )

    connection.execute(
        "INSERT INTO directors (director_name) VALUES (?)",
        ("John Lasseter",),
    )

    genre_id = connection.execute(
        "SELECT genre_id FROM genres WHERE genre_name = ?",
        ("Animation",),
    ).fetchone()[0]

    director_id = connection.execute(
        "SELECT director_id FROM directors WHERE director_name = ?",
        ("John Lasseter",),
    ).fetchone()[0]

    connection.execute(
        """
        INSERT INTO movie_genres (movie_id, genre_id)
        VALUES (?, ?)
        """,
        (1, genre_id),
    )

    connection.execute(
        """
        INSERT INTO movie_directors (movie_id, director_id)
        VALUES (?, ?)
        """,
        (1, director_id),
    )

    connection.execute(
        """
        INSERT INTO movie_details
            (movie_id, imdb_id, plot, api_status)
        VALUES (?, ?, ?, ?)
        """,
        (1, "tt0114709", "A toy comes to life.", "success"),
    )

    connection.commit()
    print("PASS: Valid related records were inserted")

    print("\n3. Testing database constraints")

    expect_failure(
        connection,
        "Duplicate movie ID",
        """
        INSERT INTO movies (movie_id, title)
        VALUES (?, ?)
        """,
        (1, "Duplicate Movie"),
    )

    expect_failure(
        connection,
        "Rating for a nonexistent movie",
        """
        INSERT INTO ratings
            (user_id, movie_id, rating, rated_at)
        VALUES (?, ?, ?, ?)
        """,
        (2, 999999, 4.0, 964982703),
    )

    expect_failure(
        connection,
        "Rating above 5",
        """
        INSERT INTO ratings
            (user_id, movie_id, rating, rated_at)
        VALUES (?, ?, ?, ?)
        """,
        (2, 1, 6.0, 964982703),
    )

    expect_failure(
        connection,
        "Duplicate user-movie rating",
        """
        INSERT INTO ratings
            (user_id, movie_id, rating, rated_at)
        VALUES (?, ?, ?, ?)
        """,
        (1, 1, 5.0, 964982704),
    )

    expect_failure(
        connection,
        "Invalid API status",
        """
        INSERT INTO movie_details (movie_id, api_status)
        VALUES (?, ?)
        """,
        (1, "unknown_status"),
    )

    print("\n4. Verifying the stored rating")

    result = connection.execute("""
        SELECT m.title, r.rating
        FROM movies AS m
        JOIN ratings AS r
            ON m.movie_id = r.movie_id
    """).fetchone()

    print(f"Movie: {result[0]}")
    print(f"Rating: {result[1]}")

    connection.close()

    print("\nAll schema tests completed!")


if __name__ == "__main__":
    main()
