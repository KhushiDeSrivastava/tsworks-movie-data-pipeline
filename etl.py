
from pathlib import Path
import logging
import re
import sqlite3

import pandas as pd


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent

DATA_DIR = PROJECT_DIR / "data" / "raw" / "ml-latest-small"
MOVIES_FILE = DATA_DIR / "movies.csv"
RATINGS_FILE = DATA_DIR / "ratings.csv"

SCHEMA_FILE = PROJECT_DIR / "schema.sql"
DB_FILE = PROJECT_DIR / "movie_data.db"

LOG_DIR = PROJECT_DIR / "logs"
LOG_FILE = LOG_DIR / "etl.log"


# ============================================================
# 2. LOGGING
# ============================================================

def configure_logging():
    """Write progress messages to the terminal and a log file."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
        force=True,
    )


# ============================================================
# 3. EXTRACT
# ============================================================

def extract_data():
    """Read the MovieLens CSV files into pandas DataFrames."""
    if not MOVIES_FILE.exists():
        raise FileNotFoundError(
            f"Movies CSV not found: {MOVIES_FILE}"
        )

    if not RATINGS_FILE.exists():
        raise FileNotFoundError(
            f"Ratings CSV not found: {RATINGS_FILE}"
        )

    logging.info("Reading movies.csv")
    movies = pd.read_csv(MOVIES_FILE)

    logging.info("Reading ratings.csv")
    ratings = pd.read_csv(RATINGS_FILE)

    required_movie_columns = {
        "movieId", "title", "genres"
    }
    required_rating_columns = {
        "userId", "movieId", "rating", "timestamp"
    }

    if not required_movie_columns.issubset(movies.columns):
        raise ValueError(
            f"Missing movie columns: "
            f"{required_movie_columns - set(movies.columns)}"
        )

    if not required_rating_columns.issubset(ratings.columns):
        raise ValueError(
            f"Missing rating columns: "
            f"{required_rating_columns - set(ratings.columns)}"
        )

    logging.info(
        "Extracted %s movies and %s ratings",
        len(movies),
        len(ratings),
    )

    return movies, ratings


# ============================================================
# 4. TRANSFORM MOVIES
# ============================================================

def transform_movies(movies):
    """Clean movie records and extract their release years."""
    movies = movies.copy()

    # Convert IDs to numeric values.
    movies["movieId"] = pd.to_numeric(
        movies["movieId"], errors="coerce"
    )

    if movies["movieId"].isna().any():
        raise ValueError("Some movies have invalid movie IDs.")

    if (movies["movieId"] <= 0).any():
        raise ValueError("Movie IDs must be positive.")

    if (movies["movieId"] % 1 != 0).any():
        raise ValueError("Movie IDs must be integers.")

    movies["movieId"] = movies["movieId"].astype(int)

    # Every MovieLens movie ID must be unique.
    if movies["movieId"].duplicated().any():
        raise ValueError("Duplicate movie IDs found.")

    # Clean required text fields.
    movies["title"] = movies["title"].astype("string").str.strip()
    movies["genres"] = movies["genres"].astype("string").str.strip()

    invalid_titles = (
        movies["title"].isna()
        | movies["title"].eq("")
    )

    if invalid_titles.any():
        raise ValueError("Some movies have missing titles.")

    if movies["genres"].isna().any():
        raise ValueError("Some movies have missing genre fields.")

    # Extract a four-digit year at the end of the title.
    year_text = movies["title"].str.extract(
        r"\((\d{4})\)$",
        expand=False,
    )

    movies["release_year"] = pd.to_numeric(
        year_text, errors="coerce"
    ).astype("Int64")

    # Check the years we successfully extracted.
    invalid_years = (
        movies["release_year"].notna()
        & ~movies["release_year"].between(1888, 2100)
    )

    if invalid_years.any():
        raise ValueError("Unreasonable release years found.")

    # Split the genre strings into lists.
    def split_genres(value):
        if value == "(no genres listed)" or value == "":
            return []
        return [genre.strip() for genre in value.split("|")
                if genre.strip()]

    movies["genre_list"] = movies["genres"].map(split_genres)

    logging.info(
        "Transformed %s movie records",
        len(movies),
    )

    return movies


# ============================================================
# 5. TRANSFORM RATINGS
# ============================================================

def transform_ratings(ratings, valid_movie_ids):
    """Validate ratings before inserting them into SQLite."""
    ratings = ratings.copy()

    for column in ["userId", "movieId", "rating", "timestamp"]:
        ratings[column] = pd.to_numeric(
            ratings[column], errors="coerce"
        )

    if ratings[
        ["userId", "movieId", "rating", "timestamp"]
    ].isna().any().any():
        raise ValueError(
            "Ratings contain missing or non-numeric values."
        )

    # User and movie IDs must be positive integers.
    for column in ["userId", "movieId"]:
        if (ratings[column] <= 0).any():
            raise ValueError(f"{column} must be positive.")

        if (ratings[column] % 1 != 0).any():
            raise ValueError(f"{column} must contain integers.")

        ratings[column] = ratings[column].astype(int)

    # Validate rating range and half-star increments.
    if not ratings["rating"].between(0.5, 5.0).all():
        raise ValueError("Ratings must be between 0.5 and 5.0.")

    doubled = ratings["rating"] * 2

    if ((doubled - doubled.round()).abs() > 1e-8).any():
        raise ValueError(
            "Ratings must use half-star increments."
        )

    # Validate Unix timestamps.
    if (ratings["timestamp"] < 0).any():
        raise ValueError("Negative timestamps found.")

    if (ratings["timestamp"] % 1 != 0).any():
        raise ValueError("Timestamps must be integers.")

    ratings["timestamp"] = ratings["timestamp"].astype(int)

    # Every rating must refer to a movie in movies.csv.
    unknown_movies = ~ratings["movieId"].isin(valid_movie_ids)

    if unknown_movies.any():
        raise ValueError(
            f"{unknown_movies.sum()} ratings reference "
            "unknown movie IDs."
        )

    # Our schema permits one rating per user/movie pair.
    if ratings.duplicated(
        subset=["userId", "movieId"]
    ).any():
        raise ValueError(
            "Duplicate user/movie rating pairs found."
        )

    logging.info(
        "Validated %s rating records",
        len(ratings),
    )

    return ratings


# ============================================================
# 6. LOAD
# ============================================================

def load_data(movies, ratings, db_file=None):
    """Create the schema and load all transformed data."""
    if db_file is None:
        db_file = DB_FILE
    if not SCHEMA_FILE.exists():
        raise FileNotFoundError(
            f"Schema file not found: {SCHEMA_FILE}"
        )

    connection = sqlite3.connect(db_file)

    try:
        # Enforce foreign keys for this connection.
        connection.execute("PRAGMA foreign_keys = ON")

        schema = SCHEMA_FILE.read_text(encoding="utf-8")
        connection.executescript(schema)

        # One transaction for the data load:
        # either the batch succeeds, or its changes roll back.
        with connection:
            # Load movie catalogue.
            movie_records = [
                (
                    int(row.movieId),
                    str(row.title),
                    (
                        None
                        if pd.isna(row.release_year)
                        else int(row.release_year)
                    ),
                )
                for row in movies.itertuples(index=False)
            ]

            connection.executemany(
                """
                INSERT INTO movies
                    (movie_id, title, release_year)
                VALUES (?, ?, ?)
                ON CONFLICT(movie_id) DO UPDATE SET
                    title = excluded.title,
                    release_year = excluded.release_year
                """,
                movie_records,
            )

            # Load unique genres.
            all_genres = sorted({
                genre
                for genre_list in movies["genre_list"]
                for genre in genre_list
            })

            connection.executemany(
                """
                INSERT INTO genres (genre_name)
                VALUES (?)
                ON CONFLICT(genre_name) DO NOTHING
                """,
                [(genre,) for genre in all_genres],
            )

            # Retrieve the database IDs for each genre.
            genre_rows = connection.execute(
                "SELECT genre_id, genre_name FROM genres"
            ).fetchall()

            genre_ids = {
                name: genre_id
                for genre_id, name in genre_rows
            }

            # Build the movie-to-genre relationships.
            movie_genre_records = [
                (int(movie_id), genre_ids[genre])
                for movie_id, genre_list in zip(
                    movies["movieId"],
                    movies["genre_list"],
                )
                for genre in genre_list
            ]

            connection.executemany(
                """
                INSERT INTO movie_genres (movie_id, genre_id)
                VALUES (?, ?)
                ON CONFLICT(movie_id, genre_id) DO NOTHING
                """,
                movie_genre_records,
            )

            # Load ratings. Reruns update existing records.
            rating_records = [
                (
                    int(row.userId),
                    int(row.movieId),
                    float(row.rating),
                    int(row.timestamp),
                )
                for row in ratings.itertuples(index=False)
            ]

            connection.executemany(
                """
                INSERT INTO ratings
                    (user_id, movie_id, rating, rated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, movie_id) DO UPDATE SET
                    rating = excluded.rating,
                    rated_at = excluded.rated_at
                """,
                rating_records,
            )

            # Create pending enrichment records for Lesson 5.
            # Existing API results are preserved on reruns.
            connection.executemany(
                """
                INSERT INTO movie_details (movie_id, api_status)
                VALUES (?, 'pending')
                ON CONFLICT(movie_id) DO NOTHING
                """,
                [(int(movie_id),) for movie_id in movies["movieId"]],
            )


            logging.info("Data loaded successfully into %s", db_file)

    finally:
        connection.close()



# ============================================================
# 7. VALIDATE LOADED DATA
# ============================================================

def validate_database(db_file=None):
    """Check row counts and foreign-key integrity."""
    if db_file is None:
        db_file = DB_FILE

    connection = sqlite3.connect(db_file)

    try:
        connection.execute("PRAGMA foreign_keys = ON")

        for table in [
            "movies",
            "ratings",
            "genres",
            "movie_genres",
            "movie_details",
        ]:
            count = connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

            logging.info("%s contains %s rows", table, count)

        violations = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()

        if violations:
            raise RuntimeError(
                f"Foreign-key violations found: {violations}"
            )

        logging.info("Foreign-key validation passed")

    finally:
        connection.close()


# ============================================================
# 8. ENRICHMENT STATUS
# ============================================================

RECOGNIZED_API_STATUSES = {"pending", "success", "not_found", "error"}

def get_enrichment_status_counts(db_file=None):
    """Query movie_details to count records in each api_status state."""
    if db_file is None:
        db_file = DB_FILE

    connection = sqlite3.connect(db_file)
    try:
        return dict(
            connection.execute(
                "SELECT api_status, COUNT(*) FROM movie_details GROUP BY api_status"
            ).fetchall()
        )
    finally:
        connection.close()


# ============================================================
# 9. MAIN PROGRAM
# ============================================================

def main():
    configure_logging()

    logging.info("Starting MovieLens ETL pipeline")

    movies, ratings = extract_data()

    movies = transform_movies(movies)

    ratings = transform_ratings(
        ratings,
        set(movies["movieId"]),
    )

    load_data(movies, ratings)

    validate_database()

    logging.info("Core MovieLens data ingested and validated successfully.")

    # Stage 2: External Metadata Enrichment via OMDb API
    logging.info("Starting external metadata enrichment stage via OMDb API...")

    try:
        import enrich_movies
        enrichment_result = enrich_movies.main()
    except RuntimeError as exc:
        exc_str = str(exc)
        if "OMDB_API_KEY is missing" in exc_str:
            logging.warning(
                "Core ETL succeeded, but OMDb enrichment was skipped: %s",
                exc_str.strip(),
            )
            logging.info(
                "To enrich movies, configure OMDB_API_KEY in .env and run "
                "'python etl.py' or 'python enrich_movies.py'."
            )
        elif "already running" in exc_str.lower():
            logging.error(
                "Core ETL succeeded, but enrichment could not start: "
                "another enrichment process is currently running."
            )
        else:
            sanitized = re.sub(
                r"([?&]apikey=)[^&\s'\"]+",
                r"\1[REDACTED]",
                exc_str,
                flags=re.IGNORECASE,
            )
            logging.error(
                "Core ETL succeeded, but enrichment failed with unexpected RuntimeError: %s",
                sanitized,
            )
        return
    except Exception as exc:
        sanitized = re.sub(
            r"([?&]apikey=)[^&\s'\"]+",
            r"\1[REDACTED]",
            str(exc),
            flags=re.IGNORECASE,
        )
        logging.error(
            "Core ETL succeeded, but enrichment encountered an unexpected error: %s",
            sanitized,
        )
        return

    # Check enrichment outcomes
    if enrichment_result.get("auth_failed"):
        logging.error(
            "Core ETL succeeded, but enrichment halted: OMDb authentication failed (HTTP 401). "
            "Please verify OMDB_API_KEY in your .env file."
        )
        return

    if enrichment_result.get("quota_exhausted"):
        logging.warning(
            "Core ETL succeeded, but enrichment paused: daily OMDb API quota exhausted. "
            "Unprocessed movies remain in pending status. Re-run 'python etl.py' when quota resets."
        )
        return

    if enrichment_result.get("max_reached"):
        logging.info(
            "Core ETL succeeded. Enrichment batch limit reached (%s movies processed). "
            "Additional runs may be needed to complete remaining pending records. "
            "Re-run 'python etl.py' or 'python enrich_movies.py' to process the next batch.",
            enrichment_result.get("processed_count", 0),
        )
        return

    totals = enrichment_result.get("totals", {})
    processed = enrichment_result.get("processed_count", 0)

    # Perform a safe, direct database status check to determine true catalog state
    try:
        status_counts = get_enrichment_status_counts(DB_FILE)
    except Exception as exc:
        sanitized = re.sub(
            r"([?&]apikey=)[^&\s'\"]+",
            r"\1[REDACTED]",
            str(exc),
            flags=re.IGNORECASE,
        )
        logging.error(
            "Core ETL succeeded, but enrichment status could not be verified: %s",
            sanitized,
        )
        return

    # Check for unexpected or unrecognized api_status values
    unrecognized = {
        status: count
        for status, count in status_counts.items()
        if status not in RECOGNIZED_API_STATUSES
    }
    if unrecognized:
        logging.warning(
            "Core ETL succeeded, but enrichment status is invalid: "
            "unrecognized api_status values found in database: %s. "
            "Enrichment status is incomplete.",
            unrecognized,
        )
        return

    total_records = sum(status_counts.values())
    pending_count = status_counts.get("pending", 0)
    error_count = status_counts.get("error", 0)
    not_found_count = status_counts.get("not_found", 0)
    success_count = status_counts.get("success", 0)

    # If there are unresolved errors in the database or batch, report incomplete status
    if error_count > 0 or totals.get("error", 0) > 0:
        logging.warning(
            "Core ETL succeeded, but enrichment finished with errors: "
            "%s movie(s) marked as error, %s not found, %s pending (%s success). "
            "See enrichment.log for details.",
            error_count,
            not_found_count,
            pending_count,
            success_count,
        )
        return

    # If pending records remain, report batch completion rather than full success
    if pending_count > 0:
        logging.info(
            "Core ETL succeeded. Enrichment batch completed (%s movie(s) processed in this run); "
            "%s movie(s) remain in pending status (%s success, %s not found, 0 errors). "
            "Re-run 'python etl.py' or 'python enrich_movies.py' to continue.",
            processed,
            pending_count,
            success_count,
            not_found_count,
        )
        return

    # If any records are not_found, catalog processing is complete but not all were enriched
    if not_found_count > 0 or totals.get("not_found", 0) > 0:
        if processed == 0:
            logging.info(
                "Enrichment check complete: all catalog movies processed. "
                "%s movie(s) successfully enriched, %s not found (0 pending, 0 errors).",
                success_count,
                not_found_count,
            )
        else:
            logging.info(
                "Enrichment completed for this run (%s movie(s) processed): "
                "%s total movie(s) successfully enriched, %s not found (0 pending, 0 errors).",
                processed,
                success_count,
                not_found_count,
            )
        logging.info(
            "Core ETL succeeded. Catalog processing finished (%s not-found record(s) handled without metadata).",
            not_found_count,
        )
        return

    # Ensure all records are accounted for and strictly in success status
    if total_records == 0 or success_count != total_records:
        logging.warning(
            "Core ETL succeeded, but enrichment is incomplete: "
            "%s of %s movie_details records are in success status.",
            success_count,
            total_records,
        )
        return

    # All pending are 0, no errors, no not_found, and all records are verified success
    if processed == 0:
        logging.info(
            "Enrichment check complete: all %s movies in the catalog are successfully enriched. "
            "No pending, not-found, or error records remain.",
            success_count,
        )
    else:
        logging.info(
            "Enrichment completed for this run: %s movie(s) processed. "
            "All %s movies in the catalog are successfully enriched.",
            processed,
            success_count,
        )

    logging.info("Full ETL and enrichment pipeline completed successfully.")


if __name__ == "__main__":
    main()
