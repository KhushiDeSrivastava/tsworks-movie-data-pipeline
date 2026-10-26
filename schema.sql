
-- ============================================================
-- Movie Data Pipeline
-- Database schema for SQLite
-- ============================================================

-- 1. MOVIES
-- Stores the original MovieLens movie catalogue.

CREATE TABLE IF NOT EXISTS movies (
    movie_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    release_year INTEGER,

    CHECK (
        release_year IS NULL
        OR release_year BETWEEN 1888 AND 2100
    )
);


-- 2. RATINGS
-- Each user can have one rating per movie in this dataset.

CREATE TABLE IF NOT EXISTS ratings (
    user_id INTEGER NOT NULL,
    movie_id INTEGER NOT NULL,
    rating REAL NOT NULL,
    rated_at INTEGER NOT NULL,

    PRIMARY KEY (user_id, movie_id),

    FOREIGN KEY (movie_id)
        REFERENCES movies(movie_id)
        ON DELETE CASCADE,

    CHECK (rating >= 0.5 AND rating <= 5.0),
    CHECK (rating * 2 = CAST(rating * 2 AS INTEGER)),
    CHECK (rated_at >= 0)
);


-- 3. GENRES
-- Stores each distinct genre once.

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT NOT NULL UNIQUE
        COLLATE NOCASE
);


-- 4. MOVIE_GENRES
-- Junction table for the many-to-many relationship.

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,

    PRIMARY KEY (movie_id, genre_id),

    FOREIGN KEY (movie_id)
        REFERENCES movies(movie_id)
        ON DELETE CASCADE,

    FOREIGN KEY (genre_id)
        REFERENCES genres(genre_id)
        ON DELETE CASCADE
);


-- 5. MOVIE_DETAILS
-- Stores OMDb enrichment separately from MovieLens data.
-- A row can exist even if the API lookup fails.

CREATE TABLE IF NOT EXISTS movie_details (
    movie_id INTEGER PRIMARY KEY,
    imdb_id TEXT,
    plot TEXT,
    box_office TEXT,
    api_status TEXT NOT NULL DEFAULT 'pending',
    fetched_at TEXT,
    last_error TEXT,

    FOREIGN KEY (movie_id)
        REFERENCES movies(movie_id)
        ON DELETE CASCADE,

    CHECK (
        api_status IN (
            'pending',
            'success',
            'not_found',
            'error'
        )
    )
);


-- 6. DIRECTORS
-- Stores each distinct director once.

CREATE TABLE IF NOT EXISTS directors (
    director_id INTEGER PRIMARY KEY AUTOINCREMENT,
    director_name TEXT NOT NULL UNIQUE
        COLLATE NOCASE
);


-- 7. MOVIE_DIRECTORS
-- Junction table for movies and directors.

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id INTEGER NOT NULL,
    director_id INTEGER NOT NULL,

    PRIMARY KEY (movie_id, director_id),

    FOREIGN KEY (movie_id)
        REFERENCES movies(movie_id)
        ON DELETE CASCADE,

    FOREIGN KEY (director_id)
        REFERENCES directors(director_id)
        ON DELETE CASCADE
);


-- ============================================================
-- INDEXES
-- Help common joins and lookups run more efficiently.
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_ratings_movie_id
    ON ratings(movie_id);

CREATE INDEX IF NOT EXISTS idx_movie_genres_genre_id
    ON movie_genres(genre_id);

CREATE INDEX IF NOT EXISTS idx_movie_directors_director_id
    ON movie_directors(director_id);

CREATE INDEX IF NOT EXISTS idx_movies_release_year
    ON movies(release_year);

CREATE INDEX IF NOT EXISTS idx_directors_name
    ON directors(director_name);

CREATE INDEX IF NOT EXISTS idx_movie_details_imdb_id
    ON movie_details(imdb_id);
