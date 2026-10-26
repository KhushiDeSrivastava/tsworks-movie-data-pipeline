
-- ============================================================
-- TSWorks Data Engineering Assignment
-- File: queries.sql
-- ============================================================


-- 1. Which movie has the highest average rating?
-- Returns all movies tied for the highest average rating.

WITH movie_averages AS (
    SELECT
        m.movie_id,
        m.title,
        AVG(r.rating) AS average_rating,
        COUNT(r.rating) AS rating_count
    FROM movies AS m
    JOIN ratings AS r
        ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
)
SELECT
    title,
    ROUND(average_rating, 2) AS average_rating,
    rating_count
FROM movie_averages
WHERE average_rating = (
    SELECT MAX(average_rating)
    FROM movie_averages
)
ORDER BY title;


-- ============================================================
-- 2. What are the top 5 movie genres with the highest average
--    rating?
-- ============================================================

SELECT
    g.genre_name AS genre,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS rating_count
FROM genres AS g
JOIN movie_genres AS mg
    ON g.genre_id = mg.genre_id
JOIN ratings AS r
    ON mg.movie_id = r.movie_id
GROUP BY g.genre_id, g.genre_name
ORDER BY average_rating DESC, rating_count DESC
LIMIT 5;


-- ============================================================
-- 3. Who is the director with the most movies in this dataset?
-- ============================================================

SELECT
    d.director_name AS director,
    COUNT(DISTINCT md.movie_id) AS movie_count
FROM directors AS d
JOIN movie_directors AS md
    ON d.director_id = md.director_id
GROUP BY d.director_id, d.director_name
ORDER BY movie_count DESC, director ASC
LIMIT 1;


-- ============================================================
-- 4. What is the average rating of movies released each year?
-- ============================================================

SELECT
    m.release_year,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS rating_count
FROM movies AS m
JOIN ratings AS r
    ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;
