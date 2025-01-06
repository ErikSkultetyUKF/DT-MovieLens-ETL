-- Vytvorenie databázy
CREATE OR REPLACE DATABASE SPARROW_MovieLens;
USE DATABASE SPARROW_MovieLens;

-- Vytvorenie schémy pre staging tabuľky
CREATE OR REPLACE SCHEMA SPARROW_MovieLens.staging;
USE SCHEMA SPARROW_MovieLens.staging;

CREATE OR REPLACE STAGE SPARROW_MovieLens_Stage;

CREATE OR REPLACE FILE FORMAT SPARROW_MOVIELENS_CSV
TYPE = CSV
COMPRESSION = NONE
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FILE_EXTENSION = 'csv'
SKIP_HEADER = 1
RECORD_DELIMITER = '\n'
TRIM_SPACE = FALSE
NULL_IF = ('NULL', 'null', '');

-- Vytvorenie tabuľky occupations (staging)
CREATE OR REPLACE TABLE occupations_staging (
    occupationId INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Vytvorenie tabuľky age_group (staging)
CREATE OR REPLACE TABLE age_group_staging (
    ageGroupId INT PRIMARY KEY,
    name VARCHAR(45)
);

-- Vytvorenie tabuľky users (staging)
CREATE OR REPLACE TABLE users_staging (
    userId INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupationId INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (occupationId) REFERENCES occupations_staging(occupationId)
);

-- Vytvorenie tabuľky movies (staging)
CREATE OR REPLACE TABLE movies_staging (
    movieId INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

-- Vytvorenie tabuľky genres (staging)
CREATE OR REPLACE TABLE genres_staging (
    genreId INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Vytvorenie tabuľky genres_movies (staging)
CREATE OR REPLACE TABLE genres_movies_staging (
    genreMovieId INT PRIMARY KEY,
    movieId INT,
    genreId INT,
    FOREIGN KEY (movieId) REFERENCES movies_staging(movieId),
    FOREIGN KEY (genreId) REFERENCES genres_staging(genreId)
);

-- Vytvorenie tabuľky ratings (staging)
CREATE OR REPLACE TABLE ratings_staging (
    ratingId INT PRIMARY KEY,
    userId INT,
    movieId INT,
    ratings INT,
    rated_at DATETIME,
    FOREIGN KEY (userId) REFERENCES users_staging(userId),
    FOREIGN KEY (movieId) REFERENCES movies_staging(movieId)
);

-- Vytvorenie tabuľky tags (staging)
CREATE OR REPLACE TABLE tags_staging (
    tagId INT PRIMARY KEY,
    userId INT,
    movieId INT,
    tags VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY (userId) REFERENCES users_staging(userId),
    FOREIGN KEY (movieId) REFERENCES movies_staging(movieId)
);

-- Nakopírovanie dát do tabuľky occupations (staging)
COPY INTO occupations_staging
FROM @SPARROW_MOVIELENS_STAGE/occupations.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky age_group (staging)
COPY INTO age_group_staging
FROM @SPARROW_MOVIELENS_STAGE/age_group.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky users (staging)
COPY INTO users_staging 
FROM @SPARROW_MOVIELENS_STAGE/users.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky movies (staging)
COPY INTO movies_staging
FROM @SPARROW_MOVIELENS_STAGE/movies.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky genres (staging)
COPY INTO genres_staging
FROM @SPARROW_MOVIELENS_STAGE/genres.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky genres_movies (staging)
COPY INTO genres_movies_staging
FROM @SPARROW_MOVIELENS_STAGE/genres_movies.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky tags (staging)
COPY INTO tags_staging
FROM @SPARROW_MOVIELENS_STAGE/tags.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nakopírovanie dát do tabuľky ratings (staging)
COPY INTO ratings_staging
FROM @SPARROW_MOVIELENS_STAGE/ratings.csv
FILE_FORMAT = SPARROW_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Vytvorenie tabuľky tags (dimenzia)
CREATE OR REPLACE TABLE dim_tags AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tags) AS ID,
    tags,
    created_at
FROM tags_staging

-- Vytvorenie tabuľky time (dimenzia)
CREATE OR REPLACE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY EXTRACT(HOUR FROM rated_at), EXTRACT(MINUTE FROM rated_at)) AS ID,
    EXTRACT(HOUR FROM rated_at) AS hour,
    EXTRACT(MINUTE FROM rated_at) AS minute,
    EXTRACT(SECOND FROM rated_at) AS second
FROM ratings_staging
GROUP BY EXTRACT(HOUR FROM rated_at), EXTRACT(MINUTE FROM rated_at), EXTRACT(SECOND FROM rated_at)
ORDER BY hour, minute, second;

-- Vytvorenie tabuľky date (dimenzia)
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS ID,
    CAST(rated_at AS DATE) AS date,
    EXTRACT(DAY FROM rated_at) AS day,
    EXTRACT(MONTH FROM rated_at) AS month,
    EXTRACT(YEAR FROM rated_at) AS year,
FROM (
    SELECT DISTINCT
        CAST(rated_at AS DATE) AS rated_at,
        EXTRACT(DAY FROM rated_at) AS day,
        EXTRACT(MONTH FROM rated_at) AS month,
        EXTRACT(YEAR FROM rated_at) AS year
    FROM ratings_staging
) unique_dates;

-- Vytvorenie tabuľky movies (dimenzia)
CREATE OR REPLACE TABLE dim_movies AS
SELECT
    ROW_NUMBER() OVER (ORDER BY title) AS ID,
    title,
    release_year    
FROM movies_staging;

-- Vytvorenie tabuľky genres (dimenzia)
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS ID,
    name
FROM genres_staging
GROUP BY name;

-- Vytvorenie tabuľky users (dimenzia)
CREATE OR REPLACE TABLE dim_users AS
SELECT
    u.userId AS ID,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group_name,
    u.gender,
    o.name AS occupation_name,
FROM users_staging u
JOIN occupations_staging o ON u.occupationId = o.occupationId;

-- Vytvorenie tabuľky ratings (faktová)
CREATE OR REPLACE TABLE fact_ratings AS
SELECT 
    r.ratingId AS ID,
    r.ratings,
    du.ID AS dim_users_ID,
    dt.ID AS dim_tags_ID,
    dm.ID AS dim_movies_ID,
    dg.ID AS dim_genres_ID,
    dtime.ID AS dim_time_ID,
    ddate.ID AS dim_date_ID
FROM ratings_staging r
JOIN dim_users du ON r.userId = du.ID
JOIN dim_movies dm ON r.movieId = dm.ID
LEFT JOIN tags_staging ts ON r.userId = ts.userId AND r.movieId = ts.movieId
LEFT JOIN dim_tags dt ON ts.tags = dt.tags
JOIN genres_movies_staging gm ON r.movieId = gm.movieId
JOIN dim_genres dg ON gm.genreId = dg.ID
JOIN dim_time dtime 
    ON EXTRACT(HOUR FROM r.rated_at) = dtime.hour
    AND EXTRACT(MINUTE FROM r.rated_at) = dtime.minute
    AND EXTRACT(SECOND FROM r.rated_at) = dtime.second
JOIN dim_date ddate ON CAST(r.rated_at AS DATE) = ddate.date;

-- Dropnutie staging tabuliek
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
