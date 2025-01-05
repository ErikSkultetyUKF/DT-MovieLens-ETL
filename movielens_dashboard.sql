-- Top 10 filmov podľa počtu hodnotení
SELECT 
    m.title,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_movies m ON m.ID = fact_ratings.dim_movies_ID
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;

-- Top 10 filmov s najväčším hodnotením
SELECT 
    m.title,
    SUM(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_movies m ON m.ID = fact_ratings.dim_movies_ID
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;

-- Top 20 rokov vydania filmov s najväčším hodnotením
SELECT
    m.release_year,
    SUM(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_movies m ON m.ID = fact_ratings.dim_movies_ID
GROUP BY m.release_year
ORDER BY total_ratings DESC
LIMIT 20;

-- Top 5 najčasteších hodín hodnotenia užívateľmi
SELECT
    tm.hour,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_time tm ON tm.ID = fact_ratings.dim_time_ID
GROUP BY tm.hour
ORDER BY total_ratings DESC
LIMIT 5;

-- Počet hodnotení podľa vekovej kategórie
SELECT 
    u.age_group_name,
    COUNT(fact_ratings.ID) AS total_ratings
FROM fact_ratings
JOIN dim_users u ON u.ID = fact_ratings.dim_users_ID
GROUP BY u.age_group_name
ORDER BY total_ratings DESC;
