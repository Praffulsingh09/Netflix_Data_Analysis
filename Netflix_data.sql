
SELECT * FROM netflix_data;


-- making data short
CREATE TABLE netflix_data (
    show_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(10) NULL,
    title VARCHAR(200) NULL,
    director VARCHAR(250) NULL,
    cast VARCHAR(1000) NULL,
    country VARCHAR(150) NULL,
    date_added VARCHAR(20) NULL,
    release_year INT NULL,
    rating VARCHAR(10) NULL,
    duration VARCHAR(10) NULL,
    listed_in VARCHAR(100) NULL,
    description VARCHAR(500) NULL
)
    
SELECT * FROM netflix_data;

-- removing duplicates
-- finding
SELECT * 
FROM netflix_data
WHERE CONCAT(title, type) IN (
    SELECT CONCAT(title, type)
    FROM netflix_data
    GROUP BY title, type
    HAVING COUNT(*) > 1
)
ORDER BY title;

-- removing
WITH cte AS(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY title, type ORDER BY show_id) AS rn
FROM netflix_data
)
SELECT * FROM cte 
WHERE rn = 1;

-- now we leftwith 8803 rows

-- new table for listed in, director, country, cast
CREATE TABLE numbers (n INT);
INSERT INTO numbers (n)
VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

DELIMITER $$

CREATE FUNCTION split_string(
    str TEXT, -- Changed to TEXT to handle larger strings
    delim VARCHAR(12), 
    pos INT
) RETURNS VARCHAR(255)
DETERMINISTIC
BEGIN
    DECLARE result VARCHAR(255);
    SET result = TRIM(
        REPLACE(
            SUBSTRING(
                SUBSTRING_INDEX(str, delim, pos),
                LENGTH(SUBSTRING_INDEX(str, delim, pos - 1)) + 1,
                LENGTH(str)
            ),
            delim, ''
        )
    );
    RETURN result;
END $$

DELIMITER ;


-- Create table for directors
CREATE TABLE netflix_directors AS
SELECT show_id, LTRIM(RTRIM(split_string(director, ',', n.n))) AS director
FROM netflix_data
JOIN numbers n ON CHAR_LENGTH(director) - CHAR_LENGTH(REPLACE(director, ',', '')) >= n.n - 1
WHERE LTRIM(RTRIM(split_string(director, ',', n.n))) <> '';

-- Check the results
SELECT * FROM netflix_directors;

-- Create table for genres
CREATE TABLE netflix_genre AS
SELECT show_id, LTRIM(RTRIM(split_string(listed_in, ',', n.n))) AS genre
FROM netflix_data
JOIN numbers n ON CHAR_LENGTH(listed_in) - CHAR_LENGTH(REPLACE(listed_in, ',', '')) >= n.n - 1
WHERE LTRIM(RTRIM(split_string(listed_in, ',', n.n))) <> '';

-- Check the results
SELECT * FROM netflix_genre;

-- Create table for countries
CREATE TABLE netflix_countries AS
SELECT show_id, LTRIM(RTRIM(split_string(country, ',', n.n))) AS country
FROM netflix_data
JOIN numbers n ON CHAR_LENGTH(country) - CHAR_LENGTH(REPLACE(country, ',', '')) >= n.n - 1
WHERE LTRIM(RTRIM(split_string(country, ',', n.n))) <> '';

-- Check the results
SELECT * FROM netflix_countries;

-- Create table for cast
CREATE TABLE netflix_cast AS
SELECT show_id, LTRIM(RTRIM(split_string(cast, ',', n.n))) AS cast
FROM netflix_data
JOIN numbers n ON CHAR_LENGTH(cast) - CHAR_LENGTH(REPLACE(cast, ',', '')) >= n.n - 1
WHERE LTRIM(RTRIM(split_string(cast, ',', n.n))) <> '';

-- Check the results
SELECT * FROM netflix_cast;

-- data type conversions for date added
WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_data
)
SELECT show_id, type, title, STR_TO_DATE(date_added, '%M %d, %Y') AS date_added,
       release_year, rating, duration, description
FROM cte
WHERE rn = 1;

-- populate missing values in country

SELECT *
FROM
    netflix_data
WHERE
    country IS NULL;

select * from netflix_data
where director = 'Ahishor Solomon';

select director, country 
from netflix_countries as nc inner join netflix_directors as nd
on nc.show_id = nd.show_id 
group by director, country
order by director;

INSERT INTO netflix_countries
SELECT show_id, m.country 
FROM netflix_data AS n
INNER JOIN (
SELECT director, country 
FROM netflix_countries AS nc INNER JOIN netflix_directors AS nd
ON nc.show_id = nd.show_id 
GROUP BY director, country 
) AS m 
ON n.director = m.director
WHERE n.country IS NULL;

select * from netflix_countries;

-- populate missing values in duration

WITH cte AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_data
)
SELECT show_id, type, title, STR_TO_DATE(date_added, '%M %d, %Y') AS date_added,
       release_year, rating,
       CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration,
       description
FROM cte
WHERE rn = 1;

-- final clean table
CREATE TABLE netflix_f (
    show_id VARCHAR(10),
    type VARCHAR(10),
    title NVARCHAR(200),
    date_added DATE,
    release_year INT,
    rating VARCHAR(10),
    duration VARCHAR(10),
    description VARCHAR(500)
);

INSERT INTO netflix_f (show_id, type, title, date_added, release_year, rating, duration, description)
SELECT show_id, type, title, STR_TO_DATE(date_added, '%M %d, %Y') AS date_added,
       release_year, rating,
       CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration,
       description
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_data
) AS cte
WHERE rn = 1;

SELECT * FROM netflix_f;

-- data analysis

-- 1. Which actors have collaborated most frequently on Netflix projects, and what are the titles of those projects?
-- Purpose: This question helps identify key actor collaborations which could influence casting decisions for future projects.
-- Functions Used: Join, Window Functions

WITH actor_pairs AS (
    SELECT 
        n1.show_id,
        n1.cast AS actor1,
        n2.cast AS actor2,
        nd.title
    FROM 
        netflix_cast n1
    JOIN 
        netflix_cast n2 ON n1.show_id = n2.show_id
    JOIN
        netflix_data nd ON n1.show_id = nd.show_id
    WHERE 
        n1.cast < n2.cast
)
SELECT 
    actor1, 
    actor2, 
    COUNT(*) AS collaboration_count,
    GROUP_CONCAT(title) AS projects
FROM 
    actor_pairs
GROUP BY 
    actor1, actor2
HAVING 
    collaboration_count > 1
ORDER BY 
    collaboration_count DESC;
    
-- 2. Which genres tend to have the highest average ratings, and how do these ratings vary by country?
-- Purpose: This helps in understanding which genres are most favorably received and how cultural preferences influence ratings.
-- Functions Used: Join, Subquery
    
SELECT 
    ng.genre, 
    nc.country, 
    AVG(CASE 
        WHEN n.rating = 'G' THEN 1
        WHEN n.rating = 'PG' THEN 2
        WHEN n.rating = 'PG-13' THEN 3
        WHEN n.rating = 'R' THEN 4
        WHEN n.rating = 'NC-17' THEN 5
        -- Add more ratings and their numeric values if needed
        ELSE NULL
    END) AS average_rating
FROM 
    netflix_genre ng
JOIN 
    netflix_data n ON ng.show_id = n.show_id
JOIN 
    netflix_countries nc ON n.show_id = nc.show_id
WHERE 
    n.rating IS NOT NULL
GROUP BY 
    ng.genre, nc.country
ORDER BY 
    average_rating DESC;
    
-- 3. Which directors have shown versatility by working across multiple genres, and what is the distribution of their work?
-- Purpose: Identifying versatile directors can help in recognizing talent capable of handling diverse content, which is valuable for varied content production.
-- Functions Used: Join, CTE    
WITH director_genres AS (
    SELECT 
        nd.director, 
        ng.genre, 
        COUNT(*) AS genre_count
    FROM 
        netflix_directors nd
    JOIN 
        netflix_genre ng ON nd.show_id = ng.show_id
    GROUP BY 
        director, genre
)
SELECT 
    director, 
    COUNT(DISTINCT genre) AS genre_count, 
    GROUP_CONCAT(genre) AS genres
FROM 
    director_genres
GROUP BY 
    director
HAVING 
    genre_count > 1
ORDER BY 
    genre_count DESC;

-- 4. What is the impact of release year on the popularity of different types of content (movies vs. TV shows)?
-- Purpose: Analyzing this can help understand how the popularity of movies vs. TV shows has evolved over time.
-- Functions Used: Window Functions, CTEs
WITH release_trend AS (
    SELECT 
        type, 
        release_year, 
        COUNT(*) AS release_count
    FROM 
        netflix_data
    GROUP BY 
        type, release_year
)
SELECT 
    type, 
    release_year, 
    release_count,
    SUM(release_count) OVER (PARTITION BY type ORDER BY release_year) AS cumulative_count
FROM 
    release_trend
ORDER BY 
    release_year, type;



-- 5. What are the most common combinations of genres in Netflix shows?
-- Purpose: This question aims to understand which genres are often combined in Netflix shows, providing insights into popular genre pairings that could influence future content decisions.
-- Functions Used: Join, Group By, Aggregation
WITH genre_combinations AS (
    SELECT 
        ng1.genre AS genre1, 
        ng2.genre AS genre2,
        COUNT(*) AS combination_count
    FROM 
        netflix_genre ng1
    JOIN 
        netflix_genre ng2 ON ng1.show_id = ng2.show_id AND ng1.genre < ng2.genre
    GROUP BY 
        genre1, genre2
)
SELECT 
    genre1, 
    genre2, 
    combination_count
FROM 
    genre_combinations
ORDER BY 
    combination_count DESC;





