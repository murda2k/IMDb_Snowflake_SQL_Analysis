#I've created a new warehouse called PROJECT_WH, which we will use for this project, and a Resource monitor of 3 daily credits with the help of the ACCOUNTADMIN role.

#We will use the database we created previously, called IMDB_DB, and its PUBLIC schema.
USE DATABASE IMDB_DB;
USE SCHEMA PUBLIC;



#With the help of Snowflake's UI, we've created a stage called @project and uploaded the same file as in project 1.
#After checking the file, we are going to create a table for it with its columns and a file format.
select $1, $2, $3, $4, $5, $6, $7, $8, $9
from @project;

CREATE TABLE MOVIES2(
TITLE VARCHAR,
IMDB_RATING NUMBER,
YEAR NUMBER,
CERTIFICATES VARCHAR,
GENRE VARCHAR,
DIRECTOR VARCHAR,
STAR_CAST VARCHAR,
METASCORE NUMBER,
DURATION NUMBER
);

CREATE FILE FORMAT PROJECT_MOVIES_CSV
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';



#After creating the table and the file format, we then copied the file into the table.
COPY INTO MOVIES2
FROM @project
files = ('IMDb_Dataset.csv')
file_format = ( format_name=PROJECT_MOVIES_CSV );

SELECT * FROM MOVIES2;



#We saw that the table has duplicates in it, so we will curate the file into a new table without dupes.
CREATE TABLE MOVIES_CLEAN LIKE MOVIES2;

INSERT INTO MOVIES_CLEAN
SELECT DISTINCT *
FROM MOVIES2;

SELECT * FROM MOVIES_CLEAN;


#Let's create CTEs to calculate genre-level statistics and rank movies within each genre, then let's retrieve the top movies per genre along with their performance relative to the genre average
WITH genre_stats AS (
    SELECT 
        GENRE,
        AVG(IMDB_RATING) AS avg_rating,
        COUNT(*) AS total_movies
    FROM MOVIES_CLEAN
    GROUP BY GENRE
),
ranked_movies AS (
    SELECT 
        TITLE,
        GENRE,
        IMDB_RATING,
        YEAR,
        DIRECTOR,
        ROW_NUMBER() OVER (PARTITION BY GENRE ORDER BY IMDB_RATING DESC) AS row_num
    FROM MOVIES_CLEAN
)
SELECT 
    r.GENRE,
    r.TITLE,
    r.IMDB_RATING,
    r.YEAR,
    r.DIRECTOR,
    g.avg_rating,
    g.total_movies,
    r.row_num AS genre_rank,
    CASE 
        WHEN r.IMDB_RATING > g.avg_rating THEN 'Above Average'
        WHEN r.IMDB_RATING = g.avg_rating THEN 'Average'
        ELSE 'Below Average'
    END AS performance
FROM ranked_movies r
JOIN genre_stats g ON r.GENRE = g.GENRE
WHERE r.row_num <= 3
ORDER BY g.avg_rating DESC, r.GENRE, r.row_num;