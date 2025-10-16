# IMDb Movie Analytics with Snowflake & SQL

##  Project Overview
This project demonstrates how to load, clean, and analyze movie data in **Snowflake** using **SQL**.  
It simulates a real-world workflow where raw CSV data is uploaded into a cloud warehouse, curated, and queried to extract business insights.

---

## Tech Stack
- **Snowflake** – cloud data warehouse  
- **SQL** – data cleaning, transformation, and analytics  
- **CTEs** – modular query design  
- **Window Functions** – ranking, averages, comparisons  

---

## Steps & Logic

### 1. Setup
A warehouse (`PROJECT_WH`) and a resource monitor (3 daily credits) were created using the `ACCOUNTADMIN` role.  
We used an existing database:  
```sql
USE DATABASE IMDB_DB;
USE SCHEMA PUBLIC;
2. Stage Creation & File Upload
The CSV file (IMDb_Dataset.csv) was uploaded to a Snowflake stage named @project.

3. Table & File Format Creation
sql
Copy code
CREATE TABLE MOVIES2 (
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
4. Load Data into Table
sql
Copy code
COPY INTO MOVIES2
FROM @project
files = ('IMDb_Dataset.csv')
file_format = ( format_name=PROJECT_MOVIES_CSV );
5. Clean Data (Remove Duplicates)
sql
Copy code
CREATE TABLE MOVIES_CLEAN LIKE MOVIES2;

INSERT INTO MOVIES_CLEAN
SELECT DISTINCT * FROM MOVIES2;
6. Analytical Query (CTE)
We calculated average rating and total movies per genre,
ranked movies within each genre, and used a CASE expression to classify performance.

sql
Copy code
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