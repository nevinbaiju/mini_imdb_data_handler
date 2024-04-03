/*
Airflow DB's and Tables
*/

CREATE DATABASE airflow_db;
CREATE USER airflow_admin IDENTIFIED BY 'password';
GRANT ALL ON airflow_db.* TO airflow_admin;

/*
Data DB's and tables
*/

CREATE DATABASE IF NOT EXISTS movie_db;

USE movie_db;

CREATE TABLE IF NOT EXISTS avg_ratings (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    avg_rating DECIMAL(3, 2),
    rating_std DECIMAL(3, 2),
    count INT,
    release_year INT
);

CREATE TABLE IF NOT EXISTS movie_ranks (
    movie_id INT,
    movie_rank INT,
    rank_diff INT,
    FOREIGN KEY (movie_id) REFERENCES avg_ratings(movie_id)
);

LOAD DATA INFILE '/var/lib/mysql-files/avg_ratings.csv'
INTO TABLE avg_ratings
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(movie_id, title, avg_rating, rating_std, count, release_year);

LOAD DATA INFILE '/var/lib/mysql-files/rankings.csv'
INTO TABLE movie_ranks
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(movie_id, movie_rank, rank_diff);

CREATE TABLE top_10_movies (
    title VARCHAR(255),
    avg_rating DECIMAL(5,2),
    release_year INT,
    movie_rank INT
);

INSERT INTO top_10_movies
SELECT 
    avg.title, 
    avg.avg_rating, 
    avg.release_year, 
    rnk.movie_rank
FROM
    avg_ratings AS avg 
JOIN 
    movie_ranks AS rnk ON avg.movie_id = rnk.movie_id
ORDER BY rnk.movie_rank
LIMIT 10;